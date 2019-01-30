import logging
import mysql.connector as mariadb

from database_loader.type_inference import DataType
from logger import logger

# Initialise the module logger
logger.initialise_logger("database-loader", log_level=logging.DEBUG)
module_logger = logging.getLogger('database-loader')


def build_database_connection(db_params, set_db=True):
    """
    Build a database connection given the database parameters.

    :param db_params: Database parameters.
    :param set_db: Set the database to use?
    :return: Database connection.
    """

    if set_db:
        return mariadb.connect(host=db_params['host'],
                               user=db_params['user'],
                               passwd=db_params['password'],
                               database=db_params['database-name'])
    else:
        return mariadb.connect(host=db_params['host'],
                               user=db_params['user'],
                               passwd=db_params['password'])


def create_database(db_params):
    """
    Create the database if it doesn't exist.

    :param db_params: Database parameters.
    """

    # Check if the database already
    module_logger.info("Checking database: %s" % db_params['database-name'])

    # Create the database
    mydb = build_database_connection(db_params, False)
    mycursor = mydb.cursor()
    create_string = "CREATE DATABASE IF NOT EXISTS %s" % db_params['database-name']
    module_logger.info("Creating database with: %s" % create_string)
    mycursor.execute(create_string)
    mycursor.close()


def drop_table(db_params, table_name):
    """
    Drop a database table (if it exists).

    :param db_params: Database parameters.
    :param table_name: Name of the table to drop.
    :return: True if the table was dropped, otherwise False.
    """

    # Get the SQL 'safe' version of the table name
    safe_table_name = safe_name(table_name)
    module_logger.info("Safe table name for %s is %s" % (table_name, safe_table_name))

    # Get a database connection
    mydb = build_database_connection(db_params)
    cursor = mydb.cursor()

    stmt = "SHOW TABLES LIKE '{0}'".format(safe_table_name)
    cursor.execute(stmt)
    result = cursor.fetchone()

    if result:
        module_logger.info("Table %s already exists" % safe_table_name)
        drop_stmt = "DROP TABLE {0}".format(safe_table_name)
        module_logger.info("Dropping table with: %s" % drop_stmt)
        cursor.execute(drop_stmt)
        table_dropped = True
    else:
        module_logger.info("Table %s doesn't exist" % safe_table_name)
        table_dropped = False

    cursor.close()
    return table_dropped


def drop_tables(db_params, table_names):
    """
    Drop the tables specified in the list of table_names if they already exist.

    :param db_params: Database parameters.
    :param table_names: List of table names.
    :return:
    """

    for name in table_names:
        drop_table(db_params, name)


def datatype_to_sql_conversion(datatype):
    """
    Convert the inferred data type to a suitable SQL type.

    :param datatype: Datatype.
    :return:
    """

    mappings = {DataType.int: "BIGINT",
                DataType.float: "DOUBLE",
                DataType.string: "TEXT",
                DataType.boolean: "BOOLEAN"}

    if datatype not in mappings:
        raise ValueError("Unknown data type: %s" % datatype)

    return mappings[datatype]


def safe_name(name):
    """
    Create a SQL-safe name.

    :param name: Name to convert.
    :return: Converted name.
    """

    # Preconditions
    assert type(name) == str

    safe_chars = [c if (c.isalpha() or c.isdigit()) else '_' for c in name]
    return "".join(safe_chars)


def create_table_statement(table_name, schema):
    """
    Build the CREATE TABLE statement.

    :param table_name: Database table name.
    :param schema: Inferred schema.
    :return: CREATE statement.
    """

    # Preconditions
    assert type(schema) == dict

    # Create a list of field name and SQL type
    name_type = ["%s %s" % (safe_name(name), datatype_to_sql_conversion(tpe)) for name, tpe in schema.items()]

    id_field_name = "%s____ID" % safe_name(table_name)
    primary_key = "PRIMARY KEY (%s)" % id_field_name
    field_spec = "%s INT NOT NULL AUTO_INCREMENT, %s, %s" % (id_field_name, ", ".join(name_type), primary_key)

    stmt = "CREATE TABLE %s (%s);" % (safe_name(table_name), field_spec)

    return stmt


def create_table(db_params, table_name, schema):
    """
    Create the database table based on the inferred schema.

    :param db_params: Database parameters.
    :param table_name: Database table name.
    :param schema: List of tuples of field name to inferred type.
    """

    # Create the statement
    stmt = create_table_statement(table_name, schema)
    module_logger.info("Creating table with: %s" % stmt)

    # Get a database connection
    mydb = build_database_connection(db_params)
    cursor = mydb.cursor()

    # Run the statement
    cursor.execute(stmt)


def insert_data_statement(table_name, schema, data, true_values, false_values):
    """
    Build the INSERT statement to put the data into the database.

    :param table_name: Database table name.
    :param schema: List of tuples of field name to inferred type.
    :param data: Dictionary of field name to value.
    :param true_values: Values deemed True.
    :param false_values: Values deemed False.
    :return: INSERT statement.
    """

    # Preconditions
    assert type(table_name) == str
    assert type(schema) == dict
    assert type(data) == dict

    # Map the field names to their safe variants and apply data type-specific transforms
    list_column_names = []
    list_values = []

    for fieldname, value in data.items():
        safe_column_name = safe_name(fieldname)

        if schema[fieldname] == DataType.boolean:
            if data[fieldname] in true_values:
                value = 'true'
            elif data[fieldname] in false_values:
                value = 'false'
            else:
                raise ValueError("Unable to parse Boolean value: %s" % data[fieldname])
        else:
            value = "\"%s\"" % value

        list_column_names.append(safe_column_name)
        list_values.append(value)

    str_list_column_names = ", ".join(list_column_names)
    str_list_values = ", ".join(list_values)

    # Return the INSERT statement
    return "INSERT INTO %s (%s) VALUES (%s);" % (safe_name(table_name), str_list_column_names, str_list_values)


def insert_data(db_params, table_name, schema, data, true_values, false_values):
    """
    Insert the data into the database table.

    :param db_params: Database parameters.
    :param table_name: Name of the database table.
    :param schema: Schema (dictionary of field name to type).
    :param data: Dictionary of the data (field name to value).
    :param true_values: Values deemed True.
    :param false_values: Values deemed False.
    """

    # Preconditions
    assert type(db_params) == dict
    assert type(table_name) == str
    assert type(schema) == dict
    assert type(data) == dict

    # Create the INSERT statement
    stmt = insert_data_statement(table_name, schema, data, true_values, false_values)

    # Get a database connection
    mydb = build_database_connection(db_params)
    cursor = mydb.cursor()

    # Run the statement
    cursor.execute(stmt)

    mydb.commit()
