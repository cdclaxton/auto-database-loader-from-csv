# -*- coding: utf-8 -*-
import glob
import logging
import os
import re

from data_reader.csv_reader import DelimitedSource
from database_loader.database_utilities import create_database, drop_table, create_table, insert_data
from database_loader.type_inference import build_field_type, update_field_type, merge_field_types
from logger import logger

# Initialise the module logger
logger.initialise_logger("loader", log_level=logging.INFO)
module_logger = logging.getLogger('loader')


def table_name_from_filename(file_path):
    """
    Get the table name from a filename.

    :param file_path: File path.
    :return: Database table name to files that should be used to populate that table.
    """

    # Extract the filename part from the path
    filename = os.path.basename(file_path)

    # Remove the file extension
    filename_minus_ext = os.path.splitext(filename)[0]

    # If the end of the filename is of the form _<number> then remove it
    m = re.search("(.*?)_\d+$", filename_minus_ext)
    if m is not None:
        name = m.group(1)
    else:
        name = filename_minus_ext

    # Return the table name
    return name


def table_names_from_path(filepath):
    """
    Determine the database table names based on the filenames in a given folder.

    :param filepath: Folder in which
    :return: Map of table names to the files to use to populate each table.
    """

    # Search for CSV files
    if filepath.endswith("/"):
        pattern = filepath + "*.csv"
    else:
        pattern = filepath + "/*.csv"

    # Get a list of filenames based on the folder specified
    files = glob.glob(pattern)

    # Get and return the table names from the filenames
    table_name_to_files = {}
    for f in files:
        table_name = table_name_from_filename(f)

        if table_name not in table_name_to_files:
            table_name_to_files[table_name] = [f]
        else:
            table_name_to_files[table_name].append(f)

    return table_name_to_files


def build_schema_from_file(filepath, delimiter, encapsulator, encoding, true_values, false_values):
    """
    Build the schema from the data in a single file.

    :param filepath: Path of the file to process.
    :param delimiter: Delimiter used in the CSV file.
    :param encapsulator: Encapsulator used in the CSV file.
    :param encoding: Encoding format of the CSV file.
    :param true_values: List of values deemed True.
    :param false_values: List of values deemed False.
    :return: Dictionary of the field name to inferred data type.
    """

    # Preconditions
    assert type(true_values) == list
    assert type(false_values) == list
    assert len(true_values) > 0
    assert len(false_values) > 0

    # Open the CSV file for reading
    csv_reader = DelimitedSource(filepath, delimiter, encapsulator, encoding)

    # Read each data line
    num_lines_read = 0
    dict_fieldname_to_type = {}

    for data_dict in csv_reader.parse():

        if num_lines_read == 0:
            # For the first line
            dict_fieldname_to_type = build_field_type(data_dict, true_values, false_values)
        else:
            # For second lines and onwards
            dict_fieldname_to_type = update_field_type(dict_fieldname_to_type, data_dict, true_values, false_values)

        num_lines_read += 1

    module_logger.info("Read %d lines from %s" % (num_lines_read, filepath))
    module_logger.info("Field names read: %s" % dict_fieldname_to_type.keys())

    # Return a dictionary of the field name to its inferred type
    return dict_fieldname_to_type


def build_schema_from_files(files, delimiter, encapsulator, encoding, true_values, false_values):
    """
    Build the schema from the data in multiple files.

    :param filepath: Path of the file to process.
    :param delimiter: Delimiter used in the CSV file.
    :param encapsulator: Encapsulator used in the CSV file.
    :param encoding: Encoding format of the CSV file.
    :param true_values: List of values deemed True.
    :param false_values: List of values deemed False.
    :return: Dictionary of the field name to inferred data type.
    """

    # Preconditions
    assert len(files) > 0

    module_logger.info("Building schema from files: %s" % files)

    num_files_processed = 0
    overall_schema = {}

    for file in files:
        module_logger.info("Going to infer schema from file: %s" % file)
        schema = build_schema_from_file(file, delimiter, encapsulator, encoding, true_values, false_values)

        if num_files_processed == 0:
            overall_schema = schema
        else:
            overall_schema = merge_field_types(overall_schema, schema)

        num_files_processed += 1

    module_logger.info("Processed %d files" % num_files_processed)

    # Return the inferred schema
    return overall_schema


def insert_data_from_files(files_to_process, delimiter, encapsulator, encoding, db_params, table_name, schema,
                           true_values, false_values):
    """
    Insert the data from a list of files into the database using the inferred schema.

    :param files_to_process: List of files to process.
    :param delimiter: Delimiter in the CSV file.
    :param encapsulator: Encapsulator in the CSV file.
    :param encoding: Encoding of the CSV file.
    :param db_params: Dictionary of database parameters.
    :param table_name: Name of the database table.
    :param schema: Dictionary of
    :param true_values:
    :param false_values:
    :return:
    """

    for file in files_to_process:
        module_logger.info("Inserting data from file: %s" % file)

        # Open the CSV file for reading
        csv_reader = DelimitedSource(file, delimiter, encapsulator, encoding)

        for data_dict in csv_reader.parse():
            insert_data(db_params, table_name, schema, data_dict, true_values, false_values)


def load_database(filepath, delimiter, encapsulator, encoding, true_values, false_values, db_params):

    # Preconditions
    assert type(delimiter) == str
    assert type(encapsulator) == str
    assert type(true_values) == list
    assert type(false_values) == list
    assert type(db_params) == dict

    module_logger.info("Processing files in: %s" % filepath)
    module_logger.info("CSV delimiter: %s" % delimiter)
    module_logger.info("CSV encapsulator: %s" % encapsulator)
    module_logger.info("CSV encoding: %s" % encoding)
    module_logger.info("Values defined as True: %s" % true_values)
    module_logger.info("Values defined as False: %s" % false_values)

    # Get the table names based on the files within the specified folder
    table_name_to_files = table_names_from_path(filepath)
    table_names = list(table_name_to_files.keys())
    module_logger.info("Table names: %s" % table_names)

    # If the database doesn't exist, create it
    create_database(db_params)

    # Walk through each table
    for table_name in table_names:
        module_logger.info("Processing table %s ..." % table_name)

        # Drop the tables that already exist in the database
        module_logger.info("Dropping table ...")
        drop_table(db_params, table_name)

        # Determine the schema of each of the table
        module_logger.info("Determining schema ...")
        files_to_process = table_name_to_files[table_name]
        schema = build_schema_from_files(files_to_process, delimiter, encapsulator, encoding, true_values, false_values)

        # Create the table
        module_logger.info("Creating table ...")
        create_table(db_params, table_name, schema)

        # Insert the data into the database
        module_logger.info("Inserting data ...")
        insert_data_from_files(files_to_process, delimiter, encapsulator, encoding, db_params, table_name, schema,
                               true_values, false_values)
