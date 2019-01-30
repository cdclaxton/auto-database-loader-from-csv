import enum


class DataType(enum.Enum):
    """
    Class to represent the possible data types.
    """
    int = 0
    float = 1
    boolean = 2
    string = 3


def condition_check(str_value):
    """
    Can the String value be parsed to determine its type?

    :param str_value: String value to test.
    """

    if str_value is None or len(str_value.strip()) == 0:
        raise ValueError("Unable to determine the type of an empty string.")


def is_float(str_value):
    """
    Is the String value potentially a floating point value?

    :param str_value: String value to test.
    :return: Tuple of value is float (True/False) and value (float or None).
    """

    # Preconditions
    condition_check(str_value)

    # Is the value potentially a float?
    try:
        value = float(str_value)
        potentially_float = True
    except ValueError:
        value = None
        potentially_float = False

    return potentially_float, value


def is_int(str_value):
    """
    Is the String value potentially an integer?

    :param str_value: String value to test.
    :return: Tuple of value is int (True/False) and value (int or None).
    """

    # Preconditions
    condition_check(str_value)

    # Is the value potentially an int?
    try:
        value = int(str_value)
        potentially_int = True
    except ValueError:
        value = None
        potentially_int = False

    return potentially_int, value


def is_boolean(str_value, true_values, false_values):
    """
    Is the String value potentially a Boolean?

    :param str_value: String value to test.
    :param true_values: List of values that are defined as True.
    :param false_values: List of values that are defined as False.
    :return: Tuple of value is boolean (True/False) and value (boolean or None).
    """

    # Preconditions
    condition_check(str_value)
    if len(true_values) == 0 or len(false_values) == 0:
        raise ValueError("True/false values must be specified")

    if str_value in true_values:
        return True, True
    elif str_value in false_values:
        return True, False
    else:
        return False, None


def infer_type_and_value(str_value, true_values=["True"], false_values=["False"]):
    """
    Infer the type (and thus its value) of a String represenation of a value.

    :param str_value: Value to infer.
    :param true_values: List of values that are defined as True.
    :param false_values: List of values that are defined as False.
    :return: Tuple of (type, value).
    """

    # Precondition
    condition_check(str_value)

    # Try the different types
    int_result = is_int(str_value)
    if int_result[0]:
        return DataType.int, int_result[1]

    float_result = is_float(str_value)
    if float_result[0]:
        return DataType.float, float_result[1]

    boolean_result = is_boolean(str_value, true_values, false_values)
    if boolean_result[0]:
        return DataType.boolean, boolean_result[1]

    # None of the above types appear to be correct, so it might be a String
    return DataType.string, str_value


def infer_best_type(type1, type2):
    """
    Infer the most suitable type given two types.

    :param type1: Type 1.
    :param type2: Type 2.
    :return: Most suitable type.
    """

    #          | int    | float  | string | boolean
    # ---------|--------|--------|--------|---------
    #  int     | int    | float  | string | string
    #  float   | float  | float  | string | string
    #  string  | string | string | string | string
    #  boolean | string | string | string | boolean

    if type1 == DataType.string or type2 == DataType.string:
        return DataType.string

    if type1 == DataType.boolean and type2 == DataType.boolean:
        return DataType.boolean

    if type1 == DataType.int and type2 == DataType.int:
        return DataType.int

    if type1 == DataType.float and type2 == DataType.float:
        return DataType.float

    if (type1 == DataType.int and type2 == DataType.float) or (type1 == DataType.float and type2 == DataType.int):
        return DataType.float

    return DataType.string


def infer_overall_type(list_inferred_types):
    """
    Given a list of inferred types, determine the overall (most likely) type.

    :param list_inferred_types: List of types.
    :return: Overall type.
    """

    # Preconditions
    assert len(list_inferred_types) > 0

    best_type = list_inferred_types[0]
    for i in range(1, len(list_inferred_types)):
        best_type = infer_best_type(best_type, list_inferred_types[i])

    return best_type


def build_field_type(dict_data, true_values, false_values):
    """
    Build a dictionary of field name to type mappings.

    :param dict_data: Dictionary of data (field name to value).
    :param true_values: List of values deemed True.
    :param false_values: List of values deemed False.
    :return: Dictionary of field name to type.
    """

    # Preconditions
    assert type(dict_data) == dict
    assert len(true_values) > 0
    assert len(false_values) > 0

    # Create an empty dictionary to hold the field name and inferred type
    dict_field_name_to_type = {}

    # Walk through each of the fields
    for key in dict_data.keys():

        # Determine the type of the value (type, value)
        dict_field_name_to_type[key] = infer_type_and_value(dict_data[key], true_values, false_values)[0]

    return dict_field_name_to_type


def update_field_type(dict_fieldname_to_type, dict_data, true_values, false_values):
    """
    Update the field name to type dictionary.

    :param dict_fieldname_to_type: Dictionary of field name to type
    :param dict_data: Dictionary of field name to value (type of value to be inferred).
    :param true_values: List of values deemed True.
    :param false_values: List of values deemed False.
    :return: Updated field name to type dictionary.
    """

    # Preconditions
    assert type(dict_fieldname_to_type) == dict
    assert type(dict_data) == dict
    assert dict_fieldname_to_type.keys() == dict_data.keys()

    # Walk through each of the keys
    for key in dict_fieldname_to_type.keys():

        # Determine the type of the value (type, value)
        inferred_type = infer_type_and_value(dict_data[key], true_values, false_values)[0]

        # Determine the new best type for the field
        dict_fieldname_to_type[key] = infer_best_type(dict_fieldname_to_type[key], inferred_type)

    return dict_fieldname_to_type


def merge_field_types(dict_fieldname_to_type1, dict_fieldname_to_type2):
    """
    Merge the field name to type dictionaries.

    :param dict_fieldname_to_type1: First dictionary of field name to type.
    :param dict_fieldname_to_type2: Second dictionary of field name to type.
    :return: Merged dictionary of field name to type.
    """

    # Preconditions
    assert type(dict_fieldname_to_type1) == dict
    assert type(dict_fieldname_to_type2) == dict
    assert dict_fieldname_to_type1.keys() == dict_fieldname_to_type2.keys()

    # Walk through each key
    merged = {}
    for key in dict_fieldname_to_type1.keys():
        merged[key] = infer_best_type(dict_fieldname_to_type1[key], dict_fieldname_to_type2[key])

    return merged
