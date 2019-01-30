from database_loader.type_inference import is_float, is_int, is_boolean, infer_type_and_value, infer_overall_type, \
    DataType, infer_best_type, build_field_type, update_field_type, merge_field_types


def test_is_float():
    assert is_float("1.1") == (True, 1.1)
    assert is_float("1.23e4") == (True, 1.23e4)
    assert is_float("0.0") == (True, 0.0)

    assert is_float("hello") == (False, None)
    assert is_float("true") == (False, None)


def test_is_int():
    assert is_int("1") == (True, 1)
    assert is_int("10") == (True, 10)

    assert is_int("100e3") == (False, None)
    assert is_int("hello") == (False, None)
    assert is_int("true") == (False, None)
    assert is_int("1.0") == (False, None)


def test_is_boolean():
    assert is_boolean("True", ["True"], ["False"]) == (True, True)
    assert is_boolean("False", ["True"], ["False"]) == (True, False)

    assert is_boolean("true", ["True"], ["False"]) == (False, None)
    assert is_boolean("false", ["True"], ["False"]) == (False, None)
    assert is_boolean("cats", ["True"], ["False"]) == (False, None)
    assert is_boolean("1.1", ["True"], ["False"]) == (False, None)
    assert is_boolean("10", ["True"], ["False"]) == (False, None)


def test_infer_type_and_value():
    assert infer_type_and_value("1") == (DataType.int, 1)
    assert infer_type_and_value("10") == (DataType.int, 10)
    assert infer_type_and_value("100") == (DataType.int, 100)

    assert infer_type_and_value("1.0") == (DataType.float, 1.0)
    assert infer_type_and_value("0.0") == (DataType.float, 0.0)
    assert infer_type_and_value("12e3") == (DataType.float, 12e3)

    assert infer_type_and_value("True") == (DataType.boolean, True)
    assert infer_type_and_value("False") == (DataType.boolean, False)

    assert infer_type_and_value("hello") == (DataType.string, "hello")
    assert infer_type_and_value("true") == (DataType.string, "true")


def test_infer_best_type():
    #          | int | float | string | boolean
    #  --------|-----|-------|--------|---------
    #  int     |  x  |   x   |    x   |    x
    #  float   |     |   x   |    x   |    x
    #  string  |     |       |    x   |    x
    #  boolean |     |       |        |    x

    assert infer_best_type(DataType.int, DataType.int) == DataType.int
    assert infer_best_type(DataType.int, DataType.float) == DataType.float
    assert infer_best_type(DataType.int, DataType.string) == DataType.string
    assert infer_best_type(DataType.int, DataType.boolean) == DataType.string

    assert infer_best_type(DataType.float, DataType.float) == DataType.float
    assert infer_best_type(DataType.float, DataType.string) == DataType.string
    assert infer_best_type(DataType.float, DataType.boolean) == DataType.string

    assert infer_best_type(DataType.string, DataType.string) == DataType.string
    assert infer_best_type(DataType.string, DataType.boolean) == DataType.string

    assert infer_best_type(DataType.boolean, DataType.boolean) == DataType.boolean


def test_infer_overall_type():

    # String only
    assert infer_overall_type([DataType.string]) == DataType.string
    assert infer_overall_type([DataType.string, DataType.string]) == DataType.string

    # Int only
    assert infer_overall_type([DataType.int]) == DataType.int
    assert infer_overall_type([DataType.int, DataType.int]) == DataType.int

    # Float only
    assert infer_overall_type([DataType.float]) == DataType.float
    assert infer_overall_type([DataType.float]) == DataType.float

    # Boolean only
    assert infer_overall_type([DataType.boolean]) == DataType.boolean
    assert infer_overall_type([DataType.boolean, DataType.boolean]) == DataType.boolean

    # Int and float
    assert infer_overall_type([DataType.int, DataType.float]) == DataType.float

    # Boolean and float
    assert infer_overall_type([DataType.boolean, DataType.float]) == DataType.string

    # Int and boolean
    assert infer_overall_type([DataType.int, DataType.boolean]) == DataType.string

    # String and other types
    assert infer_overall_type([DataType.string, DataType.int]) == DataType.string
    assert infer_overall_type([DataType.string, DataType.float]) == DataType.string
    assert infer_overall_type([DataType.string, DataType.boolean]) == DataType.string


def test_build_field_type():
    assert build_field_type({"field-a": "hello"}, ["True"], ["False"]) == {"field-a": DataType.string}
    assert build_field_type({"field-a": "True"}, ["True"], ["False"]) == {"field-a": DataType.boolean}
    assert build_field_type({"field-a": "1"}, ["True"], ["False"]) == {"field-a": DataType.int}
    assert build_field_type({"field-a": "1.1"}, ["True"], ["False"]) == {"field-a": DataType.float}

    assert build_field_type({"field-a": "hello", "field-b": "1"}, ["True"], ["False"]) == \
           {"field-a": DataType.string, "field-b": DataType.int}


def test_update_field_type():
    assert update_field_type({"field-a": DataType.string}, {"field-a": "hello"}, ["True"], ["False"]) == \
           {"field-a": DataType.string}
    assert update_field_type({"field-a": DataType.int}, {"field-a": "3"}, ["True"], ["False"]) == \
           {"field-a": DataType.int}
    assert update_field_type({"field-a": DataType.boolean}, {"field-a": "True"}, ["True"], ["False"]) == \
           {"field-a": DataType.boolean}
    assert update_field_type({"field-a": DataType.float}, {"field-a": "3.2"}, ["True"], ["False"]) == \
           {"field-a": DataType.float}


def test_merge_field_types():
    assert merge_field_types({"field-a": DataType.string}, {"field-a": DataType.int}) == {"field-a": DataType.string}
    assert merge_field_types({"field-a": DataType.int}, {"field-a": DataType.int}) == {"field-a": DataType.int}

    assert merge_field_types({"field-a": DataType.string, "field-b": DataType.int},
                             {"field-a": DataType.int, "field-b": DataType.int}) == \
           {"field-a": DataType.string, "field-b": DataType.int}
