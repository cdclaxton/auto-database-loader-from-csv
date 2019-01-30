from data_generator.generate import filename_prefix, file_prefix_to_fieldname_mapping


def test_filename_prefix():
    assert filename_prefix(1, "test") == "m001_test_"
    assert filename_prefix(2, "test") == "m002_test_"


def test_file_prefix_to_fieldname_mapping():
    assert len(file_prefix_to_fieldname_mapping()) == 15
