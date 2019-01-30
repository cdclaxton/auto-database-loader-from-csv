# -*- coding: utf-8 -*-
from faker import Faker
import glob
import logging
import os

from logger import logger

# Initialise the module logger
logger.initialise_logger("data-generator", log_level=logging.INFO)
module_logger = logging.getLogger('data-generator')

# Initialise the Faker object (for generating synthetic data)
fake = Faker()


def remove_csv_files(filepath):
    """
    Remove all of the CSV files in the path specified.

    :param filepath: Location of the raw data folder.
    """

    # Build the path name
    if not filepath.endswith("/"):
        pathname = filepath + "/*.csv"
    else:
        pathname = filepath + "*.csv"

    files_to_remove = glob.glob(pathname)
    module_logger.info("Deleting %d file(s) from %s" % (len(files_to_remove), filepath))

    # Remove each of the files identified
    for p in files_to_remove:
        module_logger.info("Deleting file: %s" % p)
        os.remove(p)


def generate_sample(sample_id):
    """
    Generate a single sample of synthetic data.

    :param sample_id: Sample identifier.
    :return: Key-value pairs of synthetic data.
    """

    return {"id": sample_id,
            "first-name": fake.first_name(),
            "last-name": fake.last_name(),
            "alive": fake.boolean(chance_of_getting_true=80),
            "address": fake.address().replace("\n", ","),
            "phone-number": fake.phone_number(),
            "dob": fake.date(),
            "vrn": fake.license_plate(),
            "card-number": fake.credit_card_number(),
            "card-provider": fake.credit_card_provider(),
            "card-expiry-date": fake.credit_card_expire(start="now", end="+10y", date_format="%m/%y"),
            "card-country": fake.bank_country(),
            "company-name": fake.company(),
            "company-purpose": fake.bs(),
            "company-job-title": fake.job(),
            "reason": fake.text(max_nb_chars=200, ext_word_list=None)}


def filename_prefix(index, name):
    """
    Build the filename prefix (the first part of the output filename).

    :param index: Index number.
    :param name: Name of the module or attribute.
    :return: Filename prefix.
    """
    return "m" + str(index).zfill(3) + "_" + name + "_"


def file_prefix_to_fieldname_mapping():
    """
    Build the mapping of the filename prefix to the fields within the file.

    :return: Map of filename prefix to the list of fields within the file.
    """

    # Get a random sample of the data from which to get the field names
    sample = generate_sample(0)
    field_names = sample.keys()

    # Ensure there is an ID field (this occurs in every file)
    assert "id" in field_names

    # Get a list of the field names without the ID
    no_id_fields = set(field_names)
    no_id_fields.remove("id")

    # Create the mapping of field names to the filename
    mapping = [(filename_prefix(index, field_name), ['id', field_name]) for index, field_name in enumerate(no_id_fields)]

    # Return a map of the filename prefix to the field names within that file
    return dict(mapping)


def build_csv_row(sample, fields, delimiter, encapsulator):
    """
    Build a row of data.

    :param sample: Sample of data (in the form of a dict).
    :param fields: List of fields to write.
    :param delimiter: Delimiter to use between fields.
    :param encapsulator: Encapsulator to use (to allow the delimiter to be used in fields).
    :return: Row to write to a CSV file.
    """

    encapsulated_fields = [encapsulator + str(sample[field]) + encapsulator for field in fields]
    return delimiter.join(encapsulated_fields) + "\n"


def build_csv_header(fields, delimiter, encapsulator):
    """
    Build the header row of data.

    :param fields: List of fields to write.
    :param delimiter: Delimiter to use between fields.
    :param encapsulator: Encapsulator to use (to allow the delimiter to be used in fields).
    :return: Header row to write to a CSV file.
    """

    encapsulated_fields = [encapsulator + str(field) + encapsulator for field in fields]
    return delimiter.join(encapsulated_fields) + "\n"


def build_datasets(filepath, num_entries, max_entries_per_file, delimiter, encapsulator):
    """
    Build the CSV datasets.

    :param filepath: Path of the folder in which to store the CSV files.
    :param num_entries: Number of samples of synthetic data to write.
    :param max_entries_per_file: Maximum number of entries to write per file.
    :param delimiter: Delimiter to use in the CSV files.
    :param encapsulator: Encapsulator to use in the CSV files.
    """

    # Preconditions
    assert filepath[-1] == "/"
    assert num_entries > 0
    assert max_entries_per_file > 0

    # Get the mapping of field names of filename prefix
    mapping = file_prefix_to_fieldname_mapping()

    # Add on the full path to the prefix
    full_path_mapping = [(filepath + file_prefix, fields) for file_prefix, fields in mapping.items()]

    # Initialise the file index
    file_index = 0
    num_rows_written = {}

    for i in range(num_entries):

        # Determine if a new file needs to be written
        if i != 0 and i % max_entries_per_file == 0:
            file_index += 1
            module_logger.info("Starting to write to file index: %d" % file_index)

        # Generate the random sample of data with a given ID
        sample = generate_sample(i)

        # Walk through each file that needs to be written
        for file_path_minus_ext, fields in full_path_mapping:

            # Build the full path of the CSV file to add the data to
            full_path = file_path_minus_ext + str(file_index) + ".csv"

            with open(full_path, 'a') as fp:

                if full_path not in num_rows_written:
                    num_rows_written[full_path] = 1
                    header = build_csv_header(fields, delimiter, encapsulator)
                    fp.write(header)
                else:
                    num_rows_written[full_path] += 1

                row = build_csv_row(sample, fields, delimiter, encapsulator)
                fp.write(row)

    module_logger.info("Generated %d samples" % num_entries)


def generate_raw_data(filepath, num_entries, max_entries_per_file, delimiter, encapsulator):
    """
    Generate raw data (to simulate the modules and attributes from NetReveal).

    :param filepath: Folder where the generated data in CSV files will be stored.
    :param num_entries: Number of samples to generate.
    :param max_entries_per_file: Maximum number of samples per file (before a file is split).
    :param delimiter: Delimiter to use in the generated CSV files.
    :param encapsulator: Field encapsulator to use in the generated CSV files.
    """

    # Preconditions
    assert os.path.isdir(filepath)
    assert num_entries > 0
    assert max_entries_per_file > 0
    assert isinstance(delimiter, str)
    assert isinstance(encapsulator, str)

    # Log the parameters
    module_logger.info("Path for the raw data: %s" % filepath)
    module_logger.info("Number of entries to generate: %d" % num_entries)
    module_logger.info("CSV file delimiter: %s" % delimiter)
    module_logger.info("CSV file encapsulator: %s" % encapsulator)

    # Remove any CSV files in the output directory
    remove_csv_files(filepath)

    # Create the data files
    build_datasets(filepath, num_entries, max_entries_per_file, delimiter, encapsulator)
    module_logger.info("Datasets written to: %s" % filepath)
