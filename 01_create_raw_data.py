# -*- coding: utf-8 -*-
from data_generator.generate import generate_raw_data

if __name__ == '__main__':

    # Location where the raw-data (simulating the modules and attributes) is to be stored
    raw_data_path = "../raw-data/"

    # Number of rows of data to generate
    num_entries = 10

    # Maximum number of entries to write to a single file
    max_entries_per_file = 5

    # CSV parameters
    delimiter = ","
    encapsulator = "|"

    # Generate the data
    generate_raw_data(raw_data_path, num_entries, max_entries_per_file, delimiter, encapsulator)
