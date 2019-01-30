# -*- coding: utf-8 -*-
from database_loader.loader import load_database

if __name__ == '__main__':

    # Location where the raw-data is to be stored
    raw_data_path = "../raw-data/"

    # CSV parameters
    delimiter = ","
    encapsulator = "|"
    encoding = 'utf-8'

    # Expected values if the data is Boolean
    true_values = ["True"]
    false_values = ["False"]

    # Database parameters
    db_params = {"host": "192.168.99.100",
                 "user": "root",
                 "password": "pass",
                 "database-name": "comet"}

    # Load the SQL database
    load_database(raw_data_path, delimiter, encapsulator, encoding, true_values, false_values, db_params)
