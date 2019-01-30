# -*- coding: utf-8 -*-
import csv
import logging
import os
from logger import logger

# Initialise the module logger
logger.initialise_logger("loader", log_level=logging.INFO)
module_logger = logging.getLogger('loader')


class DelimitedSource(object):

    # Maximum number of characters in a single field
    FIELD_LIMIT = 10000000

    def __init__(self, filepath, delimiter, encapsulator, encoding):
        self.filepath = filepath
        self.delimiter = delimiter
        self.encapsulator = encapsulator
        self.encoding = encoding

        module_logger.info("Initialising CSV reader to read: %s" % self.filepath)
        module_logger.info("Delimiter set to: %s" % delimiter)
        module_logger.info("Encapsulator set to: %s" % encapsulator)
        module_logger.info("Encoding set to: %s " % self.encoding)

    def parse(self):

        # Preconditions
        if not os.path.isfile(self.filepath):
            raise ValueError("File path isn't valid: %s" % self.filepath)

        # Change the limit on the size of a field
        csv.field_size_limit(self.FIELD_LIMIT)

        # Open the file for reading
        with open(self.filepath, 'r', encoding=self.encoding) as fp:
            reader = csv.reader(fp, delimiter=self.delimiter, quotechar=self.encapsulator)

            # Get the header
            field_names = next(reader)
            if field_names is None:
                raise ValueError("Unable to read the header of the CSV file")

            # Create the generator for reading a line at a time
            for line in reader:
                yield dict(zip(field_names, line))
