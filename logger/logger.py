import logging


def initialise_logger(logger_name, log_level=logging.INFO):
    """Initialise the logger."""

    # Create a logger for the application
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Create a file handler
    fh = logging.FileHandler('./logs/loader.log')
    fh.setLevel(log_level)

    # Create a console handler to log to the console
    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # Clear the handlers and add the logging handlers
    logger.handlers = []
    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("Logging initialised")
