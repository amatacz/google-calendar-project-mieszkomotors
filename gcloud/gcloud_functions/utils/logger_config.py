import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    """
    Logging configuration used in local env.
    (Google Cloud collects logs in Logs Explorer)
    """

    logging_client = logging.Client()
    logger = logging_client.logger("google_events_logger")

    # Create handlers for displaying msgs in console (c_handler) and write them to the file (f_handler)
    c_handler = logging.StreamHandler()
    f_handler = RotatingFileHandler('../logs/google_events_logger.log', maxBytes=2000, backupCount=5, encoding='utf-8')
    c_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.DEBUG)

    # Create formatters and add them to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger