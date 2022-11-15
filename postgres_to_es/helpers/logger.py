import copy
import logging.config

from helpers.utils import SingletonType


class ColoredConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        # Need to make a actual copy of the record
        # to prevent altering the message for other loggers
        current_record = copy.copy(record)
        level = current_record.levelno
        if level >= 50:  # CRITICAL / FATAL
            color = '\x1b[31m'  # red
        elif level >= 40:  # ERROR
            color = '\x1b[31m'  # red
        elif level >= 30:  # WARNING
            color = '\x1b[33m'  # yellow
        elif level >= 20:  # INFO
            color = '\x1b[32m'  # green
        elif level >= 10:  # DEBUG
            color = '\x1b[35m'  # pink
        else:  # NOTSET and anything else
            color = '\x1b[0m'  # normal

        current_record.msg = color + str(current_record.msg) + '\x1b[0m'  # normal
        current_record.levelname = color + str(current_record.levelname) + '\x1b[0m'  # normal
        logging.StreamHandler.emit(self, current_record)


class LoggerFactory(metaclass=SingletonType):
    _logger = None

    LOGGER_NAME = "app_logger"
    LOGGER_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default_formatter": {
                "format": "[%(levelname)s:%(asctime)s | %(module)s:%(funcName)s:%(lineno)s:%(name)s] %(message)s"
            },
        },
        "handlers": {
            "stream_handler": {
                "class": "helpers.logger.ColoredConsoleHandler",
                "formatter": "default_formatter",
            },
        },
        # Loggers use the handler names declared above
        "loggers": {
            LOGGER_NAME: {
                "handlers": ["stream_handler"],
                "level": "DEBUG",
                "propagate": True
            }
        }
    }

    def __init__(self):
        logging.config.dictConfig(self.LOGGER_CONFIG)
        self._logger = logging.getLogger(self.LOGGER_NAME)
        self._logger.debug("New logger instance was generated")

    def get_logger(self):
        return self._logger
