import json
import logging.config

# Global private-ish flag for logging setup.
_IS_LOGGING_SET_UP = False


def setup_logging(logger_name, logging_config="logging.json"):
    global _IS_LOGGING_SET_UP

    # Load the logger configuration only once.
    if not _IS_LOGGING_SET_UP:
        with open(logging_config, "r") as f:
            logging.config.dictConfig(json.load(f))
        _IS_LOGGING_SET_UP = True

    return logging.getLogger(logger_name)
