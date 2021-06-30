#!/usr/bin/env python3

"""
Base class
"""


class Base:
    """
    Base class
    """

    def __init__(self, logger=None):
        self.logger = logger

    def log_debug(self, message: str):
        """
        Log a debug message
        """
        if self.logger is None:
            print(message)
        else:
            self.logger.debug(message)

    def log_error(self, message: str):
        """
        Log an error message
        """
        if self.logger is None:
            print(message)
        else:
            self.logger.error(message)

    def log_info(self, message: str):
        """
        Log an info message
        """
        if self.logger is None:
            print(message)
        else:
            self.logger.info(message)
