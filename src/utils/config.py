"""
config.py
---------
Module with utility functions to load configuration files.

Author: Ezau Faridh Torres Torres.
Date: April 2026.

Functions
---------
- load_params :
    Loads parameters from a JSON configuration file.
- create_logger :
    Creates a logger with the specified name and log path.
"""
# Necessary imports
from json import load
from typing import Dict, Any
import logging
import os

def load_params() -> Dict[str, Any]:
    """
    Load parameters from a JSON configuration file.
    """
    with open ("../config/params.json", "r") as f:
        return load(f)
    
def load_contact_codes() -> Dict:
    """
    Load contact codes from a JSON configuration file.
    """
    with open ("../config/contact_codes.json", "r") as f:
        return load(f)

class LoggerFactory:

    @staticmethod
    def create_logger(
        name: str = "app",
        log_path: str = "./logs.log",
        level: int = logging.INFO,
        mode : str = "w",
        encoding = None,
        console: bool = True
    ) -> logging.Logger:
        """
        Create a logger with the specified name and log path.

        Parameters
        ----------
        name : str
            The name of the logger.
        log_path : str
            The path to the log file.
        level : int
            The logging level.
        """
        # Create directory.
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # Create logger.
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if logger.handlers:
            return logger

        # Handler to file.
        file_handler = logging.FileHandler(
            log_path,
            mode=mode,
            encoding=encoding
        )
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

        # Format.
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)s | %(name)s : %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)

        # Handler to console.
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger