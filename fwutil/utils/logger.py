import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

import pandas as pd
from IPython.display import HTML, display


class JupyterDisplayHandler(logging.Handler):
    """
    Custom logging handler sending logs to Jupyter notebook display.

    This handler extends the logging.Handler class and overrides the emit method to
    display the log message in a Jupyter notebook using the display function.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """
        Write the log message formatted for the Jupyter notebook.

        Args:
            record (LogRecord): LogRecord object containing the log information.
        """
        log_entry = self.format(record)
        for arg in record.args:
            if isinstance(arg, pd.DataFrame):
                log_entry = log_entry.replace(str(arg), arg.to_html())
        display(HTML(log_entry))


class PipelineLogger:
    """
    Centralized configurable logging class.

    This class provides methods for configuring the logger according to given configurations
    and getting a logger with a given name.
    """

    log_level: int = logging.INFO
    console_log_level: int = logging.INFO
    file_log_level: int = logging.DEBUG
    log_file: str = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_format: str = (
        "%(asctime)s - %(name)s - (line: %(lineno)d) - %(levelname)s - %(message)s"
    )
    is_jupyter: bool = (
        "ipykernel" in sys.modules
    )  # Check if running in Jupyter notebook

    @staticmethod
    def config_logger(configs: Dict[str, Any], data_dir: Optional[str] = None) -> None:
        """
        Configures the logger according to the given configurations.

        Args:
            configs (dict): A dictionary of configurations.
            data_dir (str, optional): A path to the directory where the log file will be created.
        """
        if data_dir is not None:
            PipelineLogger.log_file = os.path.join(data_dir, PipelineLogger.log_file)

        for key, value in configs.items():
            if key == "log_file":
                value = (
                    os.path.splitext(value)[0]
                    + f"_{datetime.now().strftime('%Y-%m-%D-%H-%M-%S')}"
                    + os.path.splitext(value)[1]
                )
                if data_dir is not None:
                    value = os.path.join(data_dir, value)
            if hasattr(PipelineLogger, key):
                setattr(PipelineLogger, key, value)

        pd.set_option("display.max_columns", 500)
        pd.set_option("display.max_rows", 500)
        pd.set_option("display.width", 1000)
        pd.set_option("display.max_colwidth", 300)

        console_handler = None
        try:
            if PipelineLogger.is_jupyter:  # Check if running in Jupyter notebook
                console_handler = JupyterDisplayHandler()
                console_handler.setLevel(PipelineLogger.console_log_level)
        except NameError:
            pass

        if console_handler is None:  # Not in Jupyter notebook or something went wrong
            console_handler = logging.StreamHandler()
            console_handler.setLevel(PipelineLogger.console_log_level)

        file_handler = RotatingFileHandler(
            PipelineLogger.log_file, maxBytes=10000000, backupCount=5
        )
        file_handler.setLevel(PipelineLogger.file_log_level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter(PipelineLogger.log_format)
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Get root logger and add handlers to it
        logger = logging.getLogger()
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    @staticmethod
    def get_logger(
        name: str,
        level: Optional[int] = None,
        log_file: Optional[str] = None,
        format: Optional[str] = None,
        handlers: Optional[List[logging.Handler]] = None,
    ) -> logging.Logger:
        """
        Gets a logger with the given name.

        Args:
            name (str): The name of the logger.
            level (int, optional): The log level of the logger.
            log_file (str, optional): A path to the log file
            format (str, optional): A custom format for the logger
            handlers (list, optional): A list of logging.Handler objects to add to the logger

        Returns:
            logger (logging.Logger): A logger with the given configurations
        """
        logger: logging.Logger = logging.getLogger(name)
        if level is not None:
            logger.setLevel(level)
        else:
            logger.setLevel(PipelineLogger.log_level)

        if not logger.handlers:
            if format is None:
                format = PipelineLogger.log_format
            formatter = logging.Formatter(format)
            for handler in logger.handlers:
                handler.setFormatter(formatter)

        if handlers is not None:
            for handler in handlers:
                logger.addHandler(handler)

        # Log the location of the log file
        log_file_abs_path = os.path.abspath(PipelineLogger.log_file)
        logger.info(f"Log file is located at: {log_file_abs_path}")

        return logger
