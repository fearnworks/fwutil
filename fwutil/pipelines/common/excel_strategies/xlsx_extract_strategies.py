import glob
import os
from typing import Any, Dict

import openpyxl
import pandas as pd

import utils as Utils


class LocalExcelSheetExtractStrategy():
    """
    A class that represents a strategy for extracting data from an Excel sheet in Local Storage.
    """

    def __init__(
        self,
        file_path: str,
        sheet_name: str,
    ):
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        if not os.path.exists(file_path):
            self.logger.error("File does not exist at the provided path.:%s", file_path)
            raise FileNotFoundError("File does not exist at the provided path.")
        self.file_path = file_path
        self.sheet_name = sheet_name

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from the specified Excel sheet in Local Storage.

        Returns:
            pd.DataFrame: The extracted data as a pandas DataFrame.

        Raises:
            FileNotFoundError: If the file or the sheet does not exist.
            Exception: If there's any other error during the extraction process.
        """
        try:
            self.logger.info(f"Reading excel file from {self.file_path}")
            with pd.ExcelFile(self.file_path) as xls:
                if self.sheet_name not in xls.sheet_names:
                    self.logger.error(f"Sheet {self.sheet_name} not found in {self.file_path}")
                    raise FileNotFoundError(f"Sheet {self.sheet_name} not found in {self.file_path}")
                df = pd.read_excel(xls, self.sheet_name).convert_dtypes(dtype_backend='pyarrow')
            self.logger.info(f"Successfully read excel file from {self.file_path}")
            return df
        except Exception as e:
            self.logger.exception(e)
            raise


class LocalExcelWorkbookExtractStrategy():
    """
    A class that represents a strategy for extracting data from an Excel workbook in Local Storage.
    """

    def __init__(self, file_path: str):
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        if not os.path.exists(file_path):
            self.logger.error("File does not exist at the provided path :%s", file_path)
            raise Exception("File does not exist at the provided path.")
        self.file_path = file_path

    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extracts data from the specified Excel workbook in Local Storage.

        Returns:
            Dict[str, pd.DataFrame]: The extracted data as a dictionary of pandas DataFrames.

        Raises:
            Exception: If there's any error during the extraction process.
        """
        try:
            self.logger.info(f"Reading excel workbook from {self.file_path}")
            xls = pd.read_excel(self.file_path, sheet_name=None).convert_dtypes(dtype_backend='pyarrow')
            self.logger.info(f"Successfully read excel workbook from {self.file_path}")
            return xls
        except Exception as e:
            self.logger.error(f"Error during extraction: {str(e)}")
            raise Exception(f"Error during extraction: {str(e)}")

class LocalDirectoryExcelExtractStrategy():
    """
    A class that represents a strategy for extracting data from all Excel files in a directory.
    """

    def __init__(self, directory_path: str):
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        if not os.path.exists(directory_path):
            self.logger.error("Directory does not exist at the provided path : %s", directory_path)
            raise FileNotFoundError("Directory does not exist at the provided path.")
        self.directory_path = directory_path

    def extract(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Extracts data from all Excel files in the specified directory.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: The extracted data as a dictionary of dictionaries of pandas DataFrames.

        Raises:
            FileNotFoundError: If the directory or any file or sheet does not exist.
            Exception: If there's any other error during the extraction process.
        """
        data = {}
        try:
            self.logger.info(f"Reading all excel files from directory {self.directory_path}")
            for file in glob.glob(os.path.join(self.directory_path, '*.xlsx')):
                try:
                    self.logger.info(f"Reading excel file {file}")
                    with pd.ExcelFile(file) as xls:
                        for sheet_name in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name).convert_dtypes(dtype_backend='pyarrow')
                            # Extract the filename without the extension
                            filename = os.path.splitext(os.path.basename(file))[0]
                            # Create the key in the filename.sheetname format
                            key = f"{filename}.{sheet_name}"
                            data[key] = df
                    self.logger.info(f"Successfully read excel file {file}")
                except Exception as e:
                    self.logger.exception(e)
                    raise
            self.logger.info(f"Successfully read all excel files from directory {self.directory_path}")
            return data
        except Exception as e:
            self.logger.exception(e)
            raise
