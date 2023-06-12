from io import BytesIO

import azure_interface as AzureInterface
import pandas as pd
import pipelines.base as PipelineBases
import utils as Utils


class AzureBlobStorageBaseStrategy:
    """
    A base class for Azure Blob Storage strategies.

    Attributes:
        az_mgr (AzureInterface.AzureBlobStorageManager): The Azure Blob Storage manager instance.
        container_name (str): The name of the Azure Blob Storage container.
        blob_name (str): The name of the blob.
    """

    def __init__(
        self,
        az_mgr: AzureInterface.AzureBlobStorageManager,
        container_name: str,
        blob_name: str,
    ):
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        self.az_mgr: AzureInterface.AzureBlobStorageManager = az_mgr
        self.container_name = container_name
        self.blob_name = blob_name


class AzureBlobStorageExtractStrategy(
    AzureBlobStorageBaseStrategy, PipelineBases.ExtractStrategy
):
    """
    A class that represents a strategy for extracting data from Azure Blob Storage.
    """

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from the specified blob in Azure Blob Storage.

        Returns:
            pd.DataFrame: The extracted data as a pandas DataFrame.

        Raises:
            Exception: If there's any error during the extraction process.
        """
        self.logger.info(
            "Extracting data from blob: "
            + self.blob_name
            + " in container: "
            + self.container_name
        )
        df = self.az_mgr.load_blob_to_dataframe(self.container_name, self.blob_name)
        self.logger.info(f"Shape of the extracted data: {df.shape}")
        return df


class AzureBlobStorageExcelSheetExtractStrategy(
    AzureBlobStorageBaseStrategy, PipelineBases.ExtractStrategy
):
    """
    A class that represents a strategy for extracting data from an Excel sheet in Azure Blob Storage.
    """

    def __init__(
        self,
        az_mgr: AzureInterface.AzureBlobStorageManager,
        container_name: str,
        blob_name: str,
        sheet_name: str,
    ):
        super().__init__(az_mgr, container_name, blob_name)
        self.sheet_name = sheet_name

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from the specified Excel sheet in Azure Blob Storage.

        Returns:
            pd.DataFrame: The extracted data as a pandas DataFrame.

        Raises:
            Exception: If there's any error during the extraction process.
        """
        df = self.az_mgr.load_blob_sheet_to_dataframe(
            self.container_name, self.blob_name, self.sheet_name
        )
        return df


class AzureBlobStorageParquetLoadStrategy(
    AzureBlobStorageBaseStrategy, PipelineBases.LoadStrategy
):
    """
    A class that represents a strategy for loading data into Azure Blob Storage in Parquet format.
    """

    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Loads the data into Azure Blob Storage in Parquet format.

        Args:
            data (pd.DataFrame): The data to load.

        Returns:
            pd.DataFrame: The loaded data as a pandas DataFrame.

        Raises:
            Exception: If there's any error during the loading process.
        """
        file_data = BytesIO()
        data.to_parquet(file_data)
        self.az_mgr.upload_blob(self.container_name, self.blob_name, file_data)
        return data


class AzureBlobStorageExcelLoadStrategy(
    AzureBlobStorageBaseStrategy, PipelineBases.LoadStrategy
):
    """
    A class that represents a strategy for loading data into Azure Blob Storage in Excel format.
    """

    EXCEL_SHEET_NAME = "Sheet1"

    def load(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Loads the data into Azure Blob Storage in Excel format.

        Args:
            data (pd.DataFrame): The data to load.

        Returns:
            pd.DataFrame: The loaded data as a pandas DataFrame.

        Raises:
            Exception: If there's any error during the loading process.
        """
        file_data = BytesIO()
        with pd.ExcelWriter(file_data, engine="xlsxwriter") as writer:
            data.to_excel(writer, sheet_name=self.EXCEL_SHEET_NAME, index=False)
        file_data.seek(0)
        self.az_mgr.upload_blob(self.container_name, self.blob_name, file_data)
        return data
