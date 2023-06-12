import logging
from io import BytesIO
from typing import Dict, Optional, Union

import pandas as pd
from azure.storage.blob import BlobProperties, ContainerClient

logger = logging.getLogger(__name__)


class BlobDataLoader:
    """
    A class for loading blob data from an Azure Blob Storage container.
    """

    @staticmethod
    def get_pandas_loader(content_type: str) -> str:
        """
        Maps content types to the corresponding pandas loading function.

        Args:
            content_type (str): The content type of the blob.

        Returns:
            str: The name of the pandas loading function to use.
        """
        content_type_mapping = {
            "text/csv": "read_csv",
            "application/json": "read_json",
            "application/vnd.ms-excel": "read_excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "read_excel",
            "application/octet-stream": "read_parquet",
            "text/html": "read_html",
            "text/xml": "read_xml",
        }
        return content_type_mapping.get(content_type)

    @staticmethod
    def load_blob_to_dataframe(
        blob_data: bytes, content_type: str
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Loads the content of a blob into a pandas DataFrame.

        Args:
            blob_data (bytes): The data from the blob.
            content_type (str): The content type of the blob.

        Returns:
            Union[pd.DataFrame, Dict[str, pd.DataFrame]]: The blob data loaded into a DataFrame. If the blob contains multiple sheets (as in an Excel file), returns a dictionary where keys are sheet names and values are DataFrames.
        """
        try:
            loader = BlobDataLoader.get_pandas_loader(content_type)
            logger.info(f"Content type: {content_type}")
            logger.info(f"Loader: {loader}")
            if loader is None:
                return None

            with BytesIO(blob_data) as data:
                if loader == "read_xml":
                    data = data.getvalue().decode("utf-8")
                elif loader == "read_excel":
                    df = pd.read_excel(data, sheet_name=None)
                    logger.info(f"Loaded Excel data: {df}")
                    return df
                else:
                    df = getattr(pd, loader)(data)
                    logger.info(f"Loaded data: {df}")
                    return df
        except Exception as ex:
            logger.error(f"Failed to load blob to DataFrame - {str(ex)}")
            return None

    @staticmethod
    def load_blob_content(container_client: ContainerClient, blob_name: str) -> bytes:
        """
        Load the content of a single blob.

        Args:
            container_client (ContainerClient): The client for the container where the blob is located.
            blob_name (str): The name of the blob to load.

        Returns:
            bytes: The content of the blob.
        """
        try:
            return container_client.download_blob(blob_name).content_as_bytes()
        except Exception as ex:
            logger.error(f"Failed to load blob '{blob_name}' - {str(ex)}")
            return b""
