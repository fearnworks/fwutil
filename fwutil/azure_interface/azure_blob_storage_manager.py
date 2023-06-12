import logging
import os
import time
from io import BytesIO
from typing import Dict, List, Union

import azure_interface as AzureInterface
import pandas as pd
import utils as Utils
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient


class AzureBlobStorageManager:
    """
    A class that manages interactions with Azure Blob Storage.

    This class provides methods for uploading, downloading, and deleting blobs. It also initializes a dictionary of
    ContainerClients for each container in the storage account upon instantiation.

    Attributes:
        logger (logging.Logger): The logger to be used.
        storage_account_key (str): The storage account key, retrieved from environment variables.
        storage_account_name (str): The storage account name, retrieved from environment variables.
        connection_string (str): The connection string used to connect to Azure Blob Storage.
        blob_service_client (BlobServiceClient): The BlobServiceClient used to interact with Azure Blob Storage.
        container_names (List[str]): A list of names of containers in the storage account.
        container_clients (Dict[str, ContainerClient]): A dictionary where keys are container names and values are the corresponding ContainerClients.
    """

    logger: logging.Logger

    def __init__(self) -> None:
        """
        Initialize the AzureBlobStorageManager and read environment variables.
        """
        # Set up the logger
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        self.logger.info("Initializing AzureBlobStorageManager...")

        # Load environment variables from .env file
        self.storage_account_key: str = os.getenv("STORAGE_ACCOUNT_KEY")
        self.storage_account_name: str = os.getenv("STORAGE_ACCOUNT_NAME")
        self.endpoint_suffix: str = os.getenv("AZURE_ENDPOINT_SUFFIX")

        # Log the names of environment variables loaded
        self.logger.info(f"Loaded STORAGE_ACCOUNT_KEY: {self.storage_account_key}")
        self.logger.info(f"Loaded STORAGE_ACCOUNT_NAME: {self.storage_account_name}")
        self.logger.info(f"Loaded STORAGE_ACCOUNT_NAME: {self.endpoint_suffix}")

        self.connection_string: str = self.build_connection_string()

        # Log the connection string
        self.logger.info(f"Built connection string: {self.connection_string}")

        # Initialize BlobServiceClient
        try:
            self.blob_service_client: BlobServiceClient = (
                BlobServiceClient.from_connection_string(self.connection_string)
            )
            self.logger.info("BlobServiceClient initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize BlobServiceClient: {str(e)}")
            raise

        # Dynamically pull the list of container names from the storage account
        try:
            self.container_names: List[str] = [
                container.name
                for container in self.blob_service_client.list_containers()
            ]
            self.logger.info(f"Retrieved container names: {self.container_names}")
        except Exception as e:
            self.logger.error(f"Failed to retrieve container names: {str(e)}")
            raise

        # Initialize a dictionary of ContainerClients for the specified containers
        try:
            self.container_clients = {
                container_name: self.blob_service_client.get_container_client(
                    container_name
                )
                for container_name in self.container_names
            }
            self.logger.info("ContainerClients initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize ContainerClients: {str(e)}")
            raise

        self.logger.info("AzureBlobStorageManager initialized successfully.")

    def build_connection_string(self) -> str:
        """
        Builds the connection string used to connect to Azure Blob Storage.

        Returns:
            str: The connection string.
        """
        conn_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key}==;EndpointSuffix={self.endpoint_suffix}"
        return conn_string

    def upload_blob(
        self, container_name: str, blob_name: str, file_data: Union[str, BytesIO]
    ) -> None:
        """
        Uploads data to a blob in a specified container.

        This function supports uploading data either from a file path or a BytesIO object.

        Args:
            container_name (str): The name of the container where the blob will be uploaded.
            blob_name (str): The name of the blob to upload the data to.
            file_data (Union[str, BytesIO]): The data to be uploaded. If a string is provided,
                                            it is assumed to be a file path. If a BytesIO object is provided,
                                            the data is uploaded directly.
        """
        upload_response = None  # Initialize upload_response
        container_client = self.container_clients.get(container_name)
        self.logger.info(
            f"Uploading data to blob '{blob_name}' in container '{container_name}'"
        )
        if container_client:
            try:
                if isinstance(file_data, str):
                    self.logger.info(
                        f"Detected file data as string, attempting to open file at path: {file_data}"
                    )
                    with open(file_data, "rb") as file:
                        upload_response = container_client.upload_blob(
                            name=blob_name, data=file, overwrite=True
                        )
                elif isinstance(file_data, BytesIO):
                    self.logger.info(
                        f"Detected file data as BytesIO, uploading directly."
                    )
                    upload_response = container_client.upload_blob(
                        name=blob_name, data=file_data.getvalue(), overwrite=True
                    )
                else:
                    self.logger.error(
                        f"Unsupported data type for file_data: {type(file_data).__name__}"
                    )
                    return

                self.logger.info(f"Finished uploading data to blob '{blob_name}'")
                # Check if the blob was successfully uploaded
                blob_client = container_client.get_blob_client(blob_name)
                if blob_client.exists():
                    self.logger.info(
                        f"Successfully uploaded data to blob '{blob_name}'"
                    )
                else:
                    self.logger.error(
                        f"Failed to upload data to blob '{blob_name}', blob does not exist after upload attempt."
                    )
            except Exception as ex:
                self.logger.error(
                    f"Failed to upload data to blob '{blob_name}' - Type: {type(ex).__name__}, Message: {str(ex)}"
                )
                self.logger.error(f"Upload response: {upload_response}")
        else:
            self.logger.error(f"Container '{container_name}' not found")

    def download_blob(
        self, container_name: str, blob_name: str, download_path: str
    ) -> None:
        """
        Downloads data from a blob to a specified file path.

        Args:
            container_name (str): The name of the container where the blob is located.
            blob_name (str): The name of the blob to download the data from.
            download_path (str): The file path where the downloaded data will be stored.
        """
        container_client = self.container_clients.get(container_name)
        if container_client:
            self.logger.info(f'Downloading blob "{blob_name}" to "{download_path}"')
            blob_client = container_client.get_blob_client(blob_name)
            with open(download_path, "wb") as file:
                blob_data = blob_client.download_blob()
                blob_data.readinto(file)
            self.logger.info(
                f'Successfully downloaded blob "{blob_name}" to "{download_path}"'
            )
        else:
            self.logger.error(f"Container '{container_name}' not found")

    def delete_blob(self, container_name: str, blob_name: str) -> None:
        """
        Deletes a blob from a specified container.

        Args:
            container_name (str): The name of the container where the blob is located.
            blob_name (str): The name of the blob to be deleted.
        """
        container_client = self.container_clients.get(container_name)
        if container_client:
            self.logger.info(f'Deleting blob "{blob_name}"')
            container_client.delete_blob(blob_name)
            self.logger.info(f'Successfully deleted blob "{blob_name}"')
        else:
            self.logger.error(f"Container '{container_name}' not found")

    def list_blobs(self, container_name: str) -> List[str]:
        """
        List all blobs in the container.
        Args:
            container_name (str): The name of the container to list blobs from.

        Returns:
            List[str]: A list of blob names in the container.
        """
        container_client = self.container_clients.get(container_name)
        if container_client:
            self.logger.info(f'Listing blobs in container "{container_name}"')
            blobs = [blob.name for blob in container_client.list_blobs()]
            self.logger.info(
                f'Found {len(blobs)} blob(s) in container "{container_name}"'
            )
            return blobs
        else:
            self.logger.error(f'Container "{container_name}" not found')
            return []

    def list_containers(self) -> List[str]:
        """
        List all containers in the storage account.

        Returns:
            List[str]: A list of container names in the storage account.
        """
        return self.container_names

    def log_container_info(self) -> None:
        """
        Log information about each container in the storage account for data discovery.
        """
        # Iterate through each container in the storage account
        for container_name in self.container_names:
            try:
                container_client = self.container_clients[container_name]
                blobs = list(container_client.list_blobs())
                num_blobs = len(blobs)
                total_size = sum(blob.size for blob in blobs)
                last_modified_times = [blob.last_modified for blob in blobs]
                earliest_modified_time = (
                    min(last_modified_times) if last_modified_times else None
                )
                latest_modified_time = (
                    max(last_modified_times) if last_modified_times else None
                )

                # Log container information
                self.logger.info(f"Container name: {container_name}")
                self.logger.info(f"Number of blobs: {num_blobs}")
                self.logger.info(f"Total size of blobs (bytes): {total_size}")
                self.logger.info(
                    f"Earliest blob modification time: {earliest_modified_time}"
                )
                self.logger.info(
                    f"Latest blob modification time: {latest_modified_time}"
                )

                # Log information about each blob within the container
                for blob in blobs:
                    self.logger.info(f"  Blob name: {blob.name}")
                    self.logger.info(f"  Blob size (bytes): {blob.size}")
                    self.logger.info(
                        f"  Blob content type: {blob.content_settings.content_type}"
                    )
                    self.logger.info(f"  Blob last modified: {blob.last_modified}")

            except ResourceNotFoundError:
                self.logger.error(f"Container '{container_name}' not found")
            except Exception as ex:
                self.logger.error(
                    f"Failed to retrieve information for container '{container_name}' - {str(ex)}"
                )

    def load_blob_to_dataframe(
        self, container_name: str, blob_name: str
    ) -> pd.DataFrame:
        """
        Load a single blob to a pandas DataFrame.

        Args:
            container_name (str): The name of the container where the blob is located.
            blob_name (str): The name of the blob to load.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the blob.
                        If an error occurs, it logs the error and returns None.
        """
        container_client = self.container_clients.get(container_name)
        if container_client:
            try:
                blob_client = container_client.get_blob_client(blob_name)
                content_type = (
                    blob_client.get_blob_properties().content_settings.content_type
                )
                blob_data = AzureInterface.BlobDataLoader.load_blob_content(
                    container_client, blob_name
                )
                return AzureInterface.BlobDataLoader.load_blob_to_dataframe(
                    blob_data, content_type
                )
            except Exception as ex:
                self.logger.error(
                    f"Failed to load blob '{blob_name}' from container '{container_name}' - {str(ex)}"
                )
                return None
        else:
            self.logger.error(f"Container '{container_name}' not found")
            return None

    def load_blob_sheet_to_dataframe(
        self, container_name: str, blob_name: str, sheet_name: str = None
    ) -> pd.DataFrame:
        """
        Load a single sheet of an Excel blob to a pandas DataFrame.

        Args:
            container_name (str): The name of the container where the blob is located.
            blob_name (str): The name of the blob to load.
            sheet_name (str): The name of the sheet to load from the Excel file. If None, loads the first sheet.

        Raises:
            ValueError: If the container does not exist or if the blob's content type is not an Excel content type.
            Exception: If any other error occurs during the loading process.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the blob's sheet.
        """
        container_client = self.container_clients.get(container_name)
        if container_client is None:
            raise ValueError(f"Container '{container_name}' not found")

        blob_client = container_client.get_blob_client(blob_name)
        content_type = blob_client.get_blob_properties().content_settings.content_type
        blob_data = AzureInterface.BlobDataLoader.load_blob_content(
            container_client, blob_name
        )

        if content_type not in [
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ]:
            raise ValueError(
                f"Content type '{content_type}' is not supported. This function only supports Excel content types."
            )

        try:
            with BytesIO(blob_data) as data:
                df = pd.read_excel(data, sheet_name=sheet_name)
                return df
        except Exception as ex:
            raise Exception(
                f"Failed to load blob '{blob_name}' into a DataFrame"
            ) from ex

    def load_blobs_to_dataframes(self, container_name: str) -> Dict[str, pd.DataFrame]:
        """
        Load all blobs from a container to pandas DataFrames.

        Args:
            container_name (str): The name of the container where the blobs are located.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary where keys are blob names, and values are
                                    pandas DataFrames containing the data from the corresponding blob.
                                    If an error occurs during the loading of a particular blob,
                                    it logs the error and excludes the blob from the returned dictionary.
        """
        dataframes = {}
        container_client = self.container_clients.get(container_name)
        if container_client:
            blobs = container_client.list_blobs()
            for blob in blobs:
                self.logger.info(
                    f"Loading blob '{blob.name}' from container '{container_name}'"
                )
                df = self.load_blob_to_dataframe(container_name, blob.name)
                if df is not None:
                    self.logger.info(
                        f"Successfully loaded blob '{blob.name}' from container '{container_name}'"
                    )
                    if isinstance(df, dict):
                        for sheet_name, sheet_df in df.items():
                            dataframes[f"{blob.name} - {sheet_name}"] = sheet_df
                    else:
                        dataframes[blob.name] = df
                else:
                    self.logger.error(
                        f"Failed to load blob '{blob.name}' from container '{container_name}'"
                    )
            return dataframes
        else:
            self.logger.error(f"Container '{container_name}' not found")
            return {}
