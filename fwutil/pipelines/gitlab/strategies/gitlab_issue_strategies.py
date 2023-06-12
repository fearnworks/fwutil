# Here is an implementation that includes the suggested improvements:

from typing import Dict, Optional
from urllib.parse import urlparse

import azure_interface as AzureInterface
import gitlab
import pandas as pd
import pipelines.base as PipelineBases
import pipelines.common as CommonStrategies
import utils as Utils


class GitlabIssuesExtractStrategy:
    """
    A class that represents a strategy for extracting data from Gitlab Issues.
    """

    conn: gitlab.Gitlab

    def __init__(self, gl_conn: gitlab.Gitlab):
        """
        Initialize the GitlabIssuesExtractStrategy class.

        Args:
            gl_conn (gitlab.Gitlab): A Gitlab connection object.
        """
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        self.logger.info("Initializing Gitlab Issues Extract Strategy...")
        self.gl_conn = gl_conn

    def extract(self) -> pd.DataFrame:
        projects = self.gl_conn.projects.list(get_all=True)  # get all projects
        all_issues = []
        for project in projects:
            issues = project.issues.list(
                get_all=True
            )  # get all issues for each project
            all_issues.extend([issue.attributes for issue in issues])
        df = pd.DataFrame(all_issues)
        return df


class GitlabIssuesTransformStrategy:
    """
    A strategy for transforming transaction data for Gitlab Issues.

    This class extends the TransactionTransformStrategy class defined in the `pipelines.common` module,
    and overrides the `transform` method to include functionality specific to this project.
    """

    def __init__(self):
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        self.logger.info("Initializing Gitlab Issues Transform Strategy...")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the transaction data by standardizing vendor labels and adding additional columns.

        Args:
            df (pd.DataFrame): The input DataFrame, which contains transaction data.

        Returns:
            pd.DataFrame: The transformed DataFrame, which includes standardized vendor labels and additional columns.

        Raises:
            Exception: If there's any error during the transformation process.
        """
        self.logger.info("Transforming transaction data...")
        self.logger.info(df)
        df = Utils.DataFrameTransformer().explode_df(df)
        df = self.remove_url_columns(df)
        return df

    def remove_url_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function takes a dataframe and removes any columns containing URLs.

        Args:
            df (pd.DataFrame): Input dataframe.

        Returns:
            pd.DataFrame: Output dataframe with URL columns removed.
        """

        def is_url(val):
            """Check if a value is a URL by trying to parse it."""
            try:
                result = urlparse(str(val))
                return all([result.scheme, result.netloc])
            except ValueError:
                return False

        cols_to_drop = [col for col in df.columns if df[col].apply(is_url).any()]
        df = df.drop(columns=cols_to_drop)
        return df
