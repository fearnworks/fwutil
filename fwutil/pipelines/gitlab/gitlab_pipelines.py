import gitlab
import pandas as pd

import azure_interface as AzureInterface
import pipelines.base as PipelineBases
import pipelines.common as CommonStrategies
import pipelines.gitlab.strategies as GitlabStrategies
import utils as Utils


class GitlabIssuesETL(PipelineBases.ETLStrategy):
    """
    An Extract-Transform-Load (ETL) strategy for Gitlab Issue analysis.
    """

    def __init__(
        self, az_mgr: AzureInterface.AzureBlobStorageManager, gl_conn: gitlab.Gitlab
    ):
        try:
            extract_strategy = GitlabStrategies.GitlabIssuesExtractStrategy(gl_conn)
            transform_strategy = GitlabStrategies.GitlabIssuesTransformStrategy()
            load_strategy = CommonStrategies.AzureBlobStorageExcelLoadStrategy(
                az_mgr, "gitlab", "GitlabIssues.xlsx"
            )

            super().__init__(extract_strategy, transform_strategy, load_strategy)
            self.logger = Utils.PipelineLogger.get_logger(__name__)
            self.az_mgr = az_mgr
        except Exception as e:
            self.logger.error(f"Failed to initialize GitlabIssuesETL: {e}")
            raise


class GitlabIssuesDateTableTL(PipelineBases.DependentETLStrategy):
    """
    A Transform-Load (TL) strategy for the Gitlab Date Table.
    """

    def __init__(
        self,
        az_mgr: AzureInterface.AzureBlobStorageManager,
        transaction_df: pd.DataFrame,
    ):
        try:
            transform_strategy = CommonStrategies.DateTableTransformStrategy(
                transaction_df, "Date"
            )
            load_strategy = CommonStrategies.AzureBlobStorageExcelLoadStrategy(
                az_mgr, "gitlab", "IssueDateTable.xlsx"
            )

            super().__init__(transform_strategy, load_strategy)
            self.logger = Utils.PipelineLogger.get_logger(__name__)
            self.az_mgr = az_mgr
        except Exception as e:
            self.logger.error(f"Failed to initialize GitlabIssueDateTableTL: {e}")
            raise
