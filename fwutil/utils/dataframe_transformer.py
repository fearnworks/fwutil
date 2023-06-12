import logging

import pandas as pd

from .logger import PipelineLogger


class DataFrameTransformer:
    """
    Class for transforming a pandas DataFrame.

    This class provides methods for exploding, melting object columns, removing empty or zero columns,
    formatting column names, and renaming duplicate columns in a DataFrame.

    Attributes:
        logger (logging.Logger): The logger to be used.
    """

    logger: logging.Logger

    def __init__(self):
        """
        Initialize the DataFrameTransformer and set up logging configuration.
        """
        self.logger = PipelineLogger.get_logger(__name__)

    def explode_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Explode the given DataFrame.

        The DataFrame is exploded by converting dtypes, melting object columns, removing empty or zero columns,
        formatting column names, and renaming duplicate columns.

        Args:
            df (pd.DataFrame): The DataFrame to explode.

        Returns:
            pd.DataFrame: The exploded DataFrame.

        Raises:
            Exception: If an error occurs during the DataFrame explosion process.
        """
        try:
            self.logger.info(pd.__version__)
            self.logger.info(df.dtypes)
            df = self.melt_object_columns(df)
            self.logger.info(df.dtypes)
            df = self.remove_empty_or_zero_columns(df)
            self.logger.info(df.dtypes)
            df = self.format_column_names(df)
            self.logger.info(df.dtypes)
            df = self.rename_duplicate_columns(df)
            self.logger.info(df.columns)
            df = df.convert_dtypes(dtype_backend="pyarrow")
            self.logger.info(df.dtypes)
            return df
        except Exception as e:
            self.logger.error(f"Failed to explode DataFrame: {e}")
            raise

    def melt_object_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Melt object columns in the DataFrame.

        This method identifies columns of object dtype and expands dictionary-like objects in those columns
        into separate columns in the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame whose object columns will be melted.

        Returns:
            pd.DataFrame: The DataFrame with melted object columns.

        Raises:
            Exception: If an error occurs during the melting process.
        """
        try:
            # Identify columns with object data type
            object_columns = df.select_dtypes(include="object").columns

            # Log the identified object columns
            self.logger.info(f"Identified object columns: {object_columns.tolist()}")

            # Create a copy of the original DataFrame to prevent modifying the original
            df_copy = df.copy()

            # Iterate over object columns
            for col in object_columns:
                # Check if the column contains any dictionary-like objects
                contains_dicts = df_copy[col].apply(isinstance, args=(dict,)).any()
                if contains_dicts:
                    # Expand dictionary-like objects into separate columns
                    expanded_col = df_copy[col].apply(pd.Series)

                    # Prefix the column names with the original column name
                    expanded_col.columns = [f"{col}.{c}" for c in expanded_col.columns]

                    # Log the column being melted and the resulting column names
                    self.logger.info(
                        f'Melting column "{col}" into columns: {expanded_col.columns.tolist()}'
                    )

                    # Drop the original column from the DataFrame
                    df_copy.drop(col, axis=1, inplace=True)

                    # Concatenate the expanded columns to the DataFrame
                    df_copy = pd.concat([df_copy, expanded_col], axis=1)
                    
            if any(df_copy[col].apply(isinstance, args=(dict,)).any() for col in df_copy.columns):
                return self.melt_object_columns(df_copy)
            
            return df_copy
        
        except Exception as e:
            self.logger.error(f"Failed to melt object columns: {e}")
            raise

    def remove_empty_or_zero_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove empty or zero columns from the DataFrame.

        This method identifies columns that contain only NaN values or only zeros and drops them from the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame from which empty or zero columns will be removed.

        Returns:
            pd.DataFrame: The DataFrame with empty or zero columns removed.

        Raises:
            Exception: If an error occurs during the removal process.
        """
        try:
            self.logger.info("Remove Empty Columns for DF")
            empty_columns = df.columns[df.isna().all()]

            # Identify columns that contain only zeros
            zero_columns = df.columns[(df == 0).all()]

            # Combine the empty columns and zero columns
            columns_to_drop = empty_columns.union(zero_columns)

            # Drop the empty and zero columns from the DataFrame
            df = df.drop(columns=columns_to_drop)
            self.logger.info(
                f"Dropped empty or zero columns: {columns_to_drop.tolist()}"
            )

            return df
        except Exception as e:
            self.logger.error(f"Failed to remove empty or zero columns: {e}")
            raise
        
    def remove_empty_or_zero_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove empty or zero rows from the DataFrame.

        This method identifies rows that contain only NaN values or only zeros and drops them from the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame from which empty or zero rows will be removed.

        Returns:
            pd.DataFrame: The DataFrame with empty or zero rows removed.

        Raises:
            Exception: If an error occurs during the removal process.
        """
        logger = Utils.PipelineLogger.get_logger(__name__)
        try:
            logger.info("Remove Empty Rows for DF")

            # Identify rows that contain only NaN values
            empty_rows = df.index[df.isna().all(axis=1)]

            # Identify rows that contain only zeros
            zero_rows = df.index[(df == 0).all(axis=1)]

            # Combine the empty rows and zero rows
            rows_to_drop = empty_rows.union(zero_rows)

            # Drop the empty and zero rows from the DataFrame
            df = df.drop(rows_to_drop)
            logger.info(f"Dropped empty or zero rows: {rows_to_drop.tolist()}")

            return df
        except Exception as e:
            logger.error(f"Failed to remove empty or zero rows: {e}")
            raise

    def format_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format column names in the DataFrame.

        This method replaces '.' with '_' in column names and converts them to PascalCase.

        Args:
            df (pd.DataFrame): The DataFrame whose column names will be formatted.

        Returns:
            pd.DataFrame: The DataFrame with formatted column names.

        Raises:
            Exception: If an error occurs during the formatting process.
        """
        try:
            # Define a function to convert a string to PascalCase
            def to_pascal_case(s):
                return "".join(word.capitalize() for word in s.split("_"))

            # Get the current column names
            old_column_names = df.columns

            # Create a dictionary to store the mappings from old to new column names
            column_name_mapping = {}

            # Iterate through the old column names and create the new formatted column names
            for old_name in old_column_names:
                # Replace '.' with '_' in the column name
                new_name = old_name.replace(".", "_")
                # Convert to PascalCase
                new_name = to_pascal_case(new_name)
                # Add the mapping to the dictionary
                column_name_mapping[old_name] = new_name

            # Rename the columns using the mapping dictionary
            df.rename(columns=column_name_mapping, inplace=True)

            return df
        except Exception as e:
            self.logger.error("Failed to format column names: %s", str(e))

    def rename_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename duplicate columns in the DataFrame.

        This method identifies duplicate column names and renames them by appending a count to the end of the column name.

        Args:
            df (pd.DataFrame): The DataFrame whose duplicate columns will be renamed.

        Returns:
            pd.DataFrame: The DataFrame with renamed duplicate columns.

        Raises:
            Exception: If an error occurs during the renaming process.
        """
        try:
            # Create a dictionary to store the count of each column name
            col_count = {}
            # Create a list to store the new column names
            new_columns = []

            # Iterate over the columns in the DataFrame
            for col in df.columns:
                # If the column name is already in the dictionary, increment the count
                if col in col_count:
                    col_count[col] += 1
                    # Append the count to the column name to make it unique
                    new_col = f"{col}_{col_count[col]}"
                else:
                    # Initialize the count for the column name in the dictionary
                    col_count[col] = 0
                    new_col = col
                # Add the new column name to the list of new columns
                new_columns.append(new_col)

            # Assign the new column names to the DataFrame
            df.columns = new_columns

            return df
        except Exception as e:
            self.logger.error(f"Failed to rename duplicate columns: {e}")
            raise
