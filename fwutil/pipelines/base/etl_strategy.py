import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd


class TransformStrategy(ABC):
    """
    Abstract Class to define an interface for a transformation strategy.
    """

    @abstractmethod
    def transform(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        """
        Perform some transformation on the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        pass


class ExtractStrategy(ABC):
    """
    Abstract Class to define an interface for an extraction strategy.
    """

    @abstractmethod
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """
        Perform extraction of data.

        Returns:
            pd.DataFrame: The extracted DataFrame.
        """
        pass


class LoadStrategy(ABC):
    """
    Abstract Class to define an interface for a load strategy.
    """

    @abstractmethod
    def load(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        """
        Load the given DataFrame into a storage system.

        Args:
            df (pd.DataFrame): The DataFrame to load.
        """
        pass


class ETLPipeline:
    """
    A class that represents an ETL (Extract, Transform, Load) strategy. It utilizes
    strategy pattern for each phase of the ETL process, allowing for different behaviors
    to be swapped in and out at runtime.

    Attributes:
        extract_strategy (ExtractStrategy): The strategy to use for the extraction phase.
        transform_strategy (TransformStrategy): The strategy to use for the transformation phase.
        load_strategy (LoadStrategy): The strategy to use for the loading phase.
    """

    def __init__(
        self,
        extract_strategies: List[ExtractStrategy],
        transform_strategies: List[TransformStrategy],
        load_strategies: List[LoadStrategy],
        logger: Optional[logging.Logger] = None,
        *args,
        **kwargs
    ):
        """
        The constructor for ETLPipeline class.

        Args:
            extract_strategy (ExtractStrategy): The strategies to use for the extraction phase.
            transform_strategy (TransformStrategy): The strategies to use for the transformation phase.
            load_strategy (LoadStrategy): The strategies to use for the loading phase.
            logger (logging.Logger, optional): The logger to use for logging. If None, logging is skipped.
        """
        if logger is not None:
            logger.info("Using extract strategies: {}".format([type(strategy).__name__ for strategy in extract_strategies]))
            logger.info("Using transform strategies: {}".format([type(strategy).__name__ for strategy in transform_strategies]))
            logger.info("Using load strategies: {}".format([type(strategy).__name__ for strategy in load_strategies]))
        self.extract_strategies = extract_strategies
        self.transform_strategies = transform_strategies
        self.load_strategies = load_strategies

    def execute(self, *args, **kwargs):
        """
        Executes the ETL process by first extracting data, then transforming the data,
        and finally loading the data. The exact behavior at each phase depends on the
        specific strategies provided during the creation of the ETLPipeline instance.

        Returns:
            Any: The result of the load phase. The exact type of the return value depends on the specific LoadStrategy used.
        """
        data = None
        for strategy in self.extract_strategies:
            data = strategy.extract()
        for strategy in self.transform_strategies:
            data = strategy.transform(data)
        for strategy in self.load_strategies:
            data = strategy.load(data)
        return data


class DependentETLPipeline:
    """
    A class that represents a dependent ETL (Transform, Load) strategy. It utilizes
    strategy pattern for each phase of the ETL process, allowing for different behaviors
    to be swapped in and out at runtime. This version of the ETL strategy doesn't perform
    its own extraction, but still performs transformations and loads.

    Attributes:
        transform_strategies (List[TransformStrategy]): The strategies to use for the transformation phase.
        load_strategies (List[LoadStrategy]): The strategies to use for the loading phase.
    """

    def __init__(
        self,
        transform_strategies: List[TransformStrategy],
        load_strategies: List[LoadStrategy],
        logger: Optional[logging.Logger] = None,
        *args,
        **kwargs
    ):
        """
        The constructor for DependentETLPipeline class.

        Args:
            transform_strategies (List[TransformStrategy]): The strategies to use for the transformation phase.
            load_strategies (List[LoadStrategy]): The strategies to use for the loading phase.
            logger (logging.Logger, optional): The logger to use for logging. If None, logging is skipped.
        """
        if logger is not None:
            logger.info("Using transform strategies: {}".format([type(strategy).__name__ for strategy in transform_strategies]))
            logger.info("Using load strategies: {}".format([type(strategy).__name__ for strategy in load_strategies]))
        self.transform_strategies = transform_strategies
        self.load_strategies = load_strategies

    def execute(self, *args, **kwargs):
        """
        Executes the ETL process by first transforming the input data,
        and finally loading the data. The exact behavior at each phase depends on the
        specific strategies provided during the creation of the ETLPipeline instance.

        Args:dat
            data: The data to be transformed and loaded.

        Returns:
            Any: The result of the load phase. The exact type of the return value depends on the specific LoadStrategy used.
        """
        
        data = None
        for strategy in self.transform_strategies:
            data = strategy.transform()  
        for strategy in self.load_strategies:
            data = strategy.load(data)
        return data