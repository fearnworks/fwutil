# File: cycle_time_distribution_by_category.py

import matplotlib.pyplot as plt
import seaborn as sns

from .. import IAnalyzer, IDataTransformer, IDataVisualizer


class CycleTimeDistributionTransformer(IDataTransformer):
    """
    A data transformer class that performs data transformation for visualizing
    cycle time distribution by category.

    Implements the IDataTransformer interface.
    """

    def transform(self, data, **kwargs):
        """
        Performs data transformation for the cycle time distribution use case.

        Args:
            data (pd.DataFrame): A DataFrame containing the input data.
            **kwargs: Additional keyword arguments for the transformation logic.

        Returns:
            pd.DataFrame: A DataFrame containing the transformed data.

        Example:
            transformer = CycleTimeDistributionTransformer()
            transformed_data = transformer.transform(data)
        """
        return data


class CycleTimeDistributionVisualizer(IDataVisualizer):
    """
    A data visualizer class that generates visualizations for cycle time
    distribution by category.

    Implements the IDataVisualizer interface.
    """

    def visualize(self, data, cycle_time_col, category_col):
        """
        Generates a histogram visualization of cycle time distribution by category.

        Args:
            data (pd.DataFrame): A DataFrame containing the transformed data.
            cycle_time_col (str): The column name representing the cycle time.
            category_col (str): The column name representing the category.

        Returns:
            None

        Example:
            visualizer = CycleTimeDistributionVisualizer()
            visualizer.visualize(data, cycle_time_col='cycle_time', category_col='category')
        """
        sns.histplot(
            data=data, x=cycle_time_col, hue=category_col, kde=True, multiple="stack"
        )
        plt.title("Cycle Time Distribution by Category")
        plt.xlabel("Cycle Time (Days)")
        plt.ylabel("Frequency")
        plt.show()


class CycleTimeDistributionAnalyzer(IAnalyzer):
    """
    An analyzer class that performs data analysis and generates visualizations
    for cycle time distribution by category.

    Implements the IAnalyzer interface.
    """

    def __init__(self, transformer, visualizer):
        self.transformer = transformer
        self.visualizer = visualizer

    def analyze(self, data, cycle_time_col, category_col):
        """
        Performs data analysis and generates visualizations for cycle time
        distribution by category.

        Args:
            data (pd.DataFrame): A DataFrame containing the input data.
            cycle_time_col (str): The column name representing the cycle time.
            category_col (str): The column name representing the category.

        Returns:
            None

        Example:
            transformer = CycleTimeDistributionTransformer()
            visualizer = CycleTimeDistributionVisualizer()
            analyzer = CycleTimeDistributionAnalyzer(transformer, visualizer)
            analyzer.analyze(data, cycle_time_col='cycle_time', category_col='category')
        """
        transformed_data = self.transformer.transform(data)
        self.visualizer.visualize(
            transformed_data, cycle_time_col=cycle_time_col, category_col=category_col
        )


# Sample usage:
# transformer = CycleTimeDistributionTransformer()
# visualizer = CycleTimeDistributionVisualizer()
# analyzer = CycleTimeDistributionAnalyzer(transformer, visualizer)
# analyzer.analyze(data, cycle_time_col='cycle_time', category_col='category')
