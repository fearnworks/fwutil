from abc import ABC, abstractmethod


class IDataTransformer(ABC):
    """
    An abstract interface for data transformers that perform data transformations
    as part of data analysis.

    Methods:
        transform: Transforms the input data based on the specific use case.
    """

    @abstractmethod
    def transform(self, data, **kwargs):
        """
        Transforms the input data based on the specific use case.

        Args:
            data: A pandas DataFrame containing the input data.
            **kwargs: Additional keyword arguments for the transformation logic.

        Returns:
            A pandas DataFrame containing the transformed data.

        Example:
            class MyTransformer(IDataTransformer):
                def transform(self, data, column_name):
                    return data[column_name].mean()

            transformer = MyTransformer()
            transformed_data = transformer.transform(data, column_name='cycle_time')
        """
        pass

class IDataVisualizer(ABC):
    """
    An abstract interface for data visualizers that generate visualizations
    based on the transformed data.

    Methods:
        visualize: Generates visualizations based on the transformed data.
    """

    @abstractmethod
    def visualize(self, data, **kwargs):
        """
        Generates visualizations based on the transformed data.

        Args:
            data: A pandas DataFrame containing the transformed data.
            **kwargs: Additional keyword arguments for the visualization logic.

        Returns:
            None

        Example:
            class MyVisualizer(IDataVisualizer):
                def visualize(self, data, column_name):
                    data[column_name].plot()

            visualizer = MyVisualizer()
            visualizer.visualize(data, column_name='cycle_time')
        """
        pass

class IAnalyzer(ABC):
    """
    An abstract interface for analyzers that perform data analysis and
    generate visualizations.

    Methods:
        analyze: Performs data analysis and generates visualizations.
    """

    @abstractmethod
    def analyze(self, data, **kwargs):
        """
        Performs data analysis and generates visualizations based on the input data.

        Args:
            data: A pandas DataFrame containing the input data.
            **kwargs: Additional keyword arguments for the analysis logic.

        Returns:
            None

        Example:
            class MyAnalyzer(IAnalyzer):
                def __init__(self, transformer, visualizer):
                    self.transformer = transformer
                    self.visualizer = visualizer

                def analyze(self, data, column_name):
                    transformed_data = self.transformer.transform(data, column_name=column_name)
                    self.visualizer.visualize(transformed_data, column_name=column_name)

            analyzer = MyAnalyzer(transformer, visualizer)
            analyzer.analyze(data, column_name='cycle_time')
        """
        pass
