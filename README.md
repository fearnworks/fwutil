# Fearnworks Util

Fearnworks Util is a Python utility library. This library contains several modules for data analysis, interface with Azure Blob Storage, and utility functions.

## Structure

The library is structured as follows:

### `analysis`

This module provides interfaces for transforming data, visualizing data, and performing data analysis.

- `IDataTransformer`: An abstract interface for data transformers, which transform input data based on the specific use case.
- `IDataVisualizer`: An abstract interface for data visualizers, which generate visualizations based on the transformed data.
- `IAnalyzer`: An abstract interface for analyzers, which perform data analysis and generate visualizations.

### `azure_interface`

This module contains functions and classes that interface with Azure Blob Storage.

- `AzureBlobStorageManager`: A class that manages interactions with Azure Blob Storage, maintaining a dictionary where the keys are container names and the values are the corresponding ContainerClients.
- `BlobDataLoader`: A class for loading blob data from an Azure Blob Storage container.

### `logs`

This directory is likely used for storing log files generated by the utility library.

### `pipelines`

This module contains several submodules related to data pipelines, particularly ETL (Extract, Transform, Load) processes:

#### `base`

This submodule provides base classes and interfaces for ETL strategies:

- `TransformStrategy`: An interface for a transformation strategy used in an ETL process.
- `ExtractStrategy`: An interface for an extraction strategy used in an ETL process.
- `LoadStrategy`: An interface for a load strategy used in an ETL process.
- `ETLStrategy`: A class that represents an ETL process using the strategies defined above.
- `DependentETLStrategy`: A class that represents a dependent ETL process.

#### `common`

This submodule provides common strategies used across different ETL processes:

- `AzureBlobStorageBaseStrategy`, `AzureBlobStorageExtractStrategy`, `AzureBlobStorageExcelSheetExtractStrategy`, `AzureBlobStorageParquetLoadStrategy`, `AzureBlobStorageExcelLoadStrategy`: Classes representing different strategies for interacting with Azure Blob Storage during ETL processes.
- `DateTableTransformStrategy`: A transformation strategy for creating a date table during an ETL process.

#### `gitlab`

This submodule provides strategies and classes specific to GitLab data:

- `GitlabIssuesETL`: An ETL strategy for GitLab issue analysis.
- `GitlabIssuesDateTableTL`: A Transform-Load (TL) strategy for the GitLab Date Table.

##### `gitlab/strategies`

This submodule provides strategies related to GitLab issues:

- `GitlabIssuesExtractStrategy`: A strategy for extracting data from GitLab issues.
- `GitlabIssuesTransformStrategy`: A strategy for transforming the extracted GitLab issues data.

### `utils`

This module contains utility functions that can be used across the library.

- `DataFrameTransformer`: A class for transforming a pandas DataFrame.
- `JupyterDisplayHandler`: A custom logging handler that sends logs to Jupyter notebooks using the display function.
- `PipelineLogger`: A centralized configurable logging class, responsible for setting up logging configurations and getting a logger with a given name.

## Dependencies

The library has several dependencies, which are listed in the 'requirements.txt' file.

## Installation

To install the library and its dependencies, you can use pip. In the root directory of the library, run the following command:

As you want to pull in other parts of the library look for requirements.txt defined at each lib level.

```bash
pip install -r requirements.txt
```

To install as a module go into the cloned repo while having the desired environment active and use 

```bash
pip install -e .
```
