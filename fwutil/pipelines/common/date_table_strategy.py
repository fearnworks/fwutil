from typing import Callable, Dict, List, Optional, Union

import pandas as pd
import pipelines.base as BaseStrategies
import utils as Utils
from pandas import DatetimeIndex
from pandas.tseries.holiday import USFederalHolidayCalendar


class DateTableTransformStrategy(BaseStrategies.TransformStrategy):
    """
    Transformation strategy for creating a date table from a date series.

    Attributes:
        df (pd.DataFrame): The input DataFrame.
        date_col (str): The column name for the date data in the DataFrame.
        included_features (List[str]): The list of date features to include in the date table.
    """

    FEATURE_GENERATORS: Dict[str, Callable[[DatetimeIndex], Union[int, str, bool]]] = {
        "Year": lambda dates: dates.year,
        "Month": lambda dates: dates.month,
        "MonthName": lambda dates: dates.strftime("%B"),
        "Day": lambda dates: dates.day,
        "Quarter": lambda dates: dates.quarter,
        "QuarterName": lambda dates: "Q" + dates.quarter.astype(str),
        "DayOfWeek": lambda dates: dates.dayofweek,
        "DayName": lambda dates: dates.strftime("%A"),
        "DayOfYear": lambda dates: dates.dayofyear,
        "WeekOfYear": lambda dates: dates.isocalendar().week,
        "IsWeekend": lambda dates: dates.weekday >= 5,
        "IsMonthStart": lambda dates: dates.is_month_start,
        "IsMonthEnd": lambda dates: dates.is_month_end,
        "IsQuarterStart": lambda dates: dates.is_quarter_start,
        "IsQuarterEnd": lambda dates: dates.is_quarter_end,
        "IsYearStart": lambda dates: dates.is_year_start,
        "IsYearEnd": lambda dates: dates.is_year_end,
        "WeekOfQuarter": lambda dates: (dates.isocalendar().week - 1) % 13 + 1,
        "DayOfQuarter": lambda dates: (dates - dates.to_period("Q").to_timestamp()).days
        + 1,
        "MonthOfQuarter": lambda dates: (dates.month - 1) % 3 + 1,
        "WeekOfMonth": lambda dates: (dates.day - 1) // 7 + 1,
        "SortableMonthYear": lambda dates: dates.strftime("%Y%m"),
    }

    def __init__(
        self,
        df: pd.DataFrame,
        date_col: str,
        included_features: Optional[List[str]] = None,
    ):
        """
        Initializes the DateTableTransformStrategy class.

        Args:
            df (pd.DataFrame): The input DataFrame.
            date_col (str): The column name for the date data in the DataFrame.
            included_features (Optional[List[str]]): The list of date features to include in the date table. If None, all features are included.
        """
        self.logger = Utils.PipelineLogger.get_logger(__name__)
        self.logger.info("Initializing DateTableTransformStrategy...")
        self.logger.info("Date column: %s", date_col)
        self.df = df
        self.date_col = date_col
        self.included_features = (
            included_features
            if included_features is not None
            else self.FEATURE_GENERATORS.keys()
        )

    def transform(self) -> pd.DataFrame:
        """
        Transforms a date series into a date table.

        Returns:
            pd.DataFrame: The transformed DataFrame.

        Raises:
            Exception: If there's any error during the transformation process.
        """
        self.logger.info(self.df.head(10))
        self.logger.info("Date column: %s", self.date_col)
        date_series = self.df[self.date_col]
        min_date = date_series.min()
        max_date = date_series.max()
        self.logger.info("Min date: %s", min_date)
        self.logger.info("Max date: %s", max_date)
        dates = pd.date_range(min_date, max_date)

        date_table = pd.DataFrame({"Date": dates})

        for feature in self.included_features:
            if feature in self.FEATURE_GENERATORS:
                generator = self.FEATURE_GENERATORS[feature]
                date_table[feature] = generator(dates)
            else:
                self.logger.warning(f"Unknown feature: {feature}")

        self.logger.info("Creating holiday table...")
        self.logger.info(date_table.head(10))

        cal = USFederalHolidayCalendar()
        holidays = cal.holidays(start=min_date, end=max_date, return_name=True)
        holidays = holidays.reset_index().rename(
            columns={"index": "Date", 0: "HolidayName"}
        )
        date_table = pd.merge(date_table, holidays, on="Date", how="left")

        return date_table
