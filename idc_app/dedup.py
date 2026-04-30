"""Deduplication logic for event clusters."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def deduplicate_events(df: DataFrame, partition_cols: list[str], order_col: str = "updated_at") -> DataFrame:
    """Keep the most recent record per partition key set."""
    from pyspark.sql.window import Window

    window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
