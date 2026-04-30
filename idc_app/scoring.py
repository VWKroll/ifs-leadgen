"""Opportunity scoring for event clusters.

The real tables already carry pre-computed scores (cluster_priority_score,
cluster_confidence_score, event_confidence_score, etc.).  This module exposes
an 'opportunity_score' alias so the rest of the app has a single column to
reference, and optionally blends the raw scores into a composite.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def score_clusters(df: DataFrame) -> DataFrame:
    """Alias cluster_priority_score as opportunity_score.

    Blend with confidence and urgency for a composite view.
    Adjust weights here as the model evolves.
    """
    # All three scores are already on a 0-100 scale — weighted average stays 0-100.
    return df.withColumn(
        "opportunity_score",
        F.least(
            F.lit(100.0),
            F.greatest(
                F.lit(0.0),
                (
                    F.coalesce(F.col("cluster_priority_score"), F.lit(0)) * 0.6
                    + F.coalesce(F.col("cluster_confidence_score"), F.lit(0)) * 0.25
                    + F.coalesce(F.col("event_urgency_score"), F.lit(0)) * 0.15
                ),
            ),
        ).cast("double"),
    )
