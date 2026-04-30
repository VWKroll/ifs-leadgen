from __future__ import annotations

import unittest
from unittest.mock import patch

from backend.app.schemas import OpportunitySummary
from backend.app.services import build_knowledge_graph


class ServicesTests(unittest.TestCase):
    def test_build_knowledge_graph_groups_events_into_regions_and_countries(self) -> None:
        items = [
            OpportunitySummary(
                cluster_id="cluster-1",
                subject_company_name="Acme Holdings",
                subject_country="United Kingdom",
                subject_region="Europe",
                trigger_type="insolvency",
                event_date="2026-04-01",
                event_summary="UK distress signal",
                opportunity_score=91.0,
                cluster_confidence_score=77.0,
            ),
            OpportunitySummary(
                cluster_id="cluster-2",
                subject_company_name="Bravo GmbH",
                subject_country="Germany",
                subject_region="Europe",
                trigger_type="regulatory_action",
                event_date="2026-04-02",
                event_summary="German regulatory signal",
                opportunity_score=78.0,
                cluster_confidence_score=71.0,
            ),
            OpportunitySummary(
                cluster_id="cluster-3",
                subject_company_name="Pacific Co",
                subject_country="Japan",
                subject_region="APAC",
                trigger_type="financing",
                event_date="2026-04-03",
                event_summary="Japanese financing signal",
                opportunity_score=75.0,
                cluster_confidence_score=69.0,
            ),
        ]

        with patch("backend.app.services.list_opportunities", return_value=items):
            graph = build_knowledge_graph()

        self.assertEqual(graph.region_count, 2)
        self.assertEqual(graph.country_count, 3)
        self.assertEqual(graph.event_count, 3)
        emea = next(region for region in graph.regions if region.region_id == "EMEA")
        self.assertEqual(emea.country_count, 2)
        self.assertTrue(emea.narrative)
        united_kingdom = next(country for country in emea.countries if country.country_id == "United Kingdom")
        self.assertEqual(united_kingdom.events[0].cluster_id, "cluster-1")


if __name__ == "__main__":
    unittest.main()
