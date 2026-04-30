from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from backend.app.control_plane import get_workspace_client
from idc_app.spark import _resolved_auth_type, _session_builder


class DatabricksAuthTests(unittest.TestCase):
    def test_resolved_auth_type_defaults_to_oauth_even_when_pat_is_present(self) -> None:
        with patch.dict(
            os.environ,
            {
                "IDC_PAT_TOKEN": "token",
                "IDC_DB_HOST": "https://example.databricks.com",
            },
            clear=False,
        ):
            self.assertEqual(_resolved_auth_type(), "oauth")

    def test_session_builder_prefers_oauth_profile_by_default(self) -> None:
        fake_builder = MagicMock()
        fake_builder.serverless.return_value = fake_builder
        fake_builder.profile.return_value = fake_builder
        fake_builder.host.return_value = fake_builder

        with patch.dict(
            os.environ,
            {
                "IDC_DB_AUTH_TYPE": "oauth",
                "IDC_DB_PROFILE": "DEFAULT",
                "IDC_DB_HOST": "https://example.databricks.com",
                "IDC_PAT_TOKEN": "token",
            },
            clear=False,
        ), patch("databricks.connect.DatabricksSession") as session:
            session.builder = fake_builder

            builder = _session_builder()

        self.assertIs(builder, fake_builder)
        fake_builder.serverless.assert_called_once_with(True)
        fake_builder.profile.assert_called_once_with("DEFAULT")
        fake_builder.host.assert_called_once_with("https://example.databricks.com")
        fake_builder.token.assert_not_called()

    def test_session_builder_uses_pat_only_when_explicitly_requested(self) -> None:
        fake_builder = MagicMock()
        fake_builder.serverless.return_value = fake_builder
        fake_builder.host.return_value = fake_builder
        fake_builder.token.return_value = fake_builder

        with patch.dict(
            os.environ,
            {
                "IDC_DB_AUTH_TYPE": "pat",
                "IDC_DB_HOST": "https://example.databricks.com",
                "IDC_PAT_TOKEN": "token",
            },
            clear=False,
        ), patch("databricks.connect.DatabricksSession") as session:
            session.builder = fake_builder

            builder = _session_builder()

        self.assertIs(builder, fake_builder)
        fake_builder.serverless.assert_called_once_with(True)
        fake_builder.host.assert_called_once_with("https://example.databricks.com")
        fake_builder.token.assert_called_once_with("token")

    def test_workspace_client_prefers_profile_auth_by_default(self) -> None:
        with patch("backend.app.control_plane.WorkspaceClient") as workspace_client, patch(
            "backend.app.control_plane.settings.db_auth_type", "oauth"
        ), patch("backend.app.control_plane.settings.db_host", "https://example.databricks.com"), patch(
            "backend.app.control_plane.settings.db_profile", "DEFAULT"
        ), patch("backend.app.control_plane.settings.pat_token", "token"):
            get_workspace_client()

        workspace_client.assert_called_once_with(host="https://example.databricks.com", profile="DEFAULT")


if __name__ == "__main__":
    unittest.main()
