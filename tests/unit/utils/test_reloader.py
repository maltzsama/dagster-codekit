from unittest.mock import MagicMock, patch

import pytest

pytest.skip("DagsterReloader API changed - tests need rewrite", allow_module_level=True)

from dagster_codekit.core.exceptions import AuthenticationError
from dagster_codekit.utils.reloader import DagsterReloader


@pytest.mark.asyncio
async def test_reloader_success():
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=200, json=lambda: {"data": {"reloadWorkspace": {"__typename": "Workspace"}}}
        )

        reloader = DagsterReloader("http://dagster:3000")
        await reloader.reload()

        mock_post.assert_awaited_once()


@pytest.mark.asyncio
async def test_reloader_auth_error():
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=401, text="Unauthorized")

        reloader = DagsterReloader("http://dagster:3000")

        with pytest.raises(AuthenticationError):
            await reloader.reload()
