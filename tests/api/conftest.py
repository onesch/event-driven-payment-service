from unittest.mock import AsyncMock
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.database.database import get_db
from src.payment_service.main import app


@pytest_asyncio.fixture
async def client():
    """
    HTTP client bound to the FastAPI app.
    Overrides the ``get_db`` dependency to yield a mocked session.
    """
    async def override_get_db():
        yield AsyncMock()

    app.dependency_overrides[get_db] = override_get_db

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as http_client:
            yield http_client
    finally:
        app.dependency_overrides.pop(get_db, None)
