import pytest
from unittest.mock import AsyncMock, Mock

import src.database.database as database_module


@pytest.mark.asyncio
async def test_get_db_commits_on_success(monkeypatch):
    session = AsyncMock()
    session.__aenter__.return_value = session
    monkeypatch.setattr(
        database_module,
        "async_session_maker",
        Mock(return_value=session),
    )

    agen = database_module.get_db()
    yielded = await agen.__anext__()

    assert yielded is session

    with pytest.raises(StopAsyncIteration):
        await agen.__anext__()

    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_db_rolls_back_on_exception(monkeypatch):
    session = AsyncMock()
    session.__aenter__.return_value = session
    monkeypatch.setattr(
        database_module,
        "async_session_maker",
        Mock(return_value=session),
    )

    agen = database_module.get_db()
    yielded = await agen.__anext__()

    assert yielded is session

    with pytest.raises(RuntimeError, match="boom"):
        await agen.athrow(RuntimeError("boom"))

    session.rollback.assert_awaited_once_with()
    session.commit.assert_not_awaited()
