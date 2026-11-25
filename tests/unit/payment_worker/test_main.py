from unittest.mock import AsyncMock, Mock

from src.payment_worker import main as main_module


async def test_setup_topology_connects_before_declaring(monkeypatch):
    """Retry queues and the DLQ must exist before consuming starts."""
    calls = Mock()

    connect = AsyncMock(side_effect=lambda: calls.connect())
    declare_topology = AsyncMock(side_effect=lambda broker: calls.declare(broker))

    monkeypatch.setattr(main_module.broker, "connect", connect)
    monkeypatch.setattr(main_module, "declare_topology", declare_topology)

    await main_module.setup_topology()

    assert [name for name, *_ in calls.mock_calls] == ["connect", "declare"]
    declare_topology.assert_awaited_once_with(main_module.broker)
