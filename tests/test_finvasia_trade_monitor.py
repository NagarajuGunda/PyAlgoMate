import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import zmq.asyncio

from pyalgomate.brokers.finvasia.broker import Order, TradeMonitor


@pytest.fixture
def mock_broker():
    mock = Mock()
    mock.loop = asyncio.get_event_loop()
    return mock


@pytest.fixture
def trade_monitor(mock_broker):
    return TradeMonitor(mock_broker)


@pytest.mark.asyncio
async def test_handle_order_update_pending(trade_monitor):
    order_update = {
        "t": "om",
        "norenordno": "24091400010992",
        "status": "PENDING",
        "remarks": "PyAlgoMate order 140513410642320",
    }

    await trade_monitor.handle_order_update(order_update)

    # Assert that no further processing is done for PENDING orders
    trade_monitor._TradeMonitor__broker.getActiveOrders.assert_not_called()


@pytest.mark.asyncio
async def test_handle_order_update_open(trade_monitor):
    order_update = {
        "t": "om",
        "norenordno": "24091400010992",
        "status": "OPEN",
        "remarks": "PyAlgoMate order 140513410642320",
    }

    mock_order = Mock(spec=Order)
    trade_monitor._TradeMonitor__broker.getActiveOrder.return_value = mock_order
    mock_order.getRemarks.return_value = "PyAlgoMate order 140513410642320"

    await trade_monitor.handle_order_update(order_update)

    assert mock_order in trade_monitor._TradeMonitor__openOrders


@pytest.mark.asyncio
async def test_handle_order_update_complete(trade_monitor):
    order_update = {
        "t": "om",
        "norenordno": "24091400010992",
        "status": "COMPLETE",
        "remarks": "PyAlgoMate order 140513410642320",
    }

    mock_order = Mock(spec=Order)
    trade_monitor._TradeMonitor__broker.getActiveOrder.return_value = mock_order
    mock_order.getRemarks.return_value = "PyAlgoMate order 140513410642320"
    trade_monitor._TradeMonitor__broker._onUserTrades = AsyncMock()

    await trade_monitor.handle_order_update(order_update)

    trade_monitor._TradeMonitor__broker._onUserTrades.assert_called_once()
    assert mock_order not in trade_monitor._TradeMonitor__openOrders
    assert mock_order not in trade_monitor._TradeMonitor__retryData


@pytest.mark.asyncio
async def test_handle_order_update_rejected(trade_monitor):
    order_update = {
        "t": "om",
        "norenordno": "24091400010992",
        "status": "REJECTED",
        "remarks": "PyAlgoMate order 140513410642320",
        "rejreason": "Some rejection reason",
    }

    mock_order = Mock(spec=Order)
    trade_monitor._TradeMonitor__broker.getActiveOrder.return_value = mock_order
    mock_order.getRemarks.return_value = "PyAlgoMate order 140513410642320"
    trade_monitor._TradeMonitor__broker._onUserTrades = AsyncMock()
    trade_monitor._TradeMonitor__broker.placeOrder = AsyncMock()

    await trade_monitor.handle_order_update(order_update)

    trade_monitor._TradeMonitor__broker.placeOrder.assert_called_once_with(mock_order)


if __name__ == "__main__":
    pytest.main()
