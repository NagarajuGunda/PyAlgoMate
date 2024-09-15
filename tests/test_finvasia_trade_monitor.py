import asyncio
from unittest.mock import AsyncMock, Mock, patch

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
    trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]
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
    trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]
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
    trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]
    mock_order.getRemarks.return_value = "PyAlgoMate order 140513410642320"
    trade_monitor._TradeMonitor__broker._onUserTrades = AsyncMock()
    trade_monitor._TradeMonitor__broker.placeOrder = AsyncMock()

    await trade_monitor.handle_order_update(order_update)

    trade_monitor._TradeMonitor__broker.placeOrder.assert_called_once_with(mock_order)


@pytest.mark.asyncio
async def test_process_zmq_updates(trade_monitor):
    with patch.object(zmq.asyncio, "Context"):
        trade_monitor._TradeMonitor__zmq_socket = AsyncMock()
        trade_monitor._TradeMonitor__zmq_socket.recv_multipart.side_effect = [
            (
                b"ORDER_UPDATE",
                b'{"status": "COMPLETE", "remarks": "PyAlgoMate order 1"}',
            ),
            zmq.Again(),
        ]

        mock_order = Mock(spec=Order)
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]
        mock_order.getRemarks.return_value = "PyAlgoMate order 1"

        # Patch the handle_order_update method
        with patch.object(
            trade_monitor, "handle_order_update", new_callable=AsyncMock
        ) as mock_handle_order_update, patch.object(
            trade_monitor, "process_order_update", new_callable=AsyncMock
        ) as mock_process_order_update:
            # Run process_zmq_updates in the background
            task = asyncio.create_task(trade_monitor.process_zmq_updates())

            # Wait a short time to allow the task to process
            await asyncio.sleep(0.1)

            # Cancel the task (since it's designed to run indefinitely)
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

            # Assert that handle_order_update was called with the correct arguments
            mock_handle_order_update.assert_called_once_with(
                {"status": "COMPLETE", "remarks": "PyAlgoMate order 1"}
            )

        trade_monitor._TradeMonitor__zmq_socket.recv_multipart.assert_called()


@pytest.mark.asyncio
async def test_retry_open_order(trade_monitor):
    mock_order = Mock(spec=Order)
    mock_order.getId.return_value = "mock_order_id"
    mock_order.getInstrument.return_value = "SAMPLE_INSTRUMENT"

    trade_monitor._TradeMonitor__retryData[mock_order] = {
        "retryCount": 0,
        "lastRetryTime": 0,
    }

    # Mock the modifyFinvasiaOrder method to be an AsyncMock
    trade_monitor._TradeMonitor__broker.modifyFinvasiaOrder = AsyncMock()

    # Mock the getLastPrice method of the broker
    trade_monitor._TradeMonitor__broker.getLastPrice = Mock(return_value=100.0)

    await trade_monitor.retry_open_order(mock_order, 0)

    # Assert that modifyFinvasiaOrder was called with the correct arguments
    trade_monitor._TradeMonitor__broker.modifyFinvasiaOrder.assert_called_once_with(
        order=mock_order,
        newprice_type="LMT",
        newprice=100.0,
    )

    # Assert that the retry count was incremented
    assert trade_monitor._TradeMonitor__retryData[mock_order]["retryCount"] == 1

    # Assert that the lastRetryTime was updated (it should be non-zero now)
    assert trade_monitor._TradeMonitor__retryData[mock_order]["lastRetryTime"] > 0


@pytest.mark.asyncio
async def test_retry_open_order_final_attempt(trade_monitor):
    mock_order = Mock(spec=Order)
    mock_order.getId.return_value = "mock_order_id"
    trade_monitor._TradeMonitor__retryData[mock_order] = {
        "retryCount": 1,
        "lastRetryTime": 0,
    }

    # Mock the modifyFinvasiaOrder method to be an AsyncMock
    trade_monitor._TradeMonitor__broker.modifyFinvasiaOrder = AsyncMock()

    await trade_monitor.retry_open_order(mock_order, 1)

    trade_monitor._TradeMonitor__broker.modifyFinvasiaOrder.assert_called_once_with(
        order=mock_order, newprice_type="MKT"
    )

    # Assert that the retry count was incremented
    assert trade_monitor._TradeMonitor__retryData[mock_order]["retryCount"] == 2

    # Assert that the lastRetryTime was updated (it should be non-zero now)
    assert trade_monitor._TradeMonitor__retryData[mock_order]["lastRetryTime"] > 0


if __name__ == "__main__":
    pytest.main()
