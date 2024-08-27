import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import asyncio
import queue
from pyalgomate.brokers.finvasia.broker import (
    TradeMonitor,
    OrderEvent,
    LiveBroker,
    Order,
)


@pytest.fixture
def trade_monitor():
    mock_broker = Mock(spec=LiveBroker)
    with patch(
        "pyalgomate.brokers.finvasia.broker.OrderUpdateThread"
    ) as mock_order_update_thread:
        monitor = TradeMonitor(mock_broker)
        monitor._TradeMonitor__zmq_update_thread = mock_order_update_thread.return_value
        yield monitor


class TestTradeMonitor:

    def test_init(self, trade_monitor):
        assert isinstance(trade_monitor._TradeMonitor__queue, queue.Queue)
        assert not trade_monitor._TradeMonitor__stop
        assert trade_monitor._TradeMonitor__retryData == {}
        assert trade_monitor._TradeMonitor__pendingUpdates == set()
        trade_monitor._TradeMonitor__zmq_update_thread.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_order_event_rejected(self, trade_monitor):
        mock_order_update = {
            "t": "om",
            "norenordno": "24082700978090",
            "status": "REJECTED",
            "rejreason": "SAF:order is not open to cancel",
            "remarks": "PyAlgoMate order 129094422825040",
        }
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        await trade_monitor.processOrderUpdate(mock_order, mock_order_update, [])

        assert len(trade_monitor._TradeMonitor__retryData) == 1
        assert trade_monitor._TradeMonitor__retryData[mock_order]["retryCount"] == 1

    @pytest.mark.asyncio
    async def test_process_order_event_complete(self, trade_monitor):
        mock_order_update = {
            "t": "om",
            "norenordno": "24082700978090",
            "status": "COMPLETE",
            "remarks": "PyAlgoMate order 129094422825040",
        }
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        trades = []
        await trade_monitor.processOrderUpdate(mock_order, mock_order_update, trades)

        assert len(trades) == 1
        assert trades[0].getStatus() == "COMPLETE"

    def test_stop(self, trade_monitor):
        trade_monitor.stop()

        assert trade_monitor._TradeMonitor__stop
        trade_monitor._TradeMonitor__zmq_update_thread.stop.assert_called_once()
        trade_monitor._TradeMonitor__zmq_update_thread.join.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_pending_updates(self, trade_monitor):
        mock_order_update = {
            "t": "om",
            "norenordno": "24082700978090",
            "status": "COMPLETE",
            "remarks": "PyAlgoMate order 129094422825040",
        }
        trade_monitor._TradeMonitor__pendingUpdates = set(
            [frozenset(mock_order_update.items())]
        )
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        with patch.object(
            trade_monitor, "processOrderUpdate", new_callable=AsyncMock
        ) as mock_process:
            # Mock the run_async method to process pending updates directly
            async def mock_run_async():
                while trade_monitor._TradeMonitor__pendingUpdates:
                    update = trade_monitor._TradeMonitor__pendingUpdates.pop()
                    update_dict = dict(update)
                    await trade_monitor.processOrderUpdate(mock_order, update_dict, [])
                trade_monitor._TradeMonitor__stop = True

            with patch.object(trade_monitor, "run_async", side_effect=mock_run_async):
                await trade_monitor.run_async()

        mock_process.assert_called_once()
        assert len(trade_monitor._TradeMonitor__pendingUpdates) == 0

    @pytest.mark.asyncio
    async def test_process_order_update_delayed_order(self, trade_monitor):
        mock_order_update = {
            "t": "om",
            "norenordno": "24082700978090",
            "status": "COMPLETE",
            "remarks": "PyAlgoMate order 129094422825040",
        }

        # Simulate order not found initially
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = []

        # Simulate the behavior of run_async when order is not found
        trade_monitor._TradeMonitor__pendingUpdates.add(
            frozenset(mock_order_update.items())
        )

        assert len(trade_monitor._TradeMonitor__pendingUpdates) == 1

        # Now simulate the order being found
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        # Mock the processOrderUpdate method
        with patch.object(
            trade_monitor, "processOrderUpdate", new_callable=AsyncMock
        ) as mock_process:
            # Simulate the behavior of run_async when processing pending updates
            async def mock_run_async():
                for orderUpdate in list(trade_monitor._TradeMonitor__pendingUpdates):
                    update_dict = dict(orderUpdate)
                    order = next(
                        (
                            o
                            for o in trade_monitor._TradeMonitor__broker.getActiveOrders()
                            if o.getRemarks() == update_dict.get("remarks")
                        ),
                        None,
                    )
                    if order:
                        await trade_monitor.processOrderUpdate(order, update_dict, [])
                        trade_monitor._TradeMonitor__pendingUpdates.remove(orderUpdate)

            await mock_run_async()

        assert len(trade_monitor._TradeMonitor__pendingUpdates) == 0
        trade_monitor._TradeMonitor__broker.getActiveOrders.assert_called()
        mock_process.assert_called_once_with(mock_order, mock_order_update, [])


if __name__ == "__main__":
    pytest.main()
