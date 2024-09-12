import queue
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pyalgomate.brokers.finvasia.broker import (
    LiveBroker,
    Order,
    TradeMonitor,
)


@pytest.fixture
def trade_monitor():
    mock_broker = Mock(spec=LiveBroker)
    mock_broker._LiveBroker__orderUpdateThread = Mock()
    mock_broker._LiveBroker__orderUpdateThread.getQueue.return_value = queue.Queue()
    monitor = TradeMonitor(mock_broker)
    yield monitor


class TestTradeMonitor:

    def test_init(self, trade_monitor):
        assert trade_monitor._TradeMonitor__stop == False
        assert trade_monitor._TradeMonitor__retryData == {}
        assert trade_monitor._TradeMonitor__pendingUpdates == set()

    @pytest.mark.asyncio
    async def test_process_order_update_rejected(self, trade_monitor):
        mock_order_update = {
            "norenordno": "24082700978090",
            "status": "REJECTED",
            "rejreason": "SAF:order is not open to cancel",
            "remarks": "PyAlgoMate order 129094422825040",
        }
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        await trade_monitor.processOrderUpdate(mock_order_update)

        assert len(trade_monitor._TradeMonitor__retryData) == 1
        assert trade_monitor._TradeMonitor__retryData[mock_order]["retryCount"] == 1

    @pytest.mark.asyncio
    async def test_process_order_update_complete(self, trade_monitor):
        mock_order_update = {
            "norenordno": "24082700978090",
            "status": "COMPLETE",
            "remarks": "PyAlgoMate order 129094422825040",
        }
        mock_order = Mock(spec=Order)
        mock_order.getRemarks.return_value = "PyAlgoMate order 129094422825040"
        trade_monitor._TradeMonitor__broker.getActiveOrders.return_value = [mock_order]

        with patch.object(
            trade_monitor, "processOrderEvent", new_callable=AsyncMock
        ) as mock_process_event:
            await trade_monitor.processOrderUpdate(mock_order_update)

        mock_process_event.assert_called_once()

    def test_stop(self, trade_monitor):
        trade_monitor.stop()
        assert trade_monitor._TradeMonitor__stop == True


if __name__ == "__main__":
    pytest.main()
