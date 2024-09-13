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

    def test_stop(self, trade_monitor):
        trade_monitor.stop()
        assert trade_monitor._TradeMonitor__stop == True


if __name__ == "__main__":
    pytest.main()
