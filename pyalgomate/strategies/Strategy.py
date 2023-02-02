import yaml
import pyotp
import logging

from pyalgotrade import strategy
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.INFO)

class Strategy(strategy.BaseStrategy):
    def __init__(self, feed, broker):
        super(Strategy, self).__init__(feed, broker)

        # Subscribe to order book update events to get bid/ask prices to trade.
        feed.getOrderBookUpdateEvent().subscribe(self.__onOrderBookUpdate)

    def __onOrderBookUpdate(self, orderBookUpdate):
        bid = orderBookUpdate.getBidPrices()[0]
        ask = orderBookUpdate.getAskPrices()[0]

        if bid != self.__bid or ask != self.__ask:
            self.__bid = bid
            self.__ask = ask
            self.info("Order book updated. Best bid: %s. Best ask: %s" %
                      (self.__bid, self.__ask))

    def onEnterOk(self, position):
        self.info("Position opened at %s" %
                  (position.getEntryOrder().getExecutionInfo().getPrice()))

    def onEnterCanceled(self, position):
        self.info("Position entry canceled")

    def onExitOk(self, position):
        self.info("Position closed at %s" %
                  (position.getExitOrder().getExecutionInfo().getPrice()))

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitLimit(self.__bid)

    def onBars(self, bars):
        for key, value in bars.items():
            self.info("Instrument: %s. Datetime: %s. Price: %s. Volume: %s." %
                      (key, value.getDateTime(), value.getClose(), value.getVolume()))
