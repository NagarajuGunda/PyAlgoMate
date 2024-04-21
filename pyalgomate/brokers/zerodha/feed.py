"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import six
import queue
import traceback

from .kiteext import KiteExt

from pyalgotrade import bar
from pyalgotrade import observer

from pyalgomate.brokers.zerodha import wsclient
from pyalgomate.barfeed import BaseBarFeed

logger = logging.getLogger(__name__)


class TradeBar(bar.Bar):
    def __init__(self, trade):
        self.__dateTime = trade.getDateTime()
        self.__trade = trade

    def getInstrument(self):
        return self.__trade.getExtraColumns().get("instrument")

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted:
            raise Exception("Adjusted close is not available")

    def getTrade(self):
        return self.__trade

    def getTradeId(self):
        return self.__trade.getId()

    def getFrequency(self):
        return bar.Frequency.TRADE

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        return self.__trade.getPrice()

    def getHigh(self, adjusted=False):
        return self.__trade.getPrice()

    def getLow(self, adjusted=False):
        return self.__trade.getPrice()

    def getClose(self, adjusted=False):
        return self.__trade.getPrice()

    def getVolume(self):
        return self.__trade.getAmount() if self.__trade.getAmount() is not None else 0

    def getAdjClose(self):
        return None

    def getTypicalPrice(self):
        return self.__trade.getPrice()

    def getPrice(self):
        return self.__trade.getPrice()

    def getUseAdjValue(self):
        return False

    def isBuy(self):
        return self.__trade.isBuy()

    def isSell(self):
        return not self.__trade.isBuy()


class ZerodhaLiveFeed(BaseBarFeed):
    """A real-time BarFeed that builds bars from live trades.

    :param instruments: A list of currency pairs.
    :type instruments: list of :class:`pyalgotrade.instrument.Instrument` or a string formatted like
        QUOTE_SYMBOL/PRICE_CURRENCY..
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        Note that a Bar will be created for every trade, so open, high, low and close values will all be the same.
    """

    QUEUE_TIMEOUT = 0.01

    def __init__(self, api: KiteExt, tokenMappings, timeout=10, maxLen=None):
        super(ZerodhaLiveFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__tradeBars = queue.Queue()
        self.__channels = tokenMappings
        self.__api = api
        self.__timeout = timeout

        for key, value in tokenMappings.items():
            self.registerDataSeries(value)

        self.__thread = None
        self.__enableReconnection = True
        self.__stopped = False
        self.__orderBookUpdateEvent = observer.Event()
        self.__lastDataTime = None
        self.__nextBarsTime = None
        self.__lastUpdateTime = None

    def getApi(self):
        return self.__api

    # Factory method for testing purposes.
    def buildWebSocketClientThread(self):
        return wsclient.WebSocketClientThread(self.__api, self.__channels)

    def getCurrentDateTime(self):
        return datetime.datetime.now()

    def enableReconection(self, enableReconnection):
        self.__enableReconnection = enableReconnection

    def __initializeClient(self):
        logger.info("Initializing websocket client")
        initialized = False
        try:
            # Start the thread that runs the client.
            self.__thread = self.buildWebSocketClientThread()
            self.__thread.start()
        except Exception as e:
            logger.error("Error connecting : %s" % str(e))

        logger.info("Waiting for websocket initialization to complete")
        while not initialized and not self.__stopped:
            initialized = self.__thread.waitInitialized(self.__timeout)

        if initialized:
            logger.info("Initialization completed")
        else:
            logger.error("Initialization failed")
        return initialized

    def __onDisconnected(self):
        if self.__enableReconnection:
            logger.info("Reconnecting")
            while not self.__stopped and not self.__initializeClient():
                pass
        elif not self.__stopped:
            logger.info("Stopping")
            self.__stopped = True

    def __dispatchImpl(self, eventFilter):
        ret = False
        try:
            eventType, eventData = self.__thread.getQueue().get(
                True, ZerodhaLiveFeed.QUEUE_TIMEOUT)
            if eventFilter is not None and eventType not in eventFilter:
                return False

            ret = True
            if eventType == wsclient.WebSocketClient.Event.TRADE:
                self.__onTrade(eventData)
                self.__lastDataTime = datetime.datetime.now()
            elif eventType == wsclient.WebSocketClient.Event.ORDER_BOOK_UPDATE:
                self.__orderBookUpdateEvent.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.DISCONNECTED:
                self.__onDisconnected()
            else:
                ret = False
                logger.error(
                    "Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except six.moves.queue.Empty:
            pass
        return ret

    def __onTrade(self, trade):
        self.__tradeBars.put(
            {trade.getExtraColumns().get("Instrument"): trade})

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        if self.__tradeBars.qsize() > 0:
            return bar.Bars(self.__tradeBars.get())
        lastQuoteDateTime = self.__thread.getWsClient().getLastQuoteDateTime()
        self.__nextBarsTime = datetime.datetime.now()
        self.__lastUpdateTime = lastQuoteDateTime
        return None

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        if self.__thread is not None:
            logger.info("Already running!")
            return

        super(ZerodhaLiveFeed, self).start()
        if not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def dispatch(self):
        try:
            # Note that we may return True even if we didn't dispatch any Bar
            # event.
            ret = False
            if self.__dispatchImpl(None):
                ret = True
            if super(ZerodhaLiveFeed, self).dispatch():
                ret = True
            return ret
        except Exception as e:
            logger.error(
                f'Exception: {e}')
            logger.exception(traceback.format_exc())

    # This should not raise.
    def stop(self):
        try:
            self.__stopped = True
            if self.__thread is not None and self.__thread.is_alive():
                logger.info("Stopping websocket client.")
                self.__thread.stop()
        except Exception as e:
            logger.error("Error shutting down client: %s" % (str(e)))

    # This should not raise.
    def join(self):
        if self.__thread is not None:
            self.__thread.join()

    def eof(self):
        return self.__stopped

    def getOrderBookUpdateEvent(self):
        """
        Returns the event that will be emitted when the orderbook gets updated.

        Eventh handlers should receive one parameter:
         1. A :class:`pyalgotrade.bitstamp.wsclient.OrderBookUpdate` instance.

        :rtype: :class:`pyalgotrade.observer.Event`.
        """
        return self.__orderBookUpdateEvent

    def getLastUpdatedDateTime(self):
        return self.__lastDataTime

    def isDataFeedAlive(self, heartBeatInterval=5):

        if self.__lastUpdateTime is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.__lastUpdateTime
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval
