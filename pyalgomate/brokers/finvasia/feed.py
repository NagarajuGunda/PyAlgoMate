"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import six
import queue

import abc

from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio

from pyalgotrade import bar
from pyalgotrade import barfeed
from pyalgotrade import observer
from pyalgomate.brokers.finvasia import wsclient

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
        return self.__trade.getAmount()

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

class MyBarFeed(feed.BaseFeed):
    """Base class for :class:`pyalgotrade.bar.Bar` providing feeds.

    :param frequency: The bars frequency. Valid values defined in :class:`pyalgotrade.bar.Frequency`.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, frequency, maxLen=None):
        super(MyBarFeed, self).__init__(maxLen)
        self.__frequency = frequency
        self.__useAdjustedValues = False
        self.__defaultInstrument = None
        self.__currentBars = None
        self.__lastBars = {}

    def reset(self):
        self.__currentBars = None
        self.__lastBars = {}
        super(MyBarFeed, self).reset()

    def setUseAdjustedValues(self, useAdjusted):
        if useAdjusted and not self.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        # This is to affect future dataseries when they get created.
        self.__useAdjustedValues = useAdjusted
        # Update existing dataseries
        for instrument in self.getRegisteredInstruments():
            self[instrument].setUseAdjustedValues(useAdjusted)

    # Return the datetime for the current bars.
    @abc.abstractmethod
    def getCurrentDateTime(self):
        raise NotImplementedError()

    # Return True if bars provided have adjusted close values.
    @abc.abstractmethod
    def barsHaveAdjClose(self):
        raise NotImplementedError()

    # Subclasses should implement this and return a pyalgotrade.bar.Bars or None if there are no bars.
    @abc.abstractmethod
    def getNextBars(self):
        """Override to return the next :class:`pyalgotrade.bar.Bars` in the feed or None if there are no bars.

        .. note::
            This is for BaseBarFeed subclasses and it should not be called directly.
        """
        raise NotImplementedError()

    def createDataSeries(self, key, maxLen):
        ret = bards.BarDataSeries(maxLen)
        ret.setUseAdjustedValues(self.__useAdjustedValues)
        return ret

    def getNextValues(self):
        dateTime = None
        bars = self.getNextBars()
        if bars is not None:
            dateTime = bars.getDateTime()

            # Check that current bar datetimes are greater than the previous one.
            if self.__currentBars is not None and self.__currentBars.getDateTime() > dateTime:
                return (None, None)
                raise Exception(
                    "Bar date times are not in order. Previous datetime was %s and current datetime is %s" % (
                        self.__currentBars.getDateTime(),
                        dateTime
                    )
                )

            # Update self.__currentBars and self.__lastBars
            self.__currentBars = bars
            for instrument in bars.getInstruments():
                self.__lastBars[instrument] = bars[instrument]
        return (dateTime, bars)

    def getFrequency(self):
        return self.__frequency

    def isIntraday(self):
        return self.__frequency < bar.Frequency.DAY

    def getCurrentBars(self):
        """Returns the current :class:`pyalgotrade.bar.Bars`."""
        return self.__currentBars

    def getLastBar(self, instrument):
        """Returns the last :class:`pyalgotrade.bar.Bar` for a given instrument, or None."""
        return self.__lastBars.get(instrument, None)

    def getDefaultInstrument(self):
        """Returns the last instrument registered."""
        return self.__defaultInstrument

    def getRegisteredInstruments(self):
        """Returns a list of registered intstrument names."""
        return self.getKeys()

    def registerInstrument(self, instrument):
        self.__defaultInstrument = instrument
        self.registerDataSeries(instrument)

    def getDataSeries(self, instrument=None):
        """Returns the :class:`pyalgotrade.dataseries.bards.BarDataSeries` for a given instrument.

        :param instrument: Instrument identifier. If None, the default instrument is returned.
        :type instrument: string.
        :rtype: :class:`pyalgotrade.dataseries.bards.BarDataSeries`.
        """
        if instrument is None:
            instrument = self.__defaultInstrument
        return self[instrument]

    def getDispatchPriority(self):
        return dispatchprio.BAR_FEED

class LiveTradeFeed(MyBarFeed):

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

    def __init__(self, api, tokenMappings, timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
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
                True, LiveTradeFeed.QUEUE_TIMEOUT)
            if eventFilter is not None and eventType not in eventFilter:
                return False

            ret = True
            if eventType == wsclient.WebSocketClient.Event.TRADE:
                self.__onTrade(eventData)
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

        return None

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        super(LiveTradeFeed, self).start()
        if self.__thread is not None:
            raise Exception("Already running")
        elif not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        ret = False
        if self.__dispatchImpl(None):
            ret = True
        if super(LiveTradeFeed, self).dispatch():
            ret = True
        return ret

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
