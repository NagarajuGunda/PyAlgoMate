"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import queue

from collections import defaultdict
from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.barfeed.BasicBarEx import BasicBarEx
from pyalgomate.brokers.finvasia import wsclient
from NorenRestApiPy.NorenApi import NorenApi

logger = logging.getLogger(__name__)


class QuoteMessage(object):
    # t	tk	‘tk’ represents touchline acknowledgement
    # e	NSE, BSE, NFO ..	Exchange name
    # tk	22	Scrip Token
    # pp	2 for NSE, BSE & 4 for CDS USDINR	Price precision
    # ts		Trading Symbol
    # ti		Tick size
    # ls		Lot size
    # lp		LTP
    # pc		Percentage change
    # v		volume
    # o		Open price
    # h		High price
    # l		Low price
    # c		Close price
    # ap		Average trade price
    # oi		Open interest
    # poi		Previous day closing Open Interest
    # toi		Total open interest for underlying
    # bq1		Best Buy Quantity 1
    # bp1		Best Buy Price 1
    # sq1		Best Sell Quantity 1
    # sp1		Best Sell Price 1

    def __init__(self, eventDict, tokenMappings):
        self.__eventDict = eventDict
        self.__tokenMappings = tokenMappings

    def __str__(self):
        return f'{self.__eventDict}'

    @property
    def field(self):
        return self.__eventDict["t"]

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def dateTime(self):
        return datetime.datetime.fromtimestamp(int(self.__eventDict['ft']))
        #return self.__eventDict["ct"]

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime)

    @property
    def instrument(self): return f"{self.exchange}|{self.__tokenMappings[f'{self.exchange}|{self.scriptToken}'].split('|')[1]}"

    def getBar(self) -> BasicBarEx:
        open = high = low = close = self.price

        return BasicBarEx(self.dateTime,                
                    open,
                    high,
                    low,
                    close,
                    self.volume,
                    None,
                    bar.Frequency.TRADE,
                    {
                        "Instrument": self.instrument,
                        "Open Interest": self.openInterest,
                        "Message": self.__eventDict
                    }
                )

class LiveTradeFeed(BaseBarFeed):

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

    def __init__(self, api: NorenApi, tokenMappings: dict, instruments: list, timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__instruments = instruments
        self.__instrumentToTokenIdMapping = {instrument: tokenMappings[instrument] for instrument in self.__instruments if instrument in tokenMappings}
        self.__channels = {value: key for key, value in self.__instrumentToTokenIdMapping.items()}
        self.__api = api
        self.__timeout = timeout

        for key, value in self.__instrumentToTokenIdMapping.items():
            self.registerDataSeries(value)

        self.__thread = None
        self.__stopped = False
        self.__lastDateTime = None

    def getApi(self):
        return self.__api

    # Factory method for testing purposes.
    def buildWebSocketClientThread(self):
        return wsclient.WebSocketClientThread(self.__api, self.__channels)

    def getCurrentDateTime(self):
        return datetime.datetime.now()

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

    def barsHaveAdjClose(self):
        return False

    def getLastBar(self, instrument) -> bar.Bar:
        lastBarQuote = self.__thread.getQuotes().get(self.__instrumentToTokenIdMapping[instrument], None)
        if lastBarQuote:
            return QuoteMessage(lastBarQuote, self.__channels).getBar()
        return None

    def getNextBars(self):
        groupedQuoteMessages = defaultdict(dict)
        for lastBar in self.__thread.getQuotes().copy().values():
            quoteBar = QuoteMessage(lastBar, self.__channels).getBar()

            groupedQuoteMessages[quoteBar.getDateTime()][quoteBar.getInstrument()] = quoteBar

        latestDateTime = max(groupedQuoteMessages.keys(), default=None)
        bars = None
        if latestDateTime is not None and self.__lastDateTime != latestDateTime:
            bars = bar.Bars(groupedQuoteMessages[latestDateTime])
            self.__lastDateTime = latestDateTime
        return bars

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        if self.__thread is not None:
            logger.info("Already running!")
            return
        
        super(LiveTradeFeed, self).start()
        if not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        ret = False
        if super(LiveTradeFeed, self).dispatch():
            ret = True
        return ret

    # This should not raise.
    def stop(self):
        pass

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
        return None

    def getLastUpdatedDateTime(self):
        return self.__lastDateTime

    def isDataFeedAlive(self, heartBeatInterval=5):
        if self.__lastDateTime is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.__lastDateTime
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval
