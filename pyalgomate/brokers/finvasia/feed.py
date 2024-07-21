"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import traceback
import threading
import zmq
import zmq.asyncio
import asyncio
from queue import Queue, Empty

from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.barfeed.BasicBarEx import BasicBarEx
from NorenRestApiPy.NorenApi import NorenApi
from pyalgomate.core import OptionType
from typing import List, Union, Tuple, Optional
from . import getOptionContract

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
        return self.__eventDict["ft"]

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime)

    @property
    def instrument(
        self): return f"{self.exchange}|{self.__tokenMappings[f'{self.exchange}|{self.scriptToken}'].split('|')[1]}"

    def getBar(self, dateTime=None) -> BasicBarEx:
        open = high = low = close = self.price

        return BasicBarEx(dateTime or self.dateTime,
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
    def __init__(self, api: NorenApi, tokenMappings: dict, instruments: list, zmq_port="5555", timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__instruments = instruments
        self.__instrumentToTokenIdMapping = {
            instrument: tokenMappings[instrument] for instrument in self.__instruments if instrument in tokenMappings}
        self.__tokenIdToInstrumentMappings = {
            value: key for key, value in tokenMappings.items()}

        if len(self.__instruments) != len(self.__instrumentToTokenIdMapping):
            raise Exception(
                f'Could not get tokens for the instruments {[instrument for instrument in self.__instruments if instrument not in tokenMappings]}')

        self.__api = api

        for key, value in self.__instrumentToTokenIdMapping.items():
            self.registerDataSeries(key)

        self.__stopped = False
        self.__lastQuoteDateTime = None
        self.__lastReceivedDateTime = None
        self.__lastUpdateTime = None
        self.__nextBarsTime = None

        # ZeroMQ setup
        self.__context = zmq.asyncio.Context()
        self.__socket = self.__context.socket(zmq.SUB)
        self.__socket.connect(f"tcp://localhost:{zmq_port}")
        self.__socket.setsockopt_string(zmq.SUBSCRIBE, '')

        # Queue to communicate between threads
        self.__queue = Queue()

        self.__latestQuotes = {}

        # Thread to run the asyncio event loop
        self.__loopThread = threading.Thread(target=self.__run_event_loop)
        self.__loopThread.start()

    def getApi(self):
        return self.__api

    def getCurrentDateTime(self):
        return datetime.datetime.now()

    def barsHaveAdjClose(self):
        return False

    def getLastBar(self, instrument):
        lastBarQuote = self.__latestQuotes.get(
            self.__instrumentToTokenIdMapping[instrument], None)
        if lastBarQuote is not None:
            return QuoteMessage(lastBarQuote, self.__tokenIdToInstrumentMappings).getBar()
        return None

    def __run_event_loop(self):
        asyncio.run(self.__async_main())

    async def __async_main(self):
        while not self.__stopped:
            try:
                message = await self.__socket.recv_pyobj()
                key = message['e'] + '|' + message['tk']
                if float(message.get('lp', 0)) <= 0:
                    continue
                self.__latestQuotes[key] = message
                self.__queue.put_nowait(message)
                self.__lastQuoteDateTime = message['ft']
                self.__lastReceivedDateTime = datetime.datetime.now()
            except zmq.Again:
                pass

    def getNextBars(self):
        def getBar(message, lastQuoteDateTime):
            bar = QuoteMessage(message, self.__tokenIdToInstrumentMappings).getBar(
                lastQuoteDateTime)
            return bar.getInstrument(), bar
        bars = None
        lastQuoteDateTime = self.__lastQuoteDateTime
        if self.__lastUpdateTime != lastQuoteDateTime:
            self.__nextBarsTime = datetime.datetime.now()
            self.__lastUpdateTime = lastQuoteDateTime
            barDateTime = self.__nextBarsTime.replace(microsecond=0)
            bars = bar.Bars({
                instrument: bar
                for quoteMessage in list(self.__latestQuotes.values())
                for instrument, bar in [getBar(quoteMessage, barDateTime)]
            })
        return bars

    def peekDateTime(self):
        return None

    def start(self):
        super(LiveTradeFeed, self).start()
        logger.info("LiveTradeFeed started")

    def dispatch(self):
        try:
            ret = False
            if super(LiveTradeFeed, self).dispatch():
                ret = True
            return ret
        except Exception as e:
            logger.error(f'Exception: {e}')
            logger.exception(traceback.format_exc())

    def stop(self):
        self.__stopped = True
        self.__socket.close()
        self.__context.term()
        self.__loopThread.join()

    def join(self):
        pass

    def eof(self):
        return self.__stopped

    def getOrderBookUpdateEvent(self):
        return None

    def getLastUpdatedDateTime(self):
        return self.__lastQuoteDateTime

    def getLastReceivedDateTime(self):
        return self.__lastReceivedDateTime

    def getNextBarsDateTime(self):
        return self.__nextBarsTime

    def isDataFeedAlive(self, heartBeatInterval=5):
        if self.__lastQuoteDateTime is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.__lastQuoteDateTime
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval

    def findNearestPremiumOption(self, expiry: datetime.datetime, optionType: OptionType,
                                 premium: float, time: datetime.datetime) -> Optional[Tuple[str, float]]:
        nearestOption = None
        nearestPremium = None
        minDifference = float('inf')

        for tokenId, quote in self.__latestQuotes.items():
            instrument = self.__tokenIdToInstrumentMappings[tokenId]
            optionContract = getOptionContract(instrument)

            if optionContract is None or optionContract.expiry != expiry or \
                    optionContract.type != ('c' if optionType == OptionType.CALL else 'p'):
                continue

            close = QuoteMessage(
                quote, self.__tokenIdToInstrumentMappings).price

            difference = abs(close - premium)

            if difference < minDifference:
                minDifference = difference
                nearestOption = instrument
                nearestPremium = close

        return nearestOption, nearestPremium
