"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import threading
import queue

from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logger = logging.getLogger(__name__)


class SubscribeEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict
        self.__datetime = None

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def tradingSymbol(self):
        return self.__eventDict["ts"]

    @property
    def dateTime(self):
        if self.__datetime is None:
            self.__datetime = datetime.datetime.fromtimestamp(int(self.__eventDict['ft'])) if self.__eventDict.get(
                'ft', None) is not None else datetime.datetime.now()

        return self.__datetime

    @dateTime.setter
    def dateTime(self, value):
        self.__datetime = value

    @property
    def tickDateTime(self):
        return datetime.datetime.fromtimestamp(int(self.__eventDict['ft'])) if self.__eventDict.get(
            'ft', None) is not None else datetime.datetime.now()

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime())

    @property
    def instrument(self): return f"{self.exchange}|{self.tradingSymbol}"

    def Bar(self):
        open = high = low = close = self.price

        return bar.BasicBar(self.dateTime,
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
                                "Date/Time": self.tickDateTime
                            })


class LiveTradeFeed(BaseBarFeed):
    def __init__(self, api: ShoonyaApi, tokenMappings, timeout=10, maxLen=None):
        assert len(tokenMappings), "Missing subscriptions"

        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__tradeBars = queue.Queue()
        self.__api = api
        self.__tokenMappings = tokenMappings
        self.__pending_subscriptions = list(tokenMappings.keys())
        self.__timeout = timeout
        self.__initialized = threading.Event()
        self.__stopped = False
        self.__lastDataTime = None

        for key, value in tokenMappings.items():
            self.registerDataSeries(value)

    def getApi(self):
        return self.__api

    def getCurrentDateTime(self):
        return datetime.datetime.now()

    def barsHaveAdjClose(self):
        return False

    def eof(self):
        return self.__stopped

    def join(self):
        pass

    def peekDateTime(self):
        return None

    def start(self):
        if self.__initialized.is_set():
            logger.info("Feed already started!")
            return

        super(LiveTradeFeed, self).start()
        self.startClient()
        if not self.waitInitialized():
            self.__stopped = True
            raise Exception("Initialization failed")

    def stop(self):
        try:
            self.__stopped = True
        except Exception as e:
            logger.error(f"Error stopping feed: {e}")

    def getNextBars(self):
        if self.__tradeBars.qsize() > 0:
            return bar.Bars(self.__tradeBars.get())

        return None

    def setInitialized(self):
        self.__initialized.set()

    def waitInitialized(self):
        logger.info(
            f"Waiting for waitInitialized with timeout of {self.__timeout}")
        initialized = self.__initialized.wait(self.__timeout)

        if initialized:
            logger.info("Initialization completed")
        else:
            logger.error("Initialization failed")
        return initialized

    def startClient(self):
        self.__api.start_websocket(order_update_callback=self.onOrderBookUpdate,
                                   subscribe_callback=self.onQuoteUpdate,
                                   socket_open_callback=self.onOpened,
                                   socket_close_callback=self.onClosed,
                                   socket_error_callback=self.onError)

    def onOrderBookUpdate(self, message):
        logger.debug(message)

    def onQuoteUpdate(self, message):
        logger.debug(message)

        field = message.get("t")
        message["ts"] = self.__tokenMappings[f"{message['e']}|{message['tk']}"].split('|')[
            1]

        # t='tk' is sent once on subscription for each instrument.
        # this will have all the fields with the most recent value thereon t='tf' is sent for fields that have changed.
        subscribeEvent = SubscribeEvent(message)

        if field not in ["tf", "tk"]:
            self.onUnknownEvent(subscribeEvent)
            return

        if field == "tk":
            self.__onSubscriptionSucceeded(subscribeEvent)

        self.onTrade(subscribeEvent.Bar())
        self.__lastDataTime = datetime.datetime.now()

    def __onSubscriptionSucceeded(self, event: SubscribeEvent):
        logger.info(
            f"Subscription succeeded for <{event.exchange}|{event.tradingSymbol}>")

        self.__pending_subscriptions.remove(
            f"{event.exchange}|{event.scriptToken}")

        if not self.__pending_subscriptions:
            self.setInitialized()

    def onTrade(self, tradeBar):
        if tradeBar.getPrice() > 0:
            self.__tradeBars.put(
                {tradeBar.getExtraColumns().get("Instrument"): tradeBar})

    def onUnknownEvent(self, event):
        logger.warning(f"Unknown event: {event}")

    def onOpened(self):
        for channel in self.__pending_subscriptions:
            logger.info("Subscribing to channel %s." % channel)
            self.__api.subscribe(channel)

    def onClosed(self):
        logger.warn("Websocket disconnected")

    def onError(self, exception):
        logger.error(f"Error: {exception}")

    def isDataFeedAlive(self, heartBeatInterval=5):
        if self.__lastDataTime is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.__lastDataTime
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval
