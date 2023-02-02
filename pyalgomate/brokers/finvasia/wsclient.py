"""
.. moduleauthor:: Nagaraju Gunda
"""

import threading
import queue
import logging
import datetime
import pytz

from pyalgotrade import bar

logger = logging.getLogger(__name__)


class SubscribeEvent(object):
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

    def __init__(self, eventDict):
        self.__eventDict = eventDict

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def dateTime(self):
        return datetime.datetime.now()
        now = datetime.datetime.now()
        milliseconds = int(str(now)[20:])
        microseconds = int(str(datetime.datetime.now().microsecond)[:2])
        return datetime.datetime.fromtimestamp(int(self.__eventDict["ft"])) + datetime.timedelta(milliseconds=milliseconds, microseconds=microseconds)

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def seq(self): return int(self.dateTime())

    @property
    def instrument(self): return f"{self.exchange}|{self.scriptToken}"

    def TradeBar(self):
        open = high = low = close = self.price

        return bar.BasicBar(self.dateTime, open, high, low, close, self.volume, None, bar.Frequency.TRADE, {"instrument": self.instrument})


class WebSocketClient:
    """
    This websocket client class is designed to be running in a separate thread and for that reason
    events are pushed into a queue.
    """

    class Event:
        DISCONNECTED = 1
        TRADE = 2
        ORDER_BOOK_UPDATE = 3

    def __init__(self, queue, api, subscriptions):
        assert len(subscriptions), "Missing subscriptions"
        self.__queue = queue
        self.__api = api
        self.__pending_subscriptions = subscriptions
        self.__connected = False
        self.__initialized = threading.Event()

    def startClient(self):
        self.__api.start_websocket(order_update_callback=self.onOrderBookUpdate,
                                   subscribe_callback=self.onQuoteUpdate,
                                   socket_open_callback=self.onOpened)

    def stopClient(self):
        try:
            if self.__connected:
                self.close()
        except Exception as e:
            logger.error("Failed to close connection: %s" % e)

    def setInitialized(self):
        assert self.isConnected()
        self.__initialized.set()

    def waitInitialized(self, timeout):
        logger.info(f"Waiting for WebSocketClient waitInitialized with timeout of {timeout}")
        return self.__initialized.wait(timeout)

    def isConnected(self):
        return self.__connected

    def onOpened(self):
        self.__connected = True
        for channel in self.__pending_subscriptions:
            logger.info("Subscribing to channel %s." % channel)
            self.__api.subscribe(channel)

    def onClosed(self, code, reason):
        if self.__connected:
            self.__connected = False

        logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onError(self, exception):
        logger.error("Error: %s." % exception)

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onTrade(self, trade):
        self.__queue.put((WebSocketClient.Event.TRADE, trade))

    def onQuoteUpdate(self, message):
        logger.debug(message)

        field = message.get("t")

        # t='tk' is sent once on subscription for each instrument.
        # this will have all the fields with the most recent value thereon t='tf' is sent for fields that have changed.
        if field == "tf":
            self.onTrade(SubscribeEvent(message).TradeBar())
        elif field == "tk":
            self.__onSubscriptionSucceeded(SubscribeEvent(message))
            self.onTrade(SubscribeEvent(message).TradeBar())
        else:
            self.onUnknownEvent(SubscribeEvent(message))

    def onOrderBookUpdate(self, message):
        hello = True
        # orderBookUpdate = message
        # self.__queue.put(
        #     (WebSocketClient.Event.ORDER_BOOK_UPDATE, orderBookUpdate))

    def __onSubscriptionSucceeded(self, event):
        instrument = f"{event.exchange}|{event.scriptToken}"

        logger.info(f"Subscription succeeded for <{instrument}>")

        self.__pending_subscriptions.remove(instrument)

        if not self.__pending_subscriptions:
            self.setInitialized()


class WebSocketClientThreadBase(threading.Thread):
    def __init__(self, wsCls, *args, **kwargs):
        super(WebSocketClientThreadBase, self).__init__()
        self.__queue = queue.Queue()
        self.__wsClient = None
        self.__wsCls = wsCls
        self.__args = args
        self.__kwargs = kwargs

    def getQueue(self):
        return self.__queue

    def waitInitialized(self, timeout):
        return self.__wsClient is not None and self.__wsClient.waitInitialized(timeout)

    def run(self):
        # We create the WebSocketClient right in the thread, instead of doing so in the constructor,
        # because it has thread affinity.
        try:
            self.__wsClient = self.__wsCls(
                self.__queue, *self.__args, **self.__kwargs)
            logger.debug("Running websocket client")
            self.__wsClient.startClient()
        except Exception as e:
            logger.exception("Unhandled exception %s" % e)
            self.__wsClient.stopClient()

    def stop(self):
        try:
            if self.__wsClient is not None:
                logger.debug("Stopping websocket client")
                self.__wsClient.stopClient()
        except Exception as e:
            logger.error("Error stopping websocket client: %s" % e)


class WebSocketClientThread(WebSocketClientThreadBase):
    """
    This thread class is responsible for running a WebSocketClient.
    """

    def __init__(self, api, channels):
        super(WebSocketClientThread, self).__init__(
            WebSocketClient, api, channels)
