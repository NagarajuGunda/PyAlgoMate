"""
.. moduleauthor:: Nagaraju Gunda
"""

import threading
import queue
import logging
import datetime
import pytz
from .kiteext import KiteExt

from pyalgotrade import bar

logger = logging.getLogger(__name__)


class WebSocketClient:
    """
    This websocket client class is designed to be running in a separate thread and for that reason
    events are pushed into a queue.
    """

    class Event:
        DISCONNECTED = 1
        TRADE = 2
        ORDER_BOOK_UPDATE = 3

    def __init__(self, queue, api: KiteExt, tokenMappings):
        assert len(tokenMappings), "Missing subscriptions"
        self.__queue = queue
        self.__api = api
        self.__kws = None
        self.__tokenMappings = tokenMappings
        self.__pending_subscriptions = list(tokenMappings.keys())
        self.__connected = False
        self.__initialized = threading.Event()
        self.__lastReceivedDateTime = None
        self.__lastQuoteDateTime = None

    def getLastQuoteDateTime(self):
        return self.__lastQuoteDateTime
    
    def startClient(self):
        self.__kws = self.__api.kws()
        # Assign the callbacks.
        self.__kws.on_ticks = self.onQuoteUpdate
        self.__kws.on_order_update = self.onOrderBookUpdate
        self.__kws.on_connect = self.onOpened
        self.__kws.on_close = self.onClosed

        self.__kws.connect(threaded=True)

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
        logger.info(
            f"Waiting for WebSocketClient waitInitialized with timeout of {timeout}")
        return self.__initialized.wait(timeout)

    def isConnected(self):
        return self.__connected

    def onOpened(self, ws, response):
        self.__connected = True

        logger.info(f"Subscribing to channel {self.__pending_subscriptions}")

        # Callback on successful connect.
        # mode available MODE_FULL,MODE_LTP,MODE_QUOTE
        ws.subscribe(self.__pending_subscriptions)
        ws.set_mode(ws.MODE_FULL, self.__pending_subscriptions)

    def onClosed(self, ws, code, reason):
        if self.__connected:
            self.__connected = False

        logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onError(self, ws, code, reason):
        logger.error(f'Ticker errored out. code = {code}, reason = {reason}')

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onTrade(self, trade):
        if trade.getPrice() > 0:
            self.__queue.put((WebSocketClient.Event.TRADE, trade))

    def onQuoteUpdate(self, ws, ticks):
        logger.debug(ticks)
        self.__lastReceivedDateTime = datetime.datetime.now()
        self.__lastQuoteDateTime = self.__lastReceivedDateTime.replace(microsecond=0)

        for tick in ticks:
            tokenId = tick['instrument_token']
            instrument = self.__tokenMappings[tokenId]
            ltp = tick['last_price']
            volume = tick.get('volume_traded', 0)

            if tokenId in self.__pending_subscriptions:
                self.__onSubscriptionSucceeded(tokenId)

            basicBar = bar.BasicBar(datetime.datetime.now(),
                                    ltp,
                                    ltp,
                                    ltp,
                                    ltp,
                                    volume,
                                    None,
                                    bar.Frequency.TRADE,
                                    {
                "Instrument": instrument,
                "Open Interest": tick.get('oi', 0),
                "Date/Time": tick.get('last_trade_time', datetime.datetime.now())
            })

            self.onTrade(basicBar)

    def onOrderBookUpdate(self, message):
        hello = True

    def __onSubscriptionSucceeded(self, tokenId):
        logger.info(f"Subscription succeeded for <{self.__tokenMappings[tokenId]}>")

        self.__pending_subscriptions.remove(tokenId)

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

    def getWsClient(self) -> WebSocketClient:
        return self.__wsClient
        
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

    def __init__(self, api, tokenMappings):
        super(WebSocketClientThread, self).__init__(
            WebSocketClient, api, tokenMappings)
