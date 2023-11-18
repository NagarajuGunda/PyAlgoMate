"""
.. moduleauthor:: Sai Krishna
"""

import threading
import queue
import logging
import datetime

from pyalgotrade import bar

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

    # @property
    # def tradingSymbol(self):
    #     return self.__eventDict["ts"]

    @property
    def dateTime(self):
        if self.__datetime is None:
            ftdm_str = self.__eventDict.get('ftdm')
            if ftdm_str is not None:
                self.__datetime = datetime.datetime.strptime(
                    ftdm_str, '%d/%m/%Y %H:%M:%S')
            else:
                self.__datetime = datetime.datetime.now()

        return self.__datetime

    @dateTime.setter
    def dateTime(self, value):
        self.__datetime = value

    @property
    def tickDateTime(self):
        fdtm_str = self.__eventDict.get('fdtm')
        if fdtm_str is not None:
            return datetime.datetime.strptime(fdtm_str, '%d/%m/%Y %H:%M:%S')
        else:
            return datetime.datetime.now()

    @property
    def price(self): return float(self.__eventDict.get('ltp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime())

    # @property
    # def instrument(self): return f"{self.tradingSymbol}"

    def TradeBar(self, instrument):
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
                                "Instrument": instrument,
                                "Open Interest": self.openInterest,
                                "Date/Time": self.tickDateTime
                            })


class WebSocketClient:
    """
    This websocket client class is designed to be running in a separate thread and for that reason
    events are pushed into a queue.
    """

    class Event:
        DISCONNECTED = 1
        TRADE = 2
        ORDER_BOOK_UPDATE = 3

    def __init__(self, queue, api, tokenMappings):
        assert len(tokenMappings), "Missing subscriptions"
        self.__queue = queue
        self.__api = api
        self.__tokenIdMappings = {
            tokenMapping['instrument_token']: tokenMapping['instrument'] for tokenMapping in tokenMappings}
        self.__tokenMappings = [{'instrument_token': tokenMapping['instrument_token'],
                                 'exchange_segment': tokenMapping['exchange_segment']} for tokenMapping in tokenMappings]
        self.__pending_subscriptions = [
            tokenMapping['instrument_token'] for tokenMapping in tokenMappings]

        self.__connected = False
        self.__initialized = threading.Event()

    def startClient(self):
        self.__api.on_message = self.onQuoteUpdate
        self.__api.on_error = self.onError
        self.__api.on_close = self.onClosed
        self.__api.on_open = self.onOpened
        self.__api.subscribe(self.__tokenMappings)

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

    def onOpened(self):
        self.__connected = True

    def onClosed(self):
        if self.__connected:
            self.__connected = False

        logger.info("Websocket disconnected")
        # self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onError(self, exception):
        import traceback
        # Get the traceback information
        tb_info = traceback.format_exc()

        # Log the error along with traceback
        logger.error("Error: %s\n%s" % (exception, tb_info))

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onTrade(self, trade):
        if trade.getPrice() > 0:
            self.__queue.put((WebSocketClient.Event.TRADE, trade))

    def onQuoteUpdate(self, messages):
        try:
            if not self.__connected:
                self.__connected = True

            logger.debug(messages)
            for message in messages:
                subscribeEvent = SubscribeEvent(message)
                if subscribeEvent.scriptToken in self.__pending_subscriptions:
                    self.__onSubscriptionSucceeded(subscribeEvent)
                subscribeEvent.dateTime = datetime.datetime.now()
                self.onTrade(subscribeEvent.TradeBar(
                    self.__tokenIdMappings[subscribeEvent.scriptToken]))
        except Exception as e:
            logger.exception("Unhandled exception %s" % e)

    def __onSubscriptionSucceeded(self, event):
        logger.info(
            f"Subscription succeeded for <{self.__tokenIdMappings[event.scriptToken]}>")

        self.__pending_subscriptions.remove(
            f"{event.scriptToken}")

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
                logger.debug("Stopping websocket client from wsclient.stop()")
                self.__wsClient.stopClient()
        except Exception as e:
            logger.error(
                "Error stopping websocket client from wsclient.stop() exception catch: %s" % e)


class WebSocketClientThread(WebSocketClientThreadBase):
    """
    This thread class is responsible for running a WebSocketClient.
    """

    def __init__(self, api, tokenMappings):
        super(WebSocketClientThread, self).__init__(
            WebSocketClient, api, tokenMappings)
