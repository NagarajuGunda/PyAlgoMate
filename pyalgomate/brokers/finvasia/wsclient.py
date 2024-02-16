"""
.. moduleauthor:: Nagaraju Gunda
"""

import time
import threading
import logging
import datetime

from NorenRestApiPy.NorenApi import NorenApi

logger = logging.getLogger(__name__)

class WebSocketClient:
    def __init__(self, quotes, api, tokenMappings):
        assert len(tokenMappings), "Missing subscriptions"
        self.__quotes = quotes
        self.__api: NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__pending_subscriptions = list()
        self.__connected = False
        self.__connectionOpened = threading.Event()

    def startClient(self):
        self.__api.start_websocket(order_update_callback=self.onOrderBookUpdate,
                                   subscribe_callback=self.onQuoteUpdate,
                                   socket_open_callback=self.onOpened,
                                   socket_close_callback=self.onClosed,
                                   socket_error_callback=self.onError)

    def stopClient(self):
        try:
            if self.__connected:
                self.close()
        except Exception as e:
            logger.error("Failed to close connection: %s" % e)

    def waitInitialized(self, timeout):
        logger.info(f"Waiting for WebSocketClient waitInitialized with timeout of {timeout}")
        opened = self.__connectionOpened.wait(timeout)

        if opened:
            logger.info('Connection opened. Waiting for subscriptions to complete')
        else:
            logger.error(f'Connection not opened in {timeout} secs. Stopping the feed')
            return False

        for _ in range(timeout):
            if {pendingSubscription for pendingSubscription in self.__pending_subscriptions}.issubset(self.__quotes.keys()):
                self.__pending_subscriptions.clear()
                return True
            time.sleep(1)

        return False

    def isConnected(self):
        return self.__connected

    def onOpened(self):
        logger.info("Websocket connected")
        self.__connected = True
        self.__pending_subscriptions = list(self.__tokenMappings.keys())
        for channel in self.__pending_subscriptions:
            logger.info("Subscribing to channel %s." % channel)
            self.__api.subscribe(channel)
        self.__connectionOpened.set()

    def onClosed(self):
        if self.__connected:
            self.__connected = False

        logger.info("Websocket disconnected")

    def onError(self, exception):
        logger.error("Error: %s." % exception)

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onQuoteUpdate(self, message):
        key = message['e'] + '|' + message['tk']
        message['ct'] = datetime.datetime.now().replace(microsecond=0)

        if key in self.__quotes:
            symbolInfo =  self.__quotes[key]
            symbolInfo.update(message)
            self.__quotes[key] = symbolInfo
        else:
            self.__quotes[key] = message

    def onOrderBookUpdate(self, message):
        pass


class WebSocketClientThreadBase(threading.Thread):
    def __init__(self, wsCls, *args, **kwargs):
        super(WebSocketClientThreadBase, self).__init__()
        self.__quotes = dict()
        self.__wsClient = None
        self.__wsCls = wsCls
        self.__args = args
        self.__kwargs = kwargs

    def getQuotes(self):
        return self.__quotes

    def waitInitialized(self, timeout):
        return self.__wsClient is not None and self.__wsClient.waitInitialized(timeout)

    def run(self):
        # We create the WebSocketClient right in the thread, instead of doing so in the constructor,
        # because it has thread affinity.
        try:
            self.__wsClient = self.__wsCls(
                self.__quotes, *self.__args, **self.__kwargs)
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
