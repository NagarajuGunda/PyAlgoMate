"""
.. moduleauthor:: Nagaraju Gunda
"""

import multiprocessing
import time
import threading
import logging
import datetime

from NorenRestApiPy.NorenApi import NorenApi

logger = logging.getLogger(__name__)

class WebSocketClient:
    def __init__(self, api, tokenMappings):
        assert len(tokenMappings), "Missing subscriptions"
        self.__quotes = multiprocessing.Manager().dict()
        self.__lastQuoteDateTime = multiprocessing.Manager().Value('d', None)
        self.__lastReceivedDateTime = multiprocessing.Manager().Value('d', None)
        self.__api: NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__pending_subscriptions = list()
        self.__connected = False
        self.__connectionOpened = threading.Event()

    def getQuotes(self):
        return self.__quotes

    def getLastQuoteDateTime(self):
        return self.__lastQuoteDateTime.value
    
    def getLastReceivedDateTime(self):
        return self.__lastReceivedDateTime.value

    def startClient(self):
        self.__api.start_websocket(order_update_callback=self.onOrderBookUpdate,
                                   subscribe_callback=self.onQuoteUpdate,
                                   socket_open_callback=self.onOpened,
                                   socket_close_callback=self.onClosed,
                                   socket_error_callback=self.onError)

    def stopClient(self):
        try:
            if self.__connected:
                self.__api.close_websocket()
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
        self.__lastReceivedDateTime.value = datetime.datetime.now()
        message['ct'] = self.__lastReceivedDateTime.value
        self.__lastQuoteDateTime.value = datetime.datetime.fromtimestamp(int(message['ft'])) if 'ft' in message else self.__lastReceivedDateTime.value.replace(microsecond=0)
        message['ft'] = self.__lastQuoteDateTime.value

        if key in self.__quotes:
            symbolInfo =  self.__quotes[key]
            symbolInfo.update(message)
            self.__quotes[key] = symbolInfo
        else:
            self.__quotes[key] = message

    def onOrderBookUpdate(self, message):
        pass
