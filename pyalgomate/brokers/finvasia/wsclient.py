"""
.. moduleauthor:: Nagaraju Gunda
"""


import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))), os.pardir))

from pyalgotrade import bar
from NorenRestApiPy.NorenApi import NorenApi
import yaml
import zmq
import datetime
import logging
import threading
import time
from pyalgomate.barfeed.BasicBarEx import BasicBarEx
import pyalgomate.brokers.finvasia as finvasia

logger = logging.getLogger(__name__)


class WebSocketClient:
    def __init__(self, api, tokenMappings, zmq_ipc_path="/tmp/zmq_websocket.ipc"):
        assert len(tokenMappings), "Missing subscriptions"
        self.__quotes = dict()
        self.__lastQuoteDateTime = None
        self.__lastReceivedDateTime = None
        self.__api: NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__pendingSubscriptions = list()
        self.__connected = False
        self.__connectionOpened = threading.Event()

        # Set up ZeroMQ publisher
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PUB)
        self.__socket.bind(f"ipc://{zmq_ipc_path}")

        self.periodicThread = threading.Thread(target=self.periodicPrint)
        self.periodicThread.daemon = True
        self.periodicThread.start()

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
                self.__socket.close()
                self.__context.term()
        except Exception as e:
            logger.error("Failed to close connection: %s" % e)

    def waitInitialized(self, timeout=10):
        logger.info(
            f"Waiting for WebSocketClient waitInitialized with timeout of {timeout}")
        opened = self.__connectionOpened.wait(timeout)

        if opened:
            logger.info(
                'Connection opened. Waiting for subscriptions to complete')
        else:
            logger.error(
                f'Connection not opened in {timeout} secs. Stopping the feed')
            return False

        for _ in range(timeout):
            if {pendingSubscription for pendingSubscription in self.__pendingSubscriptions}.issubset(self.__quotes.keys()):
                self.__pendingSubscriptions.clear()
                return True
            time.sleep(1)

        return False

    def onOpened(self):
        logger.info("Websocket connected")
        self.__connected = True
        self.__pendingSubscriptions = list(self.__tokenMappings.keys())
        for channel in self.__pendingSubscriptions:
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
        self.__lastReceivedDateTime = datetime.datetime.now()
        message['ct'] = self.__lastReceivedDateTime
        self.__lastQuoteDateTime = datetime.datetime.fromtimestamp(int(
            message['ft'])) if 'ft' in message else self.__lastReceivedDateTime.replace(microsecond=0)
        message['ft'] = self.__lastQuoteDateTime

        if key in self.__quotes:
            symbolInfo = self.__quotes[key]
            symbolInfo.update(message)
            self.__quotes[key] = symbolInfo
        else:
            self.__quotes[key] = message

        self.__socket.send_pyobj(message)

    def onOrderBookUpdate(self, message):
        pass

    def periodicPrint(self):
        while True:
            logger.info(
                f'Last Quote: {self.__lastQuoteDateTime}\tLast Received: {self.__lastReceivedDateTime}')
            time.sleep(60)


if __name__ == "__main__":
    import pyalgomate.brokers.finvasia as finvasia

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(levelname)-5s]|[%(asctime)s]|[PID:%(process)d::TID:%(thread)d]|[%(name)s::%(module)s::%(funcName)s::%("
        "lineno)d]|=> %(message)s"
    )

    fileHandler = logging.FileHandler('Feed.log', 'a', 'utf-8')
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(formatter)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)

    logging.getLogger("requests").setLevel(logging.WARNING)

    creds = None
    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    with open("strategies.yaml", "r") as file:
        config = yaml.safe_load(file)

    broker = config['Broker']
    api, tokenMappings = None, None

    if broker == 'Finvasia':
        api, tokenMappings = finvasia.getApiAndTokenMappings(
            creds[broker], registerOptions=['Weekly'], underlyings=config['Underlyings'])
    else:
        exit(1)

    wsClient = WebSocketClient(api, tokenMappings)
    wsClient.startClient()
    if not wsClient.waitInitialized():
        exit(1)
    else:
        logger.info('Initialization complete!')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.stopClient()
