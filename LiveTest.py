import logging

import pyalgotrade.bar
import pyotp
import yaml
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
from pyalgomate.core.strategy import BaseStrategy
from pyalgomate.strategy.position import Position

from pyalgomate.brokers.finvasia.broker import (
    LiveBroker,
)
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)


def getToken(api, exchangeSymbol):
    splitStrings = exchangeSymbol.split("|")
    exchange = splitStrings[0]
    symbol = splitStrings[1]
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)

    if ret is not None:
        for value in ret["values"]:
            if value["instname"] in ["OPTIDX", "EQ"] and value["tsym"] == symbol:
                return value["token"]
            if value["instname"] == "UNDIND" and value["cname"] == symbol:
                return value["token"]

    return None


def getTokenMappings(api, exchangeSymbols):
    tokenMappings = {}

    for exchangeSymbol in exchangeSymbols:
        tokenMappings[exchangeSymbol] = "{0}|{1}".format(
            exchangeSymbol.split("|")[0], getToken(api, exchangeSymbol)
        )

    return tokenMappings


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class IntradayData(BaseStrategy):
    def __init__(self, feed, broker):
        super(IntradayData, self).__init__(feed, broker)
        self.state = State.LIVE
        self.position: Position = None

    def onEnterOk(self, position: Position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        action = "Buy" if position.getEntryOrder().isBuy() else "Sell"
        logger.info(
            f"{execInfo.getDateTime()} ===== {action} Position opened: {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> ====="
        )

    def onExitOk(self, position: Position):
        execInfo = position.getExitOrder().getExecutionInfo()
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> ====="
        )
        self.position = None

    def onEnterCanceled(self, position: Position):
        logger.info(f"===== Entry Position cancelled: {position.getInstrument()} =====")
        self.position = None

    def onExitCanceled(self, position: Position):
        logger.info(f"===== Exit Position canceled: {position.getInstrument()} =====")
        self.position = None

    def onBars(self, bars):
        pass

    def onIdle(self):
        super().onIdle()
        if self.state == State.LIVE and len(self.getActivePositions()) == 0:
            self.state = State.PLACING_ORDERS
            logger.info("Initiating trade")
            self.position = strategy.enterShort("NSE|YESBANK-EQ", 1)
        elif self.state == State.PLACING_ORDERS:
            if len(self.getActivePositions()):
                self.state = State.ENTERED
            else:
                self.state = State.LIVE
        elif self.state == State.ENTERED:
            self.position.modifyExitToLimit(1, 1)


def main():
    with open("cred.yml") as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    api = ShoonyaApi(
        host="https://api.shoonya.com/NorenWClientTP/",
        websocket="wss://api.shoonya.com/NorenWSTP/",
    )

    broker = "Finvasia"
    cred = creds[broker]
    twoFA = pyotp.TOTP(cred["factor2"]).now()
    ret = api.login(
        userid=cred["user"],
        password=cred["pwd"],
        twoFA=twoFA,
        vendor_code=cred["vc"],
        api_secret=cred["apikey"],
        imei=cred["imei"],
    )

    if ret is not None:
        # feed = CustomCSVFeed.CustomCSVFeed()
        # feed.addBarsFromParquets(
        #    dataFiles=["pyalgomate/backtesting/data/test.parquet"], ticker='BANKNIFTY')
        instruments = ["NSE|YESBANK-EQ"]
        tokenMappings = getTokenMappings(api, instruments)

        # Remove NFO| and replace index names
        # for key, value in tokenMappings.items():
        #     tokenMappings[key] = value.replace('NFO|', '').replace('NSE|NIFTY BANK', 'BANKNIFTY').replace(
        #         'NSE|NIFTY INDEX', 'NIFTY')

        feed = LiveTradeFeed(api, tokenMappings, instruments)
        broker = LiveBroker(api, feed)
        intradayData = IntradayData(feed, broker)

        return intradayData


if __name__ == "__main__":
    strategy = main()
    strategy.run()
