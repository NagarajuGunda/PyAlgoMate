import logging
import datetime
import pandas as pd
import yaml
import pyotp
import glob
import os
import time

import pyalgotrade.bar
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

import pyalgomate.utils as utils

from pyalgotrade.strategy import BaseStrategy

from pyalgomate.backtesting import CustomCSVFeed
from pyalgomate.brokers.finvasia.broker import BacktestingBroker
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker
import pyalgomate.brokers.finvasia as finvasia

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)


def getToken(api, exchangeSymbol):
    splitStrings = exchangeSymbol.split('|')
    exchange = splitStrings[0]
    symbol = splitStrings[1]
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)

    if ret != None:
        for value in ret['values']:
            if value['instname'] in ['OPTIDX', 'EQ'] and value['tsym'] == symbol:
                return value['token']
            if value['instname'] == 'UNDIND' and value['cname'] == symbol:
                return value['token']

    return None


def getTokenMappings(api, exchangeSymbols):
    tokenMappings = {}

    for exchangeSymbol in exchangeSymbols:
        tokenMappings["{0}|{1}".format(exchangeSymbol.split(
            '|')[0], getToken(api, exchangeSymbol))] = exchangeSymbol

    return tokenMappings


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class IntradayData(BaseStrategy):
    def __init__(self, feed, broker):
        super(IntradayData, self).__init__(feed, broker)
        self.resampleBarFeed(
            5 * pyalgotrade.bar.Frequency.MINUTE, self.onResampledBars)
        self.state = State.LIVE
        self.openPositions = {}

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        action = "Buy" if position.getEntryOrder().isBuy() else "Sell"
        self.openPositions[position.getInstrument()] = position.getEntryOrder()
        logger.info(f"{execInfo.getDateTime()} ===== {action} Position opened: {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        entryOrder = self.openPositions.pop(position.getInstrument())
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

    def onEnterCanceled(self, position):
        logger.info(
            f"===== Entry Position cancelled: {position.getInstrument()} =====")

    def onExitCanceled(self, position):
        logger.info(
            f"===== Exit Position canceled: {position.getInstrument()} =====")

    def onResampledBars(self, bars):
        if len(self.getActivePositions()):
            self.state = State.PLACING_ORDERS
            logger.info('Exiting positions')
            for position in self.getActivePositions().copy():
                if position.getEntryOrder().isFilled():
                    position.exitMarket()

    def onBars(self, bars):
        if self.state == State.LIVE and len(self.getActivePositions()) == 0:
            self.state = State.PLACING_ORDERS
            logger.info('Initiating trade')
            price = self.getFeed().getDataSeries(
                'NSE|YESBANK-EQ')[-1].getClose()
            strategy.enterLongLimit(
                'NSE|YESBANK-EQ', price, 50)
        elif self.state == State.PLACING_ORDERS:
            if len(self.getActivePositions()):
                self.state = State.LIVE


def main():
    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

    twoFA = pyotp.TOTP(cred['factor2']).now()
    ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=twoFA,
                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

    if ret != None:
        # feed = CustomCSVFeed.CustomCSVFeed()
        # feed.addBarsFromParquets(
        #    dataFiles=["pyalgomate/backtesting/data/test.parquet"], ticker='BANKNIFTY')
        tokenMappings = getTokenMappings(
            api, ["NSE|YESBANK-EQ"])

        # Remove NFO| and replace index names
        # for key, value in tokenMappings.items():
        #     tokenMappings[key] = value.replace('NFO|', '').replace('NSE|NIFTY BANK', 'BANKNIFTY').replace(
        #         'NSE|NIFTY INDEX', 'NIFTY')

        feed = LiveTradeFeed(api, tokenMappings)
        broker = LiveBroker(api)
        intradayData = IntradayData(feed, broker)

        return intradayData


if __name__ == "__main__":
    strategy = main()
    strategy.run()
