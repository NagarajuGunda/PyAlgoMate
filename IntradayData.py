import logging
import datetime
import pandas as pd
import yaml
import pyotp
import glob
import os

import pyalgotrade.bar
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
from pyalgomate.brokers.zerodha.kiteext import KiteExt

import pyalgomate.utils as utils

from pyalgotrade.strategy import BaseStrategy

from pyalgomate.backtesting import CustomCSVFeed
from pyalgomate.brokers.finvasia.broker import BacktestingBroker
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
import pyalgomate.brokers.finvasia as finvasia

logger = logging.getLogger(__file__)


class Broker(object):
    FINVASIA = 1
    ZERODHA = 2


def getToken(api, exchangeSymbol):
    splitStrings = exchangeSymbol.split('|')
    exchange = splitStrings[0]
    symbol = splitStrings[1]
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)

    if ret != None:
        for value in ret['values']:
            if value['instname'] == 'OPTIDX' and value['tsym'] == symbol:
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


def getZerodhaTokensList(api: KiteExt, instruments):
    tokenMappings = {}
    response = api.ltp(instruments)
    for instrument in instruments:
        token = response[instrument]['instrument_token']
        tokenMappings[token] = instrument
    return tokenMappings


class IntradayData(BaseStrategy):
    def __init__(self, feed, broker):
        super(IntradayData, self).__init__(feed, broker)
        self.resampleBarFeed(
            pyalgotrade.bar.Frequency.MINUTE, self.onResampledBars)

        self.columns = ["Ticker", "Date/Time", "Open", "High",
                        "Low", "Close", "Volume", "Open Interest"]
        self.fileName = "data.csv"

        df = pd.DataFrame(columns=self.columns)

        df.to_csv(self.fileName, index=False)

    def appendBars(self, bars, df):
        for ticker, bar in bars.items():
            newRow = {
                "Ticker": ticker,
                "Date/Time": bar.getDateTime(),
                "Open": bar.getOpen(),
                "High": bar.getHigh(),
                "Low": bar.getLow(),
                "Close": bar.getClose(),
                "Volume": bar.getVolume(),
                "Open Interest": bar.getExtraColumns().get("Open Interest", 0)
            }

            df = pd.concat([df, pd.DataFrame(
                [newRow], columns=self.columns)], ignore_index=True)

        return df

    def onResampledBars(self, bars):
        logger.info(f"On Resampled Bars - Date/Time - {bars.getDateTime()}")
        df = pd.DataFrame(columns=self.columns)
        df = self.appendBars(bars, df)
        df.to_csv(self.fileName, mode='a',
                  header=not os.path.exists(self.fileName), index=False)

    def onBars(self, bars):
        pass


def main():
    broker = Broker.ZERODHA

    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    if broker == Broker.FINVASIA:
        api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                         websocket='wss://api.shoonya.com/NorenWSTP/')

        twoFA = pyotp.TOTP(cred['factor2']).now()
        ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=twoFA,
                        vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

        if ret != None:
            underlyingInstrument = 'NSE|NIFTY BANK'

            ltp = api.get_quotes('NSE', getToken(
                api, underlyingInstrument))['lp']

            optionSymbols = finvasia.broker.getOptionSymbols(underlyingInstrument, utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date()), ltp, 10)
            optionSymbols += finvasia.broker.getOptionSymbols(underlyingInstrument, utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date()), ltp, 10)

            tokenMappings = getTokenMappings(
                api, ["NSE|NIFTY INDEX", underlyingInstrument] + optionSymbols)

            barFeed = LiveTradeFeed(api, tokenMappings)
            broker = PaperTradingBroker(200000, barFeed)
    elif broker == Broker.ZERODHA:
        api = KiteExt()
        twoFA = pyotp.TOTP(cred['factor2']).now()
        api.login_with_credentials(
            userid=cred['user'], password=cred['pwd'], twofa=twoFA)

        profile = api.profile()
        print(f"Welcome {profile.get('user_name')}")

        instruments = [
            "NSE:ABFRL",
            "NSE:ADANIENT",
            "NSE:ADANIPORTS",
            "NSE:AMARAJABAT",
            "NSE:ABB",
            "NSE:NIFTY 50"
        ]

        tokenMappings = getZerodhaTokensList(api, instruments)
        barFeed = ZerodhaLiveFeed(api, tokenMappings)
        broker = PaperTradingBroker(200000, barFeed)

    intradayData = IntradayData(barFeed, broker)

    intradayData.run()


def main_backtest():
    underlyingInstrument = 'BANKNIFTY'
    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    dataFiles = ["pyalgomate/backtesting/data/test.parquet"]
    for files in dataFiles:
        for file in glob.glob(files):
            feed.addBarsFromParquet(path=file, ticker=underlyingInstrument)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")

    broker = BacktestingBroker(200000, feed)
    intradayData = IntradayData(feed, broker)

    intradayData.run()


if __name__ == "__main__":
    main()
