import yaml
import pyotp
import logging
import datetime

from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
import pyalgomate.brokers.finvasia as finvasia
from pyalgomate.strategies.OptionsStrangleIntraday import OptionsStrangleIntraday
import pyalgomate.utils as utils

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.DEBUG)


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


def main():
    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

    ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

    if ret != None:
        underlyingInstrument = 'NSE|NIFTY BANK'

        ltp = api.get_quotes('NSE', getToken(api, underlyingInstrument))['lp']

        optionSymbols = finvasia.broker.getOptionSymbols(underlyingInstrument, utils.getNearestWeeklyExpiryDate(
            datetime.datetime.now().date()), ltp, 5)

        barFeed = LiveTradeFeed(api, getTokenMappings(
            api, ["NSE|NIFTY INDEX", underlyingInstrument] + optionSymbols))
        broker = PaperTradingBroker(200000, barFeed)

        strat = OptionsStrangleIntraday(barFeed, broker, underlyingInstrument)

    strat.run()


if __name__ == "__main__":
    main()
