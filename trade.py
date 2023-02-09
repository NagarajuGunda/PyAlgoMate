import yaml
import pyotp
import logging

from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
from pyalgomate.strategies.Strategy import Strategy
from pyalgomate.strategies.OptionsStrangleIntraday import OptionsStrangleIntraday

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.DEBUG)


def getTokenMappings(api, exchangeSymbols):
    tokenMappings = {}

    for exchangeSymbol in exchangeSymbols:
        splitStrings = exchangeSymbol.split('|')
        exchange = splitStrings[0]
        symbol = splitStrings[1]
        ret = api.searchscrip(exchange=exchange, searchtext=symbol)

        if ret != None:
            for value in ret['values']:
                if value['instname'] == 'OPTIDX' and value['tsym'] == symbol:
                    tokenMappings["{0}|{1}".format(
                        value['exch'], value['token'])] = exchangeSymbol
                    break
                if value['instname'] == 'UNDIND' and value['cname'] == symbol:
                    tokenMappings["{0}|{1}".format(
                        value['exch'], value['token'])] = exchangeSymbol
                    break

    return tokenMappings


def main():
    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

    ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

    if ret != None:
        barFeed = LiveTradeFeed(api, getTokenMappings(
            api, ["NSE|NIFTY INDEX", "NSE|NIFTY BANK", "NFO|NIFTY23FEB23C41600", "NFO|BANKNIFTY23FEB23P41600"]))
        broker = PaperTradingBroker(200000, barFeed)
        strat = Strategy(barFeed, broker, 'NSE|NIFTY BANK')

    strat.run()


if __name__ == "__main__":
    main()
