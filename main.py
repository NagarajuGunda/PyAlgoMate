import yaml
import pyotp
import logging

from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
from pyalgomate.strategies.Strategy import Strategy

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.INFO)


def getTokenMappings(api, exchangeSymbols):
    tokenMappings = {}

    for exchangeSymbol in exchangeSymbols:
        splitStrings = exchangeSymbol.split('|')
        exchange = splitStrings[0]
        symbol = splitStrings[1]
        ret = api.searchscrip(exchange=exchange, searchtext=symbol)

        if ret != None:
            for value in ret['values']:
                if value['cname'] == symbol:
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
            api, ["NSE|NIFTY BANK", "NSE|NIFTY INDEX"]))
        broker = PaperTradingBroker(200000, barFeed)
        strat = Strategy(barFeed, broker)

    strat.run()


if __name__ == "__main__":
    main()
