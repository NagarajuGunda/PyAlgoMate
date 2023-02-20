import yaml
import pyotp
import logging
import datetime
import zmq
import time
import random
import json

import pyalgotrade.bar
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
import pyalgomate.brokers.finvasia as finvasia
from pyalgomate.strategies.OptionsStrangleIntraday import OptionsStrangleIntraday
import pyalgomate.utils as utils

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__file__)

# ZeroMQ Context
context = zmq.Context()

# Define the socket using the "Context"
sock = context.socket(zmq.PUB)
sock.bind("tcp://127.0.0.1:5680")


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


def valueChangedCallback(strategy, value):
    jsonDump = json.dumps({strategy: value})
    logger.debug(jsonDump)
    sock.send_json(jsonDump)

def fakeSend():
    while True:
        jsonData = {
            "datetime": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "metrics": {
                "pnl": random.randint(-1000, 2000),
                "cePnl": random.randint(-1000, 2000),
                "pePnl": random.randint(-1000, 2000),
                "ceSL": random.randint(200, 1000),
                "peSL": random.randint(200, 1000),
                "ceTarget": random.randint(0, 100),
                "peTarget": random.randint(0, 100),
                "ceEnteredPrice": random.randint(300, 600),
                "peEnteredPrice": random.randint(300, 600),
                "ceLTP": random.randint(300, 600),
                "peLTP": random.randint(300, 600)
            },
            "charts": {
                "pnl": random.randint(-1000, 2000),
                "cePnl": random.randint(-1000, 2000),
                "pePnl": random.randint(-1000, 2000),
                "ceSL": random.randint(200, 1000),
                "peSL": random.randint(200, 1000),
                "ceTarget": random.randint(0, 100),
                "peTarget": random.randint(0, 100),
                "combinedPremium": random.randint(1000, 1200)
            }
        }
        sock.send_json(json.dumps({"Dummy": jsonData}))
        time.sleep(2)

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

        strat = OptionsStrangleIntraday(
            barFeed, broker, underlyingInstrument, valueChangedCallback, pyalgotrade.bar.Frequency.MINUTE)

    strat.run()


if __name__ == "__main__":
    main()
