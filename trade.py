import yaml
import pyotp
import logging
import datetime
import zmq
import time
import random
import json
import os
from multiprocessing import Process

import pyalgotrade.bar
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker
import pyalgomate.brokers.finvasia as finvasia
from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker
import pyalgomate.brokers.zerodha as zerodha
from pyalgomate.strategies.OptionsStrangleIntraday import OptionsStrangleIntraday
from pyalgomate.strategies.OptionsStraddleIntraday import OptionsStraddleIntraday
from pyalgomate.strategies.OptionsTimeBasedStrategy import OptionsTimeBasedStrategy
from pyalgomate.strategies.DeltaNeutralIntraday import DeltaNeutralIntraday
from pyalgomate.strategies.StraddleIntradayWithVega import StraddleIntradayWithVega
import pyalgomate.utils as utils

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
from pyalgomate.brokers.zerodha.kiteext import KiteExt

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__file__)

# ZeroMQ Context
context = zmq.Context()


class Broker(object):
    FINVASIA = 1
    ZERODHA = 2


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


def getZerodhaTokensList(api: KiteExt, instruments):
    tokenMappings = {}
    response = api.ltp(instruments)
    for instrument in instruments:
        token = response[instrument]['instrument_token']
        tokenMappings[token] = instrument
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


def runStrategy(strategy):
    strategy.run()


def main():
    broker = Broker.FINVASIA

    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    if broker == Broker.FINVASIA:
        api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                         websocket='wss://api.shoonya.com/NorenWSTP/')

        userToken = None
        tokenFile = 'shoonyakey.txt'
        if os.path.exists(tokenFile) and (datetime.datetime.fromtimestamp(os.path.getmtime(tokenFile)).date() == datetime.datetime.today().date()):
            logger.info(f"Token has been created today already. Re-using it")
            with open(tokenFile, 'r') as f:
                userToken = f.read()
            logger.info(f"userid {cred['user']} password ******** usertoken {userToken}")
            loginStatus = api.set_session(userid=cred['user'], password=cred['pwd'],usertoken=userToken)
        else:
            print(f"Logging in and persisting user token")
            loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                        vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])
            
            with open(tokenFile,'w') as f:
                f.write(loginStatus.get('susertoken'))
            
            logger.info(f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")

        if loginStatus != None:
            underlyingInstrument = 'NSE|NIFTY BANK'

            ltp = api.get_quotes('NSE', getToken(
                api, underlyingInstrument))['lp']

            currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNextMonthlyExpiryDate(
                datetime.datetime.now().date()) if monthlyExpiry == currentWeeklyExpiry else monthlyExpiry

            optionSymbols = finvasia.broker.getOptionSymbols(
                underlyingInstrument, currentWeeklyExpiry, ltp, 10)
            # optionSymbols += finvasia.broker.getOptionSymbols(
            #     underlyingInstrument, monthlyExpiry, ltp, 10)

            optionSymbols = list(dict.fromkeys(optionSymbols))

            tokenMappings = getTokenMappings(
                api, ["NSE|NIFTY INDEX", underlyingInstrument] + optionSymbols)

            # Remove NFO| and replace index names
            # for key, value in tokenMappings.items():
            #     tokenMappings[key] = value.replace('NFO|', '').replace('NSE|NIFTY BANK', 'BANKNIFTY').replace(
            #         'NSE|NIFTY INDEX', 'NIFTY')

            barFeed = LiveTradeFeed(api, tokenMappings)
            # broker = PaperTradingBroker(200000, barFeed)
            broker = LiveBroker(api)
    elif broker == Broker.ZERODHA:
        api = KiteExt()
        twoFA = pyotp.TOTP(cred['factor2']).now()
        api.login_with_credentials(
            userid=cred['user'], password=cred['pwd'], twofa=twoFA)

        profile = api.profile()
        print(f"Welcome {profile.get('user_name')}")

        underlyingInstrument = 'NSE:NIFTY BANK'

        ltp = api.quote(underlyingInstrument)[
            underlyingInstrument]["last_price"]

        currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNextMonthlyExpiryDate(
            datetime.datetime.now().date()) if monthlyExpiry == currentWeeklyExpiry else monthlyExpiry

        optionSymbols = zerodha.broker.getOptionSymbols(
            underlyingInstrument, currentWeeklyExpiry, ltp, 10)
        # optionSymbols += zerodha.broker.getOptionSymbols(
        #     underlyingInstrument, monthlyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, [underlyingInstrument] + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)
        broker = ZerodhaLiveBroker(api)
    else:
        logger.error("Api returned None")
        return

    strategy = DeltaNeutralIntraday(feed=barFeed, broker=broker, registeredOptionsCount=len(
        optionSymbols), callback=valueChangedCallback, resampleFrequency=pyalgotrade.bar.Frequency.MINUTE, collectData=True)

    strategy.run()


if __name__ == "__main__":
    main()
