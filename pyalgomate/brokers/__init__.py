"""
.. moduleauthor:: Nagaraju Gunda
"""
import os
import datetime
import re
import pandas as pd
import logging

from pyalgotrade import broker
from pyalgotrade.broker import fillstrategy
from pyalgotrade.broker import backtesting
from pyalgomate.utils import UnderlyingIndex
import pyalgomate.utils as utils
from pyalgomate.backtesting.DataFrameFeed import DataFrameFeed

from pyalgomate.strategies import OptionContract

logger = logging.getLogger()

# In a backtesting or paper-trading scenario the BacktestingBroker dispatches events while processing events from the
# BarFeed.
# It is guaranteed to process BarFeed events before the strategy because it connects to BarFeed events before the
# strategy.

underlyingMapping = {
    'MIDCAPNIFTY': {
        'optionPrefix': 'MIDCAPNIFTY',
        'index': UnderlyingIndex.MIDCAPNIFTY,
        'lotSize': 75,
        'strikeDifference': 25
    },
    'BANKNIFTY': {
        'optionPrefix': 'BANKNIFTY',
        'index': UnderlyingIndex.BANKNIFTY,
        'lotSize': 15,
        'strikeDifference': 100
    },
    'NIFTY': {
        'optionPrefix': 'NIFTY',
        'index': UnderlyingIndex.NIFTY,
        'lotSize': 50,
        'strikeDifference': 50
    },
    'FINNIFTY': {
        'optionPrefix': 'FINNIFTY',
        'index': UnderlyingIndex.FINNIFTY,
        'lotSize': 40,
        'strikeDifference': 50
    }
}


def getUnderlyingMappings():
    return underlyingMapping


def getUnderlyingDetails(underlying):
    return underlyingMapping[underlying]


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    return f'{underlyingInstrument}{expiry.strftime("%d%b%y").upper()}{callOrPut.upper()}{strikePrice}'


class QuantityTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return round(quantity, 2)


class BacktestingBroker(backtesting.Broker):
    """A Finvasia backtesting broker.

    :param cash: The initial amount of cash.
    :type cash: int/float.
    :param barFeed: The bar feed that will provide the bars.
    :type barFeed: :class:`pyalgotrade.barfeed.BarFeed`
    :param fee: The fee percentage for each order. Defaults to 0.25%.
    :type fee: float.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
    """

    def getType(self):
        return "Backtest"

    def getUnderlyingMappings(self):
        return getUnderlyingMappings()

    def getUnderlyingDetails(self, underlying):
        return getUnderlyingDetails(underlying)

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        return getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return underlyingInstrument + str(ceStrikePrice) + "CE", underlyingInstrument + str(peStrikePrice) + "PE"

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is not None:
            day = int(m.group(2))
            month = m.group(3)
            year = int(m.group(4)) + 2000
            expiry = datetime.date(
                year, datetime.datetime.strptime(month, '%b').month, day)
            return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", m.group(1))

        m = re.match(r"([A-Z]+)(\d+)(CE|PE)", symbol)

        if m is None:
            return None

        return OptionContract(symbol, int(m.group(2)), None, "c" if m.group(3) == "CE" else "p", m.group(1))

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return pd.DataFrame(columns=['Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'])

    def __init__(self, cash, barFeed, fee=0.0025):
        commission = backtesting.TradePercentage(fee)
        super(BacktestingBroker, self).__init__(cash, barFeed, commission)
        self.setFillStrategy(fillstrategy.DefaultStrategy(volumeLimit=None))

    def getInstrumentTraits(self, instrument):
        return QuantityTraits()

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)
        return super(BacktestingBroker, self).submitOrder(order)

    def _remapAction(self, action):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
        }.get(action, None)
        if action is None:
            raise Exception("Only BUY/SELL orders are supported")
        return action

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
       action = self._remapAction(action)
       return super(BacktestingBroker, self).createMarketOrder(action, instrument, quantity, onClose)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        action = self._remapAction(action)

        # if action == broker.Order.Action.BUY:
        #     # Check that there is enough cash.
        #     fee = self.getCommission().calculate(None, limitPrice, quantity)
        #     cashRequired = limitPrice * quantity + fee
        #     if cashRequired > self.getCash(False):
        #         raise Exception("Not enough cash")
        # elif action == broker.Order.Action.SELL:
        #     # Check that there are enough coins.
        #     if quantity > self.getShares(instrument):
        #         raise Exception("Not enough %s" % (instrument))
        # else:
        #     raise Exception("Only BUY/SELL orders are supported")

        return super(BacktestingBroker, self).createLimitOrder(action, instrument, limitPrice, quantity)


def getFeed(creds, broker, registerOptions=['Weekly'], underlyings=['NSE|NIFTY BANK']):
    if broker == 'Backtest':
        data = pd.read_parquet('strategies/data/2023/banknifty/08.parquet')
        data = data.query("'2023-08-22' <= `Date/Time` <= '2023-08-25'")
        return DataFrameFeed(data, tickers=['BANKNIFTY']), None
    elif broker == 'Finvasia':
        from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker, getFinvasiaToken, getFinvasiaTokenMappings
        import pyalgomate.brokers.finvasia as finvasia
        from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
        import pyotp

        cred = creds[broker]

        api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                         websocket='wss://api.shoonya.com/NorenWSTP/')
        userToken = None
        tokenFile = 'shoonyakey.txt'
        if os.path.exists(tokenFile) and (datetime.datetime.fromtimestamp(os.path.getmtime(tokenFile)).date() == datetime.datetime.today().date()):
            logger.info(f"Token has been created today already. Re-using it")
            with open(tokenFile, 'r') as f:
                userToken = f.read()
            logger.info(
                f"userid {cred['user']} password ******** usertoken {userToken}")
            loginStatus = api.set_session(
                userid=cred['user'], password=cred['pwd'], usertoken=userToken)
        else:
            logger.info(f"Logging in and persisting user token")
            loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

            if loginStatus:
                with open(tokenFile, 'w') as f:
                    f.write(loginStatus.get('susertoken'))

                logger.info(
                    f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")
            else:
                logger.info(f'Login failed!')

        if loginStatus != None:
            if len(underlyings) == 0:
                underlyings = ['NSE|NIFTY BANK']

            optionSymbols = []

            for underlying in underlyings:
                exchange = underlying.split('|')[0]
                underlyingToken = getFinvasiaToken(api, underlying)
                logger.info(
                    f'Token id for <{underlying}> is <{underlyingToken}>')
                if underlyingToken is None:
                    logger.error(
                        f'Error getting token id for {underlyingToken}')
                    exit(1)
                underlyingQuotes = api.get_quotes(exchange, underlyingToken)
                ltp = underlyingQuotes['lp']

                try:
                    underlyingDetails = finvasia.broker.getUnderlyingDetails(
                        underlying)
                    index = underlyingDetails['index']
                    strikeDifference = underlyingDetails['strikeDifference']

                    currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                        datetime.datetime.now().date(), index)
                    nextWeekExpiry = utils.getNextWeeklyExpiryDate(
                        datetime.datetime.now().date(), index)
                    monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                        datetime.datetime.now().date(), index)

                    if "Weekly" in registerOptions:
                        optionSymbols += finvasia.broker.getOptionSymbols(
                            underlying, currentWeeklyExpiry, ltp, 20, strikeDifference)
                    if "NextWeekly" in registerOptions:
                        optionSymbols += finvasia.broker.getOptionSymbols(
                            underlying, nextWeekExpiry, ltp, 20, strikeDifference)
                    if "Monthly" in registerOptions:
                        optionSymbols += finvasia.broker.getOptionSymbols(
                            underlying, monthlyExpiry, ltp, 20, strikeDifference)
                except Exception as e:
                    logger.exception(f'Exception: {e}')

            optionSymbols = list(dict.fromkeys(optionSymbols))

            logger.info('Getting token mappings')
            tokenMappings = getFinvasiaTokenMappings(
                api, underlyings + optionSymbols)

            logger.info('Creating feed object')
            barFeed = LiveTradeFeed(api, tokenMappings)
        else:
            exit(1)
    elif broker == 'Zerodha':
        from pyalgomate.brokers.zerodha.kiteext import KiteExt
        import pyalgomate.brokers.zerodha as zerodha
        from pyalgomate.brokers.zerodha.broker import getZerodhaTokensList
        from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
        from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker

        cred = creds[broker]

        api = KiteExt()
        twoFA = pyotp.TOTP(cred['factor2']).now()
        api.login_with_credentials(
            userid=cred['user'], password=cred['pwd'], twofa=twoFA)

        profile = api.profile()
        logger.info(f"Welcome {profile.get('user_name')}")

        currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
            datetime.datetime.now().date())
        nextWeekExpiry = utils.getNextWeeklyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())

        if len(underlyings) == 0:
            underlyings = ['NSE:NIFTY BANK']

        optionSymbols = []

        for underlying in underlyings:
            ltp = api.quote(underlying)[
                underlying]["last_price"]

            if "Weekly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, currentWeeklyExpiry, ltp, 10)
            if "NextWeekly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, nextWeekExpiry, ltp, 10)
            if "Monthly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, monthlyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, underlyings + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)
    elif broker == 'Kotak':
        from neo_api_client import NeoAPI
        import pyalgomate.brokers.kotak as kotak
        from pyalgomate.brokers.kotak.broker import getTokenMappings
        from pyalgomate.brokers.kotak.feed import LiveTradeFeed

        cred = creds[broker]

        api = NeoAPI(consumer_key=cred['consumer_key'],
                     consumer_secret=cred['consumer_secret'], environment=cred['environment'])
        api.login(mobilenumber=cred['mobilenumber'], password=cred['Password'])
        ret = api.session_2fa(cred['mpin'])
        if ret == None:
            print('Exited due to biscut')
            exit(0)

        if len(underlyings) == 0:
            underlyings = ['BANKNIFTY']

        tokenMappings = []
        for underlying in underlyings:
            optionSymbols = []
            ret = api.search_scrip('NSE' if underlying !=
                                   'SENSEX' else 'BSE', underlying)
            script = [
                script for script in ret if script['pSymbolName'] == underlying][0]
            tokId = script['pSymbol']
            quotes = api.quotes([
                {'instrument_token': str(tokId), 'exchange_segment': 'nse_cm'}])
            ltp = float(quotes['message'][0]['last_traded_price'])

            underlyingDetails = kotak.broker.getUnderlyingDetails(underlying)
            index = underlyingDetails['index']
            strikeDifference = underlyingDetails['strikeDifference']

            currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date(), index)
            nextWeekExpiry = utils.getNextWeeklyExpiryDate(
                datetime.datetime.now().date(), index)
            monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date(), index)

            if "Weekly" in registerOptions:
                optionSymbols += kotak.broker.getOptionSymbols(
                    underlying, currentWeeklyExpiry, ltp, 10, strikeDifference)
            if "NextWeekly" in registerOptions:
                optionSymbols += kotak.broker.getOptionSymbols(
                    underlying, nextWeekExpiry, ltp, 10, strikeDifference)
            if "Monthly" in registerOptions:
                optionSymbols += kotak.broker.getOptionSymbols(
                    underlying, monthlyExpiry, ltp, 10, strikeDifference)

            optionSymbols = list(dict.fromkeys(optionSymbols))
            tokenMappings += getTokenMappings(api, underlying, optionSymbols)

        barFeed = LiveTradeFeed(api, tokenMappings)

    return barFeed, api


def getBroker(feed, api, broker, mode, capital=200000):
    if str(broker).lower() == 'backtest':
        from pyalgomate.brokers import BacktestingBroker
        brokerInstance = BacktestingBroker(capital, feed)
    elif str(broker).lower() == 'finvasia':
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker

        if str(mode).lower() == 'paper':
            brokerInstance = PaperTradingBroker(capital, feed)
        else:
            brokerInstance = LiveBroker(api)
    elif str(broker).lower() == 'zerodha':
        from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker

        if str(mode).lower() == 'paper':
            brokerInstance = ZerodhaPaperTradingBroker(capital, feed)
        else:
            brokerInstance = ZerodhaLiveBroker(api)
    elif broker == 'Kotak':
        from pyalgomate.brokers.kotak.broker import PaperTradingBroker, LiveBroker

        if mode == 'paper':
            brokerInstance = PaperTradingBroker(200000, feed)
        else:
            brokerInstance = LiveBroker(api)

    return brokerInstance
