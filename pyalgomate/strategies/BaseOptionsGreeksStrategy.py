import logging
import numpy as np
import datetime
import pandas as pd
import os
import time

import pyalgotrade.bar
from pyalgotrade.broker import Order, OrderExecutionInfo
from pyalgotrade.strategy import position
from pyalgotrade import strategy
from pyalgotrade import broker
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
import pyalgomate.utils as utils
from pyalgomate.strategies import OptionGreeks
from pyalgomate.strategy.position import LongOpenPosition, ShortOpenPosition
from py_vollib_vectorized import vectorized_implied_volatility, get_all_greeks
from pyalgotrade.barfeed import csvfeed
from pyalgomate.telegram import TelegramBot

class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4

    @classmethod
    def toString(cls, state):
        if state == cls.LIVE:
            return "LIVE"
        elif state == cls.PLACING_ORDERS:
            return "PLACING_ORDERS"
        elif state == cls.ENTERED:
            return "ENTERED"
        elif state == cls.EXITED:
            return "EXITED"
        else:
            raise "Invalid State"


class Expiry(object):
    WEEKLY = 1
    MONTHLY = 2


class BaseOptionsGreeksStrategy(strategy.BaseStrategy):

    def __init__(self, feed, broker, strategyName, logger: logging.Logger, callback=None, resampleFrequency=None, collectData=None,
                 telegramBot:TelegramBot=None):
        super(BaseOptionsGreeksStrategy, self).__init__(feed, broker)
        self.marketEndTime = datetime.time(hour=15, minute=30)
        self.strategyName = strategyName
        self.logger = logger
        self.collectData = collectData
        self.telegramBot = telegramBot
        self._observers = []
        self.__optionContracts = dict()
        self.reset()

        # build option contracts
        self.buildOptionContracts()

        if callback:
            self._observers.append(callback)
            
        self.resampleBarFeed(pyalgotrade.bar.Frequency.MINUTE, self.on1MinBars)

        if not os.path.exists("results"):
            os.mkdir("results")

        if isinstance(self.getFeed(), csvfeed.BarFeed):
            self.tradesCSV = f"results/{self.strategyName}_backtest.csv"
        else:
            self.tradesCSV = f"results/{self.strategyName}_trades.csv"

        if os.path.isfile(self.tradesCSV):
            self.tradesDf = pd.read_csv(self.tradesCSV, index_col=False)
        else:
            self.tradesDf = pd.DataFrame(columns=['Entry Date/Time', 'Entry Order Id', 'Exit Date/Time', 'Exit Order Id',
                                                  'Instrument', 'Buy/Sell', 'Quantity', 'Entry Price', 'Exit Price', 'PnL', 'Date'])

        self.dataColumns = ["Ticker", "Date/Time", "Open", "High",
                                "Low", "Close", "Volume", "Open Interest"]
        if self.collectData is not None:
            self.dataFileName = "data.csv"

            if not os.path.isfile(self.dataFileName):
                pd.DataFrame(columns=self.dataColumns).to_csv(
                    self.dataFileName, index=False)

    def buildOrdersFromActiveOrders(self):
        if not isinstance(self.getBroker(), BacktestingBroker):
            today = datetime.date.today()

            mask = (self.tradesDf['Exit Order Id'].isnull()) & \
                (pd.to_datetime(
                    self.tradesDf['Entry Date/Time'], format='%Y-%m-%d %H:%M:%S').dt.date == today)
            openPositions = self.tradesDf.loc[mask]

            for index, openPosition in openPositions.iterrows():
                order = broker.MarketOrder(broker.Order.Action.BUY if openPosition['Buy/Sell'] == 'BUY' else broker.Order.Action.SELL,
                                           openPosition['Instrument'],
                                           float(openPosition['Quantity']),
                                           False,
                                           QuantityTraits())

                entryDateTime = datetime.datetime.strptime(
                    openPosition['Entry Date/Time'], '%Y-%m-%d %H:%M:%S')
                order.setSubmitted(
                    openPosition['Entry Order Id'], entryDateTime)
                self.getBroker()._registerOrder(order)
                order.switchState(broker.Order.State.SUBMITTED)

                position = LongOpenPosition(self, order) if order.isBuy(
                ) else ShortOpenPosition(self, order)

                order.switchState(broker.Order.State.ACCEPTED)

                fee = 0
                orderExecutionInfo = broker.OrderExecutionInfo(
                    openPosition['Entry Price'], openPosition['Quantity'], fee, entryDateTime)
                order.addExecutionInfo(orderExecutionInfo)
                if not order.isActive():
                    self.getBroker()._unregisterOrder(order)
                self.getBroker().notifyOrderEvent(broker.OrderEvent(
                    order, broker.OrderEvent.Type.FILLED, orderExecutionInfo))

            if len(openPositions) > 0:
                # Sleep so that the order notifications are acknowledged
                time.sleep(2)

    def reset(self):
        self.__optionData = dict()
        self.openPositions = set()
        self.closedPositions = set()
        self.overallPnL = 0
        self.state = State.LIVE

    def getNewRows(self, bars):
        newRows = []
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

            newRows.append(newRow)

        return newRows

    def on1MinBars(self, bars):
        self.log(f"On Resampled Bars - Date/Time - {bars.getDateTime()}", logging.DEBUG)

        jsonData = {
            "datetime": bars.getDateTime().strftime('%Y-%m-%d %H:%M:%S'),
            "metrics": {
                "pnl": self.overallPnL
            },
            "charts": {
                "pnl": self.overallPnL
            },
            "state": State.toString(self.state)
        }

        if self.collectData is not None:
            df = pd.DataFrame(self.getNewRows(bars), columns=self.dataColumns)
            df.to_csv(self.dataFileName, mode='a',
                      header=not os.path.exists(self.dataFileName), index=False)

            dataDf = df.copy()
            dataDf['Date/Time'] = dataDf['Date/Time'].dt.strftime(
                '%Y-%m-%d %H:%M:%S')

            jsonData["ohlc"] = dataDf.to_json()

        if self.state != State.LIVE:
            combinedPremium = 0
            for openPosition in self.openPositions.copy():
                instrument = openPosition.getInstrument()
                ltp = self.getLTP(instrument)
                jsonData["metrics"][f"{instrument} PnL"] = jsonData["charts"][f"{instrument} PnL"] = self.getPnL(
                    openPosition)
                jsonData["metrics"][f"{instrument} LTP"] = jsonData["charts"][f"{instrument} LTP"] = ltp
                combinedPremium += ltp

            jsonData["metrics"]["Combined Premium"] = combinedPremium
            jsonData["trades"] = self.tradesDf.to_json()

        jsonData["optionChain"] = dict()
        for instrument, optionGreek in self.__optionData.items():
            optionGreekDict = dict([attr, getattr(optionGreek, attr)]
                                    for attr in dir(optionGreek) if not attr.startswith('_'))
            optionContract = optionGreekDict.pop('optionContract')
            optionContractDict = dict([attr, getattr(optionContract, attr)] for attr in dir(
                optionContract) if not attr.startswith('_'))
            optionContractDict['expiry'] = optionContractDict['expiry'].strftime(
                '%Y-%m-%d')
            optionGreekDict.update(optionContractDict)
            jsonData["optionChain"][instrument] = optionGreekDict

        for callback in self._observers:
            callback(self.strategyName, jsonData)

    def log(self, message, level=logging.INFO):
        if level == logging.DEBUG:
            self.logger.debug(f"{self.strategyName} {self.getCurrentDateTime()} {message}")
        else:
            self.logger.info(f"{self.strategyName} {self.getCurrentDateTime()} {message}")
            if self.telegramBot:
                self.telegramBot.sendMessage(f"{self.strategyName} {self.getCurrentDateTime()} {message}")

    def getPnL(self, position: position):
        order = position.getEntryOrder()
        if order is None or not self.haveLTP(order.getInstrument()):
            return 0

        entryPrice = order.getAvgFillPrice()
        exitPrice = self.getFeed().getDataSeries(
            order.getInstrument())[-1].getClose()

        if order.isBuy():
            return (exitPrice - entryPrice) * order.getExecutionInfo().getQuantity()
        else:
            return (entryPrice - exitPrice) * order.getExecutionInfo().getQuantity()

    def getOverallPnL(self):
        pnl = 0
        for openPosition in self.openPositions.copy():
            pnl += self.getPnL(openPosition)

        for closedPosition in self.closedPositions.copy():
            entryOrder = closedPosition.getEntryOrder()
            exitOrder = closedPosition.getExitOrder()
            entryPrice = entryOrder.getAvgFillPrice()
            exitPrice = exitOrder.getAvgFillPrice()

            if entryOrder.isBuy():
                pnl += (exitPrice * exitOrder.getQuantity()) - \
                    (entryPrice * entryOrder.getQuantity())
            else:
                pnl += (entryPrice * entryOrder.getQuantity()) - \
                    (exitPrice * exitOrder.getQuantity())
        return pnl

    def onStart(self):
        # build open orders from tradeDf
        self.buildOrdersFromActiveOrders()

    def onEnterOk(self, position: position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        action = "Buy" if position.getEntryOrder().isBuy() else "Sell"
        self.log(f"===== {action} Position opened: {position.getEntryOrder().getInstrument()} at <{execInfo.getDateTime()}> with price <{execInfo.getPrice()}> and quantity <{execInfo.getQuantity()}> =====")

        self.openPositions.add(position)

        # Check if there is an order id already present in trade df for the same instrument
        if self.tradesDf[(self.tradesDf['Entry Order Id'] == position.getEntryOrder().getId())
                         & (self.tradesDf['Instrument'] == position.getInstrument())
                         ].shape[0] == 0:
            # Append a new row to the tradesDf DataFrame with the trade information
            newRow = {'Entry Date/Time': execInfo.getDateTime().strftime('%Y-%m-%d %H:%M:%S'),
                    'Entry Order Id': position.getEntryOrder().getId(),
                    'Exit Date/Time': None,
                    'Exit Order Id': None,
                    'Instrument': position.getInstrument(),
                    'Buy/Sell': "Buy" if position.getEntryOrder().isBuy() else "Sell",
                    'Quantity': execInfo.getQuantity(),
                    'Entry Price': position.getEntryOrder().getAvgFillPrice(),
                    'Exit Price': None,
                    'PnL': None,
                    'Date': None}
            self.tradesDf = pd.concat([self.tradesDf, pd.DataFrame(
                [newRow], columns=self.tradesDf.columns)], ignore_index=True)

            self.tradesDf.to_csv(self.tradesCSV, index=False)

        if self.__optionData.get(position.getInstrument(), None) is not None:
            self.log(
                f"Option greeks for {position.getInstrument()}\n{self.__optionData[position.getInstrument()]}", logging.DEBUG)

    def getOpenPosition(self, id: int) -> position:
        for position in self.openPositions.copy():
            if position.getEntryOrder().getId() == id:
                return position

        return None

    def isPendingOrdersCompleted(self):
        for position in list(self.getActivePositions()):
            if position.getEntryOrder() is None or self.getOpenPosition(position.getEntryOrder().getId()) is None:
                return False

        return True

    def onExitOk(self, position: position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.log(
            f"===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getDateTime()} with price <{execInfo.getPrice()}> and quantity <{execInfo.getQuantity()}> =====")

        openPosition = self.getOpenPosition(position.getEntryOrder().getId())
        if openPosition is None:
            self.log(
                f"{execInfo.getDateTime()} - {position.getInstrument()} not found in open positions.")
            return

        self.openPositions.remove(openPosition)
        self.closedPositions.add(openPosition)

        entryOrder = openPosition.getEntryOrder()

        # Update the corresponding row in the tradesDf DataFrame with the exit information
        entryPrice = entryOrder.getAvgFillPrice()
        exitPrice = position.getExitOrder().getAvgFillPrice()
        exitOrderId = position.getExitOrder().getId()
        pnl = ((exitPrice - entryPrice) * entryOrder.getExecutionInfo().getQuantity()
               ) if entryOrder.isBuy() else ((entryPrice - exitPrice) * entryOrder.getExecutionInfo().getQuantity())

        idx = self.tradesDf.loc[(self.tradesDf['Instrument']
                                == position.getInstrument()) & (self.tradesDf['Entry Order Id']
                                == position.getEntryOrder().getId())].index[-1]
        self.tradesDf.loc[idx, ['Exit Date/Time', 'Exit Order Id', 'Exit Price', 'PnL', 'Date']] = [
            execInfo.getDateTime().strftime('%Y-%m-%d %H:%M:%S'), exitOrderId, exitPrice, pnl, execInfo.getDateTime().strftime('%Y-%m-%d')]
        self.tradesDf.to_csv(self.tradesCSV, index=False)

        self.log(
            f"Option greeks for {position.getInstrument()}\n{self.__optionData.get(position.getInstrument(), None) if self.__optionData is not None else None}", logging.DEBUG)

    def onEnterCanceled(self, position: position):
        self.log(f"===== Entry order cancelled: {position.getEntryOrder().getInstrument()} =====", logging.WARN)

    def onExitCanceled(self, position: position):
        self.log(f"===== Exit order cancelled: {position.getExitOrder().getInstrument()} =====", logging.WARN)

    def haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def getLTP(self, instrument):
        if self.haveLTP(instrument):
            return self.getFeed().getDataSeries(instrument)[-1].getClose()
        return 0

    def getNearestDeltaOption(self, optionType, deltaValue, expiry):
        options = [opt for opt in self.__optionData.values(
        ) if opt.optionContract.type == optionType and opt.optionContract.expiry == expiry]
        options.sort(key=lambda x: abs(
            x.delta + abs(deltaValue) if optionType == 'p' else x.delta - abs(deltaValue)))
        return options[0] if len(options) > 0 else None

    def getNearestPremiumOption(self, optionType, premium, expiry):
        options = [opt for opt in self.__optionData.values(
        ) if opt.optionContract.type == optionType and opt.optionContract.expiry == expiry]
        options.sort(key=lambda x: abs(x.price - premium))
        return options[0] if len(options) > 0 else None

    def getOTMStrikeGreeks(self, strike: int, optionType: str, expiry: datetime.date, numberOfOptions: int = -1) -> list:
        options = [opt for opt in self.__optionData.values(
        ) if (opt.optionContract.type == optionType) and (opt.optionContract.expiry == expiry) and (opt.optionContract.strike > strike if optionType == 'c' else opt.optionContract.strike < strike)]
        options.sort(key=lambda x: x.optionContract.strike,
                     reverse=True if optionType == 'p' else False)
        return options[:numberOfOptions]

    def getITMStrikeGreeks(self, strike: int, optionType: str, expiry: datetime.date) -> list:
        options = [opt for opt in self.__optionData.values(
        ) if (opt.optionContract.type == optionType) and (opt.optionContract.expiry == expiry) and (opt.optionContract.strike < strike if optionType == 'c' else opt.optionContract.strike > strike)]
        options.sort(key=lambda x: x.optionContract.strike,
                     reverse=True if optionType == 'c' else False)
        return options


    def getOverallDelta(self):
        delta = 0
        for openPosition in self.openPositions.copy():
            delta += self.__optionData[openPosition.getInstrument()].delta if self.__optionData.get(
                openPosition.getInstrument(), None) is not None else 0

        return delta

    def getUnderlyingPrice(self, underlyingInstrument):
        if not (underlyingInstrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(underlyingInstrument)) > 0):
            return None
        return self.getFeed().getDataSeries(underlyingInstrument)[-1].getClose()

    def __calculateGreeks(self, bars):
        # Collect all the necessary data into NumPy arrays
        optionContracts = []
        underlyingPrices = []
        strikes = []
        prices = []
        expiries = []
        types = []
        ois = []
        for instrument, bar in bars.items():
            optionContract = self.getBroker().getOptionContract(instrument)

            if optionContract is not None:
                underlyingPrice = self.getUnderlyingPrice(
                    optionContract.underlying)
                if underlyingPrice is None:
                        return
                underlyingPrices.append(underlyingPrice)
                optionContracts.append(optionContract)
                strikes.append(optionContract.strike)
                prices.append(bar.getClose())
                ois.append(bar.getExtraColumns().get("oi", 0))
                if optionContract.expiry is None:
                    expiry = utils.getNearestWeeklyExpiryDate(
                        bar.getDateTime().date())
                else:
                    expiry = optionContract.expiry
                expiries.append(
                    ((expiry - bar.getDateTime().date()).days + 1) / 365.0)
                types.append(optionContract.type)
        underlyingPrices = np.array(underlyingPrices)
        strikes = np.array(strikes)
        prices = np.array(prices)
        expiries = np.array(expiries)
        types = np.array(types)

        try:
            # Calculate implied volatilities
            iv = vectorized_implied_volatility(prices, underlyingPrices, strikes, expiries, 0.0,
                                               types, q=0, model='black_scholes_merton', return_as='numpy', on_error='ignore')

            # Calculate greeks
            greeks = get_all_greeks(types, underlyingPrices, strikes, expiries,
                                    0.0, iv, 0.0, model='black_scholes', return_as='dict')
        except:
            return

        # Store the results
        for i in range(len(optionContracts)):
            optionContract = optionContracts[i]
            symbol = optionContract.symbol
            deltaVal = greeks['delta'][i]
            gammaVal = greeks['gamma'][i]
            thetaVal = greeks['theta'][i]
            vegaVal = greeks['vega'][i]
            ivVal = iv[i]

            if ois[i] <= 0:
                if symbol in self.__optionData:
                    ois[i] = self.__optionData[symbol].oi

            self.__optionData[symbol] = OptionGreeks(
                optionContract, prices[i], deltaVal, gammaVal, thetaVal, vegaVal, ivVal, ois[i])

    def getOptionData(self, bars) -> dict:
        self.__calculateGreeks(bars)
        return self.__optionData

    def getATMStrike(self, ltp, strikeDifference):
        inputPrice = int(ltp)
        remainder = int(inputPrice % strikeDifference)
        if remainder < int(strikeDifference / 2):
            return inputPrice - remainder
        else:
            return inputPrice + (strikeDifference - remainder)

    def buildOptionContracts(self):
        for instrument in self.getFeed().getRegisteredInstruments():
            optionContract = self.getBroker().getOptionContract(instrument)
            if optionContract is not None:
                self.__optionContracts[instrument] = optionContract

    def getOptionSymbol(self, underlying, expiry, strike, type):
        options = [opt for opt in self.__optionContracts.values(
        ) if opt.type == type and opt.expiry == expiry and opt.underlying == underlying and opt.strike == strike]
        return options[0].symbol if len(options) > 0 else None
    
    def getOptionContracts(self):
        return self.__optionContracts
