import logging
import numpy as np
import datetime
import pandas as pd

from pyalgotrade.broker import Order, OrderExecutionInfo
from pyalgotrade.strategy import position
from pyalgotrade import strategy
import pyalgomate.utils as utils
from pyalgomate.strategies import OptionGreeks
from py_vollib_vectorized import vectorized_implied_volatility, get_all_greeks


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class Expiry(object):
    WEEKLY = 1
    MONTHLY = 2


class BaseOptionsGreeksStrategy(strategy.BaseStrategy):

    def __init__(self, feed, broker, strategyName, logger: logging.Logger, callback=None, resampleFrequency=None):
        super(BaseOptionsGreeksStrategy, self).__init__(feed, broker)
        self.marketEndTime = datetime.time(hour=15, minute=30)
        self.strategyName = strategyName
        self.logger = logger

        self._observers = []
        if callback:
            self._observers.append(callback)
        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

        self.reset()

        self.tradesDf = pd.DataFrame(columns=['Entry Date/Time', 'Exit Date/Time',
                                     'Instrument', 'Buy/Sell', 'Quantity', 'Entry Price', 'Exit Price', 'PnL'])
        self.tradesCSV = f"{self.strategyName}_trades.csv"

    def reset(self):
        self.__optionData = dict()
        self.openPositions = {}
        self.closedPositions = {}
        self.overallPnL = 0
        self.state = State.LIVE

    def resampledOnBars(self, bars):
        if self.state != State.ENTERED:
            return

        jsonData = {
            "datetime": bars.getDateTime().strftime('%Y-%m-%dT%H:%M:%S'),
            "metrics": {
                "pnl": self.overallPnL
            },
            "charts": {
                "pnl": self.overallPnL
            }
        }

        combinedPremium = 0
        for instrument, openPosition in list(self.openPositions.items()):
            ltp = self.getFeed().getDataSeries(instrument)[-1].getClose()
            jsonData["metrics"][f"{instrument} PnL"] = jsonData["charts"][f"{instrument} PnL"] = self.getPnL(
                openPosition)
            jsonData["metrics"][f"{instrument} LTP"] = jsonData["charts"][f"{instrument} LTP"] = ltp
            combinedPremium += ltp

        jsonData["metrics"]["Combined Premium"] = combinedPremium

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

        jsonData["trades"] = self.tradesDf.to_json()

        for callback in self._observers:
            callback(self.strategyName, jsonData)

    def log(self, message, level=logging.INFO):
        if level == logging.DEBUG:
            self.logger.debug(f"{self.strategyName} {message}")
        else:
            self.logger.info(f"{self.strategyName} {message}")

    def getPnL(self, order: Order):
        if order is None:
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
        openPositions = self.openPositions.copy()
        for instrument, openPosition in openPositions.items():
            pnl += self.getPnL(openPosition)

        closedPositions = self.closedPositions.copy()
        for instrument, closedPositionByInstrument in closedPositions.items():
            for closedPosition in closedPositionByInstrument:
                entryOrder = closedPosition["entryOrder"]
                exitOrder = closedPosition["exitOrder"]
                entryPrice = entryOrder.getAvgFillPrice()
                exitPrice = exitOrder.getAvgFillPrice()

                if entryOrder.isBuy():
                    pnl += (exitPrice * exitOrder.getQuantity()) - \
                        (entryPrice * entryOrder.getQuantity())
                else:
                    pnl += (entryPrice * entryOrder.getQuantity()) - \
                        (exitPrice * exitOrder.getQuantity())

        return pnl

    def onEnterOk(self, position: position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        action = "Buy" if position.getEntryOrder().isBuy() else "Sell"
        self.log(f"{execInfo.getDateTime()} ===== {action} Position opened: {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

        self.openPositions[position.getInstrument()] = position.getEntryOrder()

        # Append a new row to the tradesDf DataFrame with the trade information
        newRow = {'Entry Date/Time': execInfo.getDateTime().strftime('%Y-%m-%dT%H:%M:%S'),
                  'Exit Date/Time': None,
                  'Instrument': position.getInstrument(),
                  'Buy/Sell': "Buy" if position.getEntryOrder().isBuy() else "Sell",
                  'Quantity': execInfo.getQuantity(),
                  'Entry Price': position.getEntryOrder().getAvgFillPrice(),
                  'Exit Price': None,
                  'PnL': None}
        self.tradesDf = pd.concat([self.tradesDf, pd.DataFrame(
            [newRow], columns=self.tradesDf.columns)], ignore_index=True)

        self.log(
            f"Option greeks for {position.getInstrument()}\n{self.__optionData[position.getInstrument()]}", logging.DEBUG)

    def onExitOk(self, position: position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.log(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

        if self.openPositions.get(position.getInstrument(), None) is None:
            self.log(
                f"{execInfo.getDateTime()} - {position.getInstrument()} not found in open positions.")
            return

        # Check if the symbol already exists in closedPositions
        entryOrder = self.openPositions.pop(position.getInstrument())
        if position.getInstrument() in self.closedPositions:
            # Append the new exit and entry orders to the list of dictionaries for this symbol
            self.closedPositions[position.getInstrument()].append({
                "exitOrder": position.getExitOrder(),
                "entryOrder": entryOrder
            })
        else:
            # Create a new list of dictionaries for this symbol and append the new exit and entry orders
            self.closedPositions[position.getInstrument()] = [{
                "exitOrder": position.getExitOrder(),
                "entryOrder": entryOrder
            }]

        # Update the corresponding row in the tradesDf DataFrame with the exit information
        entryPrice = entryOrder.getAvgFillPrice()
        exitPrice = position.getExitOrder().getAvgFillPrice()
        pnl = ((exitPrice - entryPrice) * entryOrder.getExecutionInfo().getQuantity()
               ) if entryOrder.isBuy() else ((entryPrice - exitPrice) * entryOrder.getExecutionInfo().getQuantity())

        idx = self.tradesDf.loc[self.tradesDf['Instrument']
                                == position.getInstrument()].index[-1]
        self.tradesDf.loc[idx, ['Exit Date/Time', 'Exit Price', 'PnL']] = [
            execInfo.getDateTime().strftime('%Y-%m-%dT%H:%M:%S'), exitPrice, pnl]
        self.tradesDf.to_csv(self.tradesCSV)

        self.log(
            f"Option greeks for {position.getInstrument()}\n{self.__optionData[position.getInstrument()]}", logging.DEBUG)

    def haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def getNearestDeltaOption(self, optionType, deltaValue, expiry):
        options = [opt for opt in self.__optionData.values(
        ) if opt.optionContract.type == optionType and opt.optionContract.expiry == expiry]
        options.sort(key=lambda x: abs(
            x.delta + abs(deltaValue) if optionType == 'p' else x.delta - abs(deltaValue)))
        return options[0] if len(options) > 0 else None

    def getOverallDelta(self):
        delta = 0
        for instrument, openPosition in self.openPositions.copy().items():
            delta += self.__optionData[openPosition.getInstrument()].delta

        return delta

    def __getUnderlyingPrice(self, underlyingInstrument):
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
        for instrument, bar in bars.items():
            optionContract = self.getBroker().getOptionContract(instrument)

            if optionContract is not None:
                underlyingPrice = self.__getUnderlyingPrice(
                    optionContract.underlying)
                if underlyingPrice is None:
                    underlyingPrice = self.__getUnderlyingPrice(
                        "NSE|NIFTY BANK" if optionContract.underlying == "NFO|BANKNIFTY" else "NSE|NIFTY INDEX")
                    if underlyingPrice is None:
                        return
                    optionContract.underlying = "NSE|NIFTY BANK" if optionContract.underlying == "NFO|BANKNIFTY" else "NSE|NIFTY INDEX"
                underlyingPrices.append(underlyingPrice)
                optionContracts.append(optionContract)
                strikes.append(optionContract.strike)
                prices.append(bar.getClose())
                if optionContract.expiry is None:
                    expiry = utils.getNearestMonthlyExpiryDate(
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
            self.__optionData[symbol] = OptionGreeks(
                optionContract, prices[i], deltaVal, gammaVal, thetaVal, vegaVal, ivVal)

    def getOptionData(self, bars) -> dict:
        self.__calculateGreeks(bars)
        return self.__optionData
