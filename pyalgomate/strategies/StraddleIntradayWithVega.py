import logging
import datetime
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy

logger = logging.getLogger(__file__)


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class Expiry(object):
    WEEKLY = 1
    MONTHLY = 2


class StraddleIntradayWithVega(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, registeredOptionsCount=None, callback=None, resampleFrequency=None, lotSize=None):
        super(StraddleIntradayWithVega, self).__init__(feed, broker)
        self._observers = []
        if callback:
            self._observers.append(callback)
        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

        self.strategyName = __class__.__name__
        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=00)
        self.marketEndTime = datetime.time(hour=15, minute=30)
        self.expiry = Expiry.WEEKLY
        self.initialDeltaDifference = 0.5
        self.deltaThreshold = 0.2
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 4000
        self.vegaSL = 10

        self.overallPnL = 0
        self.tradesDf = pd.DataFrame(columns=['Entry Date/Time', 'Exit Date/Time',
                                     'Instrument', 'Buy/Sell', 'Quantity', 'Entry Price', 'Exit Price', 'PnL'])
        self.tradesCSV = f"{self.strategyName}_trades.csv"

        self.registeredOptionsCount = registeredOptionsCount if registeredOptionsCount is not None else 0

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.optionData = dict()
        self.state = State.LIVE
        self.positionCall = None
        self.positionPut = None
        self.positionVega = None
        self.openPositions = {}
        self.closedPositions = {}
        self.overallPnL = 0
        self.numberOfAdjustments = 0

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
        for instrument, optionGreek in self.optionData.items():
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
            callback(__class__.__name__, jsonData)

    def log(self, message, level=logging.INFO):
        if level == logging.DEBUG:
            logger.debug(f"{self.strategyName} {message}")
        else:
            logger.info(f"{self.strategyName} {message}")

    def getPnL(self, order):
        if order is None:
            return 0

        entryPrice = order.getAvgFillPrice()
        exitPrice = self.getFeed().getDataSeries(
            order.getInstrument())[-1].getClose()

        if order.isBuy():
            return (exitPrice - entryPrice) * order.getExecutionInfo().getQuantity()
        else:
            return (entryPrice - exitPrice) * order.getExecutionInfo().getQuantity()

    def getOverallPnL(self, bars):
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

    def onEnterOk(self, position):
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
            f"Option greeks for {position.getInstrument()}\n{self.optionData[position.getInstrument()]}", logging.DEBUG)

    def onExitOk(self, position):
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
            f"Option greeks for {position.getInstrument()}\n{self.optionData[position.getInstrument()]}", logging.DEBUG)

    def closeAllPositions(self):
        self.state = State.EXITED
        if self.positionCall:
            self.positionCall.exitMarket()
            self.positionCall = None

        if self.positionPut:
            self.positionPut.exitMarket()
            self.positionPut = None

        if self.positionVega:
            self.positionVega.exitMarket()
            self.positionVega = None

    def __haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def getNearestDeltaOption(self, optionType, deltaValue, expiry):
        options = [opt for opt in self.optionData.values(
        ) if opt.optionContract.type == optionType and opt.optionContract.expiry == expiry]
        options.sort(key=lambda x: abs(
            x.delta + abs(deltaValue) if optionType == 'p' else x.delta - abs(deltaValue)))
        return options[0] if len(options) > 0 else None

    def getOverallDelta(self):
        delta = 0
        for instrument, openPosition in self.openPositions.copy().items():
            delta += self.optionData[openPosition.getInstrument()].delta

        return delta

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
        overallDelta = self.getOverallDelta()

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        )) if self.expiry == Expiry.WEEKLY else utils.getNearestMonthlyExpiryDate(bars.getDateTime().date())
        # monthlyExpiry = currentExpiry
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            bars.getDateTime().date())
        # monthlyExpiry = utils.getNextMonthlyExpiryDate(
        #     bars.getDateTime().date()) if monthlyExpiry == currentExpiry else monthlyExpiry

        self.optionData = self.getOptionData(bars)
        if (self.registeredOptionsCount > 0) and (len(self.optionData) < self.registeredOptionsCount):
            return

        if self.state == State.LIVE:
            if bars.getDateTime().time() >= self.entryTime and bars.getDateTime().time() < self.exitTime:
                selectedCallOption = self.getNearestDeltaOption(
                    'c', self.initialDeltaDifference, currentExpiry)
                selectedPutOption = self.getNearestDeltaOption(
                    'p', self.initialDeltaDifference, currentExpiry)

                if selectedCallOption is None or selectedPutOption is None:
                    return

                # Return if we do not have LTP for selected options yet
                if not (self.__haveLTP(selectedCallOption.optionContract.symbol) and self.__haveLTP(selectedPutOption.optionContract.symbol)):
                    return

                # Place initial delta-neutral positions
                self.positionCall = self.enterShort(
                    selectedCallOption.optionContract.symbol, self.quantity)
                self.positionPut = self.enterShort(
                    selectedPutOption.optionContract.symbol, self.quantity)
                self.log(
                    f"Date/Time {bars.getDateTime()}. Taking initial positions!")
                self.state = State.PLACING_ORDERS
        elif self.state == State.PLACING_ORDERS:
            # Wait until both positions are entered
            if self.positionCall is not None and self.positionPut is not None:
                if self.positionCall.getInstrument() in self.openPositions and self.positionPut.getInstrument() in self.openPositions:
                    if self.positionVega is not None:
                        if self.positionVega.getInstrument() in self.openPositions:
                            self.state = State.ENTERED
                    else:
                        self.state = State.ENTERED
        elif self.state == State.ENTERED:
            # Exit all positions if exit time is met or portfolio SL is hit
            if bars.getDateTime().time() >= self.exitTime:
                self.log(
                    f"Current time {bars.getDateTime().time()} is >= Exit time {self.exitTime}. Closing all positions!")
                self.closeAllPositions()
                return

            self.overallPnL = self.getOverallPnL(bars)

            if self.overallPnL <= -self.portfolioSL:
                self.log(
                    f"Portfolio SL({self.portfolioSL} is hit. Current PnL is {self.overallPnL}. Exiting all positions!)")
                self.closeAllPositions()
                return

            if self.positionVega is None:
                # Adjust positions if delta difference is more than delta threshold
                callOptionGreeks = self.optionData[self.positionCall.getInstrument(
                )]
                putOptionGreeks = self.optionData[self.positionPut.getInstrument(
                )]

                deltaDifference = abs(
                    callOptionGreeks.delta + putOptionGreeks.delta)

                if deltaDifference > self.deltaThreshold:
                    optionGreek = callOptionGreeks if abs(
                        callOptionGreeks.delta) > abs(putOptionGreeks.delta) else putOptionGreeks
                    selectedOption = self.getNearestDeltaOption(
                        optionGreek.optionContract.type, abs(optionGreek.delta) + 0.1, monthlyExpiry)
                    if selectedOption.optionContract.symbol not in [callOptionGreeks.optionContract.symbol, putOptionGreeks.optionContract.symbol]:
                        self.log(
                            f"Delta difference threshold has reached. Current difference is {deltaDifference}. Managing vega by buying an option. Current PnL is {self.overallPnL}).")
                        self.state = State.PLACING_ORDERS
                        self.positionVega = self.enterLong(
                            selectedOption.optionContract.symbol, self.quantity)
            else:
                # Check if SL is hit for the buy position
                entryOrder = self.openPositions[self.positionVega.getEntryOrder(
                ).getInstrument()]
                pnl = self.getPnL(entryOrder)
                entryPrice = entryOrder.getAvgFillPrice()

                pnLPercentage = (
                    pnl / entryPrice) * 100

                if pnLPercentage <= -self.vegaSL:
                    self.log(
                        f'SL {self.vegaSL}% hit for {self.positionVega.getInstrument()}. Exiting position!')
                    self.state = State.PLACING_ORDERS
                    self.positionVega.exitMarket()
                    self.positionVega = None
                    return
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass

        self.overallPnL = self.getOverallPnL(bars)

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
