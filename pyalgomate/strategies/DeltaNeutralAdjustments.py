import logging
import datetime
import pandas as pd

from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy

logger = logging.getLogger(__file__)


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class DeltaNeutralAdjustments(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, callback=None, resampleFrequency=None):
        super(DeltaNeutralAdjustments, self).__init__(feed, broker)
        self._observers = []
        if callback:
            self._observers.append(callback)
        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=00)
        self.initialDeltaDifference = 0.25
        self.deltaThreshold = 0.2
        self.lotSize = 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots

        self.currentDate = None
        self.overallPnL = 0
        self.tradesDf = pd.DataFrame(columns=['Entry Date/Time', 'Exit Date/Time',
                                     'Instrument', 'Buy/Sell', 'Quantity', 'Entry Price', 'Exit Price'])
        self.tradesCSV = 'trades.csv'

    def __reset__(self):
        # members that needs to be reset after exit time
        self.optionData = dict()
        self.state = State.LIVE
        self.positionCall = None
        self.positionPut = None
        self.openPositions = {}
        self.closedPositions = {}
        self.overallPnL = 0

    def resampledOnBars(self, bars):
        pass

    def getPnL(self, order):
        if order is None:
            return 0

        entryPrice = order.getAvgFillPrice()
        exitPrice = self.getFeed().getDataSeries(
            order.getInstrument())[-1].getClose()

        if order.isBuy():
            return (exitPrice - entryPrice) * order.getQuantity()
        else:
            return (entryPrice - exitPrice) * order.getQuantity()

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
        logger.info(f"{execInfo.getDateTime()} ===== Position opened: {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} {execInfo.getQuantity()} =====")

        self.openPositions[position.getInstrument()] = position.getEntryOrder()

        # Append a new row to the tradesDf DataFrame with the trade information
        newRow = {'Entry Date/Time': execInfo.getDateTime(),
                  'Exit Date/Time': None,
                  'Instrument': position.getInstrument(),
                  'Buy/Sell': "Buy" if position.getEntryOrder().isBuy() else "Sell",
                  'Quantity': execInfo.getQuantity(),
                  'Entry Price': position.getEntryOrder().getAvgFillPrice(),
                  'Exit Price': None}
        self.tradesDf = pd.concat([self.tradesDf, pd.DataFrame(
            [newRow], columns=self.tradesDf.columns)], ignore_index=True)

        logger.info(
            f"Option greeks for {position.getInstrument()}\n{self.optionData[position.getInstrument()]}")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")

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
        idx = self.tradesDf.loc[self.tradesDf['Instrument']
                                == position.getInstrument()].index[-1]
        self.tradesDf.loc[idx, ['Exit Date/Time', 'Exit Price']] = [
            execInfo.getDateTime(), position.getExitOrder().getAvgFillPrice()]
        self.tradesDf.to_csv(self.tradesCSV)

        logger.info(
            f"Option greeks for {position.getInstrument()}\n{self.optionData[position.getInstrument()]}")

    def closeAllPositions(self):
        self.state = State.EXITED
        if self.positionCall:
            self.positionCall.exitMarket()
            self.positionCall = None

        if self.positionPut:
            self.positionPut.exitMarket()
            self.positionPut = None

    def __haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def onBars(self, bars):
        if bars.getDateTime().date() != self.currentDate:
            logger.info(
                f"Overall PnL for {self.currentDate} is {self.overallPnL}")
            self.__reset__()
            self.currentDate = bars.getDateTime().date()

        self.optionData = self.getOptionData(bars)
        if self.state == State.LIVE:
            if bars.getDateTime().time() >= self.entryTime:
                # Find call and put options with delta closest to initial delta difference
                callOptions = [opt for opt in self.optionData.values(
                ) if opt.optionContract.type == 'c']
                putOptions = [opt for opt in self.optionData.values(
                ) if opt.optionContract.type == 'p']

                # Sort the options based on nearness to initial delta difference and select the nearest one
                callOptions.sort(key=lambda x: abs(
                    x.delta - self.initialDeltaDifference))
                putOptions.sort(key=lambda x: abs(
                    x.delta + self.initialDeltaDifference))
                selectedCallOption = callOptions[0]
                selectedPutOption = putOptions[0]

                # Return if we do not have LTP for selected options yet
                if not (self.__haveLTP(selectedCallOption.optionContract.symbol) and self.__haveLTP(selectedPutOption.optionContract.symbol)):
                    return

                # Place initial delta-neutral positions
                self.positionCall = self.enterShort(
                    selectedCallOption.optionContract.symbol, self.quantity)
                self.positionPut = self.enterShort(
                    selectedPutOption.optionContract.symbol, self.quantity)

                self.state = State.PLACING_ORDERS
        elif self.state == State.PLACING_ORDERS:
            # Wait until both positions are entered
            if self.positionCall is not None and self.positionPut is not None:
                if self.positionCall.getInstrument() in self.openPositions and self.positionPut.getInstrument() in self.openPositions:
                    self.state = State.ENTERED
        elif self.state == State.ENTERED:
            # Exit all positions if exit time is met
            if bars.getDateTime().time() >= self.exitTime:
                self.closeAllPositions()
                return

            # Adjust positions if delta difference is more than delta threshold
            callOptionGreeks = self.optionData[self.positionCall.getInstrument(
            )]
            putOptionGreeks = self.optionData[self.positionPut.getInstrument()]

            deltaDifference = abs(
                callOptionGreeks.delta + putOptionGreeks.delta)

            if deltaDifference > self.deltaThreshold:
                self.state = State.PLACING_ORDERS
                # Close the profit making position and take another position with delta nearest to that of other option
                if abs(callOptionGreeks.delta) > abs(putOptionGreeks.delta):
                    self.positionPut.exitMarket()
                    # Find put option with delta closest to delta of call option
                    putOptions = [opt for opt in self.optionData.values(
                    ) if opt.optionContract.type == 'p']
                    putOptions.sort(key=lambda x: abs(
                        x.delta + callOptionGreeks.delta))
                    selectedPutOption = putOptions[0]
                    self.positionPut = self.enterShort(
                        selectedPutOption.optionContract.symbol, self.quantity)
                else:
                    self.positionCall.exitMarket()
                    # Find call option with delta closest to delta of put option
                    callOptions = [opt for opt in self.optionData.values(
                    ) if opt.optionContract.type == 'c']
                    callOptions.sort(key=lambda x: abs(
                        x.delta + putOptionGreeks.delta))
                    selectedCallOption = callOptions[0]
                    self.positionCall = self.enterShort(
                        selectedCallOption.optionContract.symbol, self.quantity)
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass

        self.overallPnL = self.getOverallPnL(bars)
