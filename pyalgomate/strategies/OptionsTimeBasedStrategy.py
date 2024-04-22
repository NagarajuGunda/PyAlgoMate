import logging
from pyalgotrade.strategy import BaseStrategy
from pyalgomate.strategies import OptionStrategy
from pyalgomate import utils

logger = logging.getLogger(__name__)

class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4

class OptionsTimeBasedStrategy(BaseStrategy):
    def __init__(self, feed, broker, strategyFile, callback=None, resampleFrequency=None, telegramBot=None, strategyName=None):
        super(OptionsTimeBasedStrategy, self).__init__(feed, broker)
        self.strategyName = strategyName

        self.strategy = OptionStrategy.from_yaml_file(strategyFile)
        logger.info(f"Loaded the strategy\n\n{self.strategy}\n\n")

        self.overallStopLoss = self.strategy.overallStopLoss
        self.overallTarget = self.strategy.overallTarget

        self.quantity = 25
        self.currentDate = None
        self._observers = []
        
        if callback:
            self._observers.append(callback)

        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

    def __reset__(self):
        self.openPositions = {}
        self.closedPositions = {}
        self.overallPnL = 0
        self.state = State.LIVE

    def resampledOnBars(self, bars):
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
            jsonData["metrics"][f"{instrument} PnL"] = jsonData["charts"][f"{instrument} PnL"] =  self.getPnL(openPosition)
            jsonData["metrics"][f"{instrument} LTP"] = jsonData["charts"][f"{instrument} LTP"] = ltp
            combinedPremium += ltp
        
        jsonData["metrics"]["Combined Premium"] = combinedPremium

        for callback in self._observers:            
            callback(__class__.__name__, jsonData)

    def __getStrikePrice(self, underlyingLTP, strikeType, strike, callOrPut):
        strikeDifference = 100
        atmStrike = int(float(underlyingLTP) /
                        strikeDifference) * strikeDifference

        if strikeType == "StraddleWidth":
            if strike == 'ATM':
                return atmStrike
            else:
                offsetStrike = (int(strike[3:]) * strikeDifference)

                if strike.startswith('ITM'):
                    return (atmStrike - offsetStrike if callOrPut == "Call" else atmStrike + offsetStrike)
                elif strike.startswith('OTM'):
                    return (atmStrike + offsetStrike if callOrPut == "Call" else atmStrike - offsetStrike)
                else:
                    raise ValueError('Invalid strike value')
        else:
            raise ValueError('Invalid strike type')

    def getPnL(self, openPosition):
        order = openPosition.get("entryOrder", None)

        if order is None:
            return 0
        
        entryPrice = order.getAvgFillPrice()
        exitPrice = self.getFeed().getDataSeries(
            order.getInstrument())[-1].getClose()

        if order.isBuy():
            return (exitPrice - entryPrice) * order.getQuantity()
        else:
            return (entryPrice - exitPrice) * order.getQuantity()

    def getOverallPnL(self):
        pnl = 0
        openPositions = self.openPositions
        for instrument, openPosition in openPositions.items():
            pnl += self.getPnL(openPosition)

        closedPositions = self.closedPositions
        for instrument, closedPosition in closedPositions.items():
            if closedPosition.get("exitOrder", None) is None:
                continue

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

        self.openPositions[position.getInstrument()]["entryOrder"] = position.getEntryOrder()

        for instrument, openPosition in self.openPositions.items():
            if openPosition.get("entryOrder", None) is None:
                return
        
        self.state = State.ENTERED

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")

        self.closedPositions[position.getInstrument()]["exitOrder"] = position.getExitOrder()

    def __haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def onBars(self, bars):
        # Get the current timestamp
        currentDateTime = bars.getDateTime()

        if currentDateTime.date() != self.currentDate:
            self.__reset__()
            self.currentDate = currentDateTime.date()

        # Check if we data for underlying instrument
        if not self.__haveLTP(self.strategy.instrument):
            return

        underlyingLTP = self.getFeed().getDataSeries(self.strategy.instrument)[-1].getClose()

        # Collect the symbols and check if we have data for them
        symbols = [self.getBroker().getOptionSymbol(
            self.strategy.instrument, utils.getNearestWeeklyExpiryDate(
                currentDateTime.date()),
            self.__getStrikePrice(underlyingLTP,
                                  position.strikeType, position.strike, position.callOrPut),
            position.callOrPut) for position in self.strategy.positions]

        for symbol in symbols:
            if not self.__haveLTP(symbol):
                return

        # Check if it's time to enter a trade
        if self.strategy.entryTime <= currentDateTime.time() < self.strategy.exitTime and self.state == State.LIVE:
            self.state = State.PLACING_ORDERS
            # Place the trade
            for index, position in enumerate(self.strategy.positions):
                order = self.__placeOrder(symbols[index], position)

                self.openPositions[symbols[index]] = {
                    "placedOrder": order,
                    "position": position
                }

                logger.info(
                    f"{currentDateTime} Order placed for: {symbols[index]}")

        # Check if it's time to exit a trade
        if currentDateTime.time() >= self.strategy.exitTime:
            # Check if we have an open position
            if len(self.openPositions) == 0:
                return

            # Close the trade
            for instrument, openPosition in list(self.openPositions.items()):
                self.__closePosition(openPosition)

            return

        # Check if there are any open positions
        if not len(self.openPositions):
            return
        
    # Check if all positions needs to be closed
        if self.__shouldCloseAllPositions(bars):
            self.__closeAllPositions()
            return
        # Check if any individual position needs to be closed
        for instrument, openPosition in list(self.openPositions.items()):
            if self.__shouldClosePosition(openPosition, bars):
                self.__closePosition(openPosition)

        self.overallPnL = self.getOverallPnL()

    def __updateTrailingStopLoss(self, openPositions, bars):
        for instrument, openPosition in openPositions.items():
            position = openPosition["position"]
            instrument = position.instrument
            pnl = self.getPnL(openPosition)

            # Update trailing stop loss for individual leg
            if position.trailStopLoss is not None:
                trailStopLoss = position.trailStopLoss
                entryPrice = position.entryPrice
                currentPrice = bars[instrument].getClose()

                if position.buyOrSell == "Buy":
                    trailPips = (currentPrice - entryPrice) / \
                        trailStopLoss.trailStep
                    newTrailPrice = entryPrice + trailPips * trailStopLoss.trailStep
                    if newTrailPrice > trailStopLoss.trailPrice:
                        trailStopLoss.trailPrice = newTrailPrice
                        logger.info(
                            f"{bars.getDateTime()} Trailing stop loss updated for {instrument}: {trailStopLoss.trailPrice}")
                else:
                    trailPips = (entryPrice - currentPrice) / \
                        trailStopLoss.trailStep
                    newTrailPrice = entryPrice - trailPips * trailStopLoss.trailStep
                    if newTrailPrice < trailStopLoss.trailPrice:
                        trailStopLoss.trailPrice = newTrailPrice
                        logger.info(
                            f"{bars.getDateTime()} Trailing stop loss updated for {instrument}: {trailStopLoss.trailPrice}")

            # Update overall trailing stop loss
            overallTrailStopLoss = self.strategy.overallTrailStopLoss
            if overallTrailStopLoss is not None:
                entryPrice = position.entryPrice
                currentPrice = bars[instrument].getClose()

                if position.buyOrSell == "Buy":
                    trailPips = (currentPrice - entryPrice) / \
                        overallTrailStopLoss.trailStep
                    newTrailPrice = entryPrice + trailPips * overallTrailStopLoss.trailStep
                    if newTrailPrice > overallTrailStopLoss.trailPrice:
                        overallTrailStopLoss.trailPrice = newTrailPrice
                        logger.info(
                            f"{bars.getDateTime()} Overall trailing stop loss updated: {overallTrailStopLoss.trailPrice}")
                else:
                    trailPips = (entryPrice - currentPrice) / \
                        overallTrailStopLoss.trailStep
                    newTrailPrice = entryPrice - trailPips * overallTrailStopLoss.trailStep
                    if newTrailPrice < overallTrailStopLoss.trailPrice:
                        overallTrailStopLoss.trailPrice = newTrailPrice
                        logger.info(
                            f"{bars.getDateTime()} Overall trailing stop loss updated: {overallTrailStopLoss.trailPrice}")

    def __placeOrder(self, symbolName, position):
        order = self.enterShort(symbolName, self.quantity * position.lots) \
            if position.buyOrSell == "Sell" else self.enterLong(symbolName, self.quantity * position.lots)

        return order

    def __shouldClosePosition(self, openPosition, bars):
        entryOrder = openPosition.get("entryOrder", None)

        if entryOrder is None:
            return
        
        instrument = entryOrder.getInstrument()
        pnl = self.getPnL(openPosition)
        position = openPosition["position"]
        stopLoss = position.stopLoss
        targetProfit = position.targetProfit

        if stopLoss is not None:
            if stopLoss.type == "MTM":
                canClose = pnl <= -stopLoss.stopLoss
                if canClose:
                    logger.info(
                        f"{bars.getDateTime()} Stop loss hit for {instrument}: {pnl} <= -{stopLoss.stopLoss}")
            elif stopLoss.type == "Percentage":
                entryPrice = entryOrder.getAvgFillPrice()
                exitPrice = self.getFeed().getDataSeries(
                    instrument)[-1].getClose()
                if position.buyOrSell == "Buy":
                    diff = exitPrice - entryPrice
                else:
                    diff = entryPrice - exitPrice
                perc = (diff / entryPrice) * 100
                canClose = perc <= -stopLoss.stopLoss
                if canClose:
                    logger.info(
                        f"{bars.getDateTime()} Stop loss hit for {instrument}: {perc} <= -{stopLoss.stopLoss}%")
            else:
                raise ValueError("Invalid stop loss type")

            return canClose

        if targetProfit is not None:
            if targetProfit.type == "MTM":
                canClose = pnl >= targetProfit.targetProfit
                if canClose:
                    logger.info(
                        f"{bars.getDateTime()} Target profit hit for {instrument}: {pnl} >= {targetProfit.targetProfit}")
            elif targetProfit.type == "Percentage":
                entryPrice = entryOrder.getAvgFillPrice()
                exitPrice = self.getFeed().getDataSeries(
                    instrument)[-1].getClose()
                if position.buyOrSell == "Buy":
                    diff = exitPrice - entryPrice
                else:
                    diff = entryPrice - exitPrice    
                perc = (diff / entryPrice) * 100
                canClose = perc >= targetProfit.targetProfit
                if canClose:
                    logger.info(
                        f"{bars.getDateTime()} Target profit hit for {instrument}: {perc} >= {targetProfit.targetProfit}%")
            else:
                raise ValueError("Invalid target profit type")

            return canClose

        return False

    def __closePosition(self, openPosition):
        openPosition["placedOrder"].exitMarket()
        instrument = openPosition["placedOrder"].getInstrument()
        self.openPositions.pop(instrument)
        self.closedPositions[instrument] = openPosition
        
    def getClosedPositions(self):
        return self.closedPositions

    def __shouldCloseAllPositions(self, bars):
        totalPnL = self.getOverallPnL()

        overallStopLoss = self.overallStopLoss
        overallTargetProfit = self.overallTarget

        if overallStopLoss is not None:
            if overallStopLoss.type == "MTM":
                if totalPnL <= -overallStopLoss.stopLoss:
                    logger.info(
                        f"{bars.getDateTime()} Overall stop loss hit: {totalPnL} <= -{overallStopLoss.stopLoss}")
                    return True
            elif overallStopLoss.type == "Percentage":
                overallPerc = (totalPnL / self.getPortfolioValue()) * 100
                if overallPerc <= -overallStopLoss.stopLoss:
                    logger.info(
                        f"{bars.getDateTime()} Overall stop loss hit: {overallPerc}% <= -{overallStopLoss.stopLoss}%")
                    return True

        if overallTargetProfit is not None:
            if overallTargetProfit.type == "MTM":
                if totalPnL >= overallTargetProfit.targetProfit:
                    logger.info(
                        f"{bars.getDateTime()} Overall target profit hit: {totalPnL} >= {overallTargetProfit.targetProfit}")
                    return True
            elif overallTargetProfit.type == "Percentage":
                overallPerc = (totalPnL / self.getPortfolioValue()) * 100
                if overallPerc >= overallTargetProfit.targetProfit:
                    logger.info(
                        f"{bars.getDateTime()} Overall target profit hit: {overallPerc}% >= {overallTargetProfit.targetProfit}%")
                    return True

        return False

    def __closeAllPositions(self):
        self.state = State.EXITED
        for instrument, openPosition in list(self.openPositions.items()):
            self.__closePosition(openPosition)
