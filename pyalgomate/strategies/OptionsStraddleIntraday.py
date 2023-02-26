import logging
import datetime
from collections import deque

from pyalgotrade import strategy
import pyalgomate.utils as utils

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__file__)


def findATMStrike(ltp, strikeDifference):
    return int(round((ltp / strikeDifference), 0) * strikeDifference)


class OptionsStraddleIntraday(strategy.BaseStrategy):

    def __init__(self, feed, broker, underlyingInstrument, callback=None, resampleFrequency=None):
        super(OptionsStraddleIntraday, self).__init__(feed, broker)
        self.__reset__()
        self._observers = []
        self.currentDate = None
        self.bars = {}
        self.maxLen = 255
        self.underlyingInstrument = underlyingInstrument

        self.strikeDifference = 100
        self.quantity = 25
        self.slPercentage = 25
        self.targetPercentage = 80

        # Entry and exit timings
        self.entryHour = 9
        self.entryMinute = 17
        self.entrySecond = 0
        self.exitHour = 15
        self.exitMinute = 15
        self.exitSecond = 0
        self.takeProfit = 3000
        self.maxLoss = 2500

        if callback:
            self._observers.append(callback)

        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

    def __reset__(self):
        self.ceReEntry = 1
        self.peReEntry = 1
        self._pnl = 0
        self.cePnl = 0
        self.pePnl = 0
        self.ceSymbol = None
        self.peSymbol = None
        self.ceSL = None
        self.peSL = None
        self.ceTarget = None
        self.peTarget = None
        self.cePosition = None
        self.pePosition = None
        self.ceEnteredPrice = None
        self.peEnteredPrice = None
        self.ceLTP = None
        self.peLTP = None

    @property
    def pnl(self):
        return self._pnl

    @pnl.setter
    def pnl(self, value):
        self._pnl = value

    def resampledOnBars(self, bars):
        for callback in self._observers:
            jsonData = {
                "datetime": bars.getDateTime().strftime('%Y-%m-%dT%H:%M:%S'),
                "metrics": {
                    "pnl": self._pnl,
                    "cePnl": self.cePnl,
                    "pePnl": self.pePnl,
                    "ceSL": self.ceSL,
                    "peSL": self.peSL,
                    "ceTarget": self.ceTarget,
                    "peTarget": self.peTarget,
                    "ceEnteredPrice": self.ceEnteredPrice,
                    "peEnteredPrice": self.peEnteredPrice,
                    "ceLTP": self.ceLTP,
                    "peLTP": self.peLTP
                },
                "charts": {
                    "pnl": self._pnl,
                    "cePnl": self.cePnl,
                    "pePnl": self.pePnl,
                    "ceSL": self.ceSL,
                    "peSL": self.peSL,
                    "ceTarget": self.ceTarget,
                    "peTarget": self.peTarget,
                    "combinedPremium": (self.ceLTP if self.ceLTP != None else 0) + (self.peLTP if self.peLTP != None else 0)
                }
            }
            callback(__class__.__name__, jsonData)

    def pushBars(self, bars):
        for key, value in bars.items():
            if key not in self.bars:
                self.bars[key] = deque(maxlen=self.maxLen)
            self.bars[key].append(value)

    def onBars(self, bars):
        # push the incoming bars into bars dict
        # bars contains a queue of maxLen number of bar data for each instrument
        # access last index element of the queue for the latest bar data
        self.pushBars(bars)

        bar = self.bars.get(self.underlyingInstrument, None)
        if not bar:
            return

        bar = bar[-1]    # get the latest bar
        dateTime = bar.getDateTime()

        if dateTime.date() != self.currentDate:
            self.__reset__()
            self.currentDate = dateTime.date()

        if datetime.time(dateTime.hour, dateTime.minute, dateTime.second) < datetime.time(self.entryHour, self.entryMinute, self.entrySecond):
            return

        if not (self.ceSymbol or self.peSymbol):
            atmStrike = findATMStrike(
                bar.getClose(), self.strikeDifference)

            self.ceSymbol, self.peSymbol = self.getBroker().getOptionSymbols(
                self.underlyingInstrument, utils.getNearestWeeklyExpiryDate(dateTime.date()), atmStrike, atmStrike)

        if (not self.bars.get(self.ceSymbol, None)) or (not self.bars.get(self.peSymbol, None)):
            return

        self.ceLTP = self.bars[self.ceSymbol][-1].getClose()
        self.peLTP = self.bars[self.peSymbol][-1].getClose()

        # If either there are no positions or there are re-entries left, take the short position
        if self.ceReEntry > 0 and (not self.cePosition):
            self.enterShort(self.ceSymbol, self.quantity)
            self.ceReEntry -= 1

        if self.peReEntry > 0 and (not self.pePosition):
            self.enterShort(self.peSymbol, self.quantity)
            self.peReEntry -= 1

        if self.cePosition:
            self.cePnl = ((self.ceEnteredPrice - self.ceLTP) * self.quantity)

        if self.pePosition:
            self.pePnl = ((self.peEnteredPrice - self.peLTP) * self.quantity)

        self.pnl = self.cePnl + self.pePnl

        # Check for exit time condition and exit if there are positions
        # If there is a position and either SL/Target is hit, exit the position
        if self.cePosition and (self.isTimeToExit(dateTime) or (self.ceLTP > self.ceSL) or (self.ceLTP < self.ceTarget) or (self.pnl >= self.takeProfit) or (self.pnl <= -self.maxLoss)):
            self.exitCE()

        if self.pePosition and (self.isTimeToExit(dateTime) or (self.peLTP > self.peSL) or (self.peLTP < self.peTarget) or (self.pnl >= self.takeProfit) or (self.pnl <= -self.maxLoss)):
            self.exitPE()

    def isTimeToExit(self, dateTime):
        return datetime.time(dateTime.hour, dateTime.minute, dateTime.second) >= datetime.time(self.exitHour, self.exitMinute, self.exitSecond)

    def exitCE(self):
        self.cePosition = self.cePosition.exitMarket()
        self.cePnl = ((self.ceEnteredPrice - self.ceLTP)
                      * self.quantity)

        self.pnl = self.cePnl + self.pePnl

    def exitPE(self):
        self.pePosition = self.pePosition.exitMarket()
        self.pePnl = ((self.peEnteredPrice - self.peLTP)
                      * self.quantity)

        self.pnl = self.cePnl + self.pePnl

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        if position.getEntryOrder().getInstrument() == self.ceSymbol:
            self.cePosition = position
            self.ceEnteredPrice = execInfo.getPrice()
            self.ceSL = round(self.ceEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.ceTarget = round(self.ceEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)
        else:
            self.pePosition = position
            self.peEnteredPrice = execInfo.getPrice()
            self.peSL = round(self.peEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.peTarget = round(self.peEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)

        logger.info(
            f"===== Entered {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} {execInfo.getQuantity()} =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        if position.getExitOrder().getInstrument() == self.ceSymbol:
            self.cePnl = ((self.ceEnteredPrice - execInfo.getPrice())
                          * self.quantity)
        else:
            self.pePnl = ((self.peEnteredPrice - self.peLTP)
                          * self.quantity)

        logger.info(
            f"===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")
