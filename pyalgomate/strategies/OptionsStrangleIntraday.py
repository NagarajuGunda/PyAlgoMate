import logging
import datetime
from collections import deque

from pyalgotrade import strategy
import pyalgomate.utils as utils

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__file__)


def findOTMStrikes(ltp, strikeDifference, nStrikesAway):
    closestStrike = int(round((ltp / strikeDifference), 0) * strikeDifference)
    ceStrike = closestStrike + (nStrikesAway * strikeDifference)
    peStrike = closestStrike - (nStrikesAway * strikeDifference)

    return ceStrike, peStrike


class OptionsStrangleIntraday(strategy.BaseStrategy):

    def __init__(self, feed, broker, underlyingInstrument, callback=None, resampleFrequency=None):
        super(OptionsStrangleIntraday, self).__init__(feed, broker)
        self.__reset__()
        self._observers = []
        self.currentDate = None
        self.bars = {}
        self.maxLen = 255
        self.broker = broker
        self.underlyingInstrument = underlyingInstrument
        if callback:
            self._observers.append(callback)

        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

    def __reset__(self):
        self.entryHour = 9
        self.entryMinute = 17
        self.entrySecond = 0
        self.exitHour = 15
        self.exitMinute = 15
        self.exitSecond = 0

        self.strikeDifference = 100
        self.nStrikesAway = 0
        self.quantity = 25
        self.slPercentage = 25
        self.targetPercentage = 80
        self._pnl = 0
        self.cePnl = 0
        self.pePnl = 0
        self.premium = 100
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
        self.ceReEntry = 1
        self.peReEntry = 1
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
            callback("OptionsStrangleIntraday", jsonData)

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
            ceStrike, peStrike = findOTMStrikes(
                bar.getClose(), self.strikeDifference, self.nStrikesAway)

            self.ceSymbol, self.peSymbol = self.broker.getOptionSymbols(
                self.underlyingInstrument, utils.getNearestWeeklyExpiryDate(dateTime.date()), ceStrike, peStrike)

        if (not self.bars.get(self.ceSymbol, None)) or (not self.bars.get(self.peSymbol, None)):
            return

        self.ceLTP = self.bars[self.ceSymbol][-1].getClose()
        self.peLTP = self.bars[self.peSymbol][-1].getClose()

        if self.cePosition and ((self.ceLTP > self.ceSL) or (self.ceLTP < self.ceTarget)):
            self.cePosition = self.cePosition.exitMarket()

        if self.pePosition and ((self.peLTP > self.peSL) or (self.peLTP < self.peTarget)):
            self.pePosition = self.pePosition.exitMarket()

        if self.ceReEntry > 0 and (not self.cePosition):
            self.cePosition = self.enterShort(self.ceSymbol, self.quantity)
            # self.cePosition.getEntryOrder().getExecutionInfo().getPrice()
            self.ceEnteredPrice = self.ceLTP
            self.ceSL = round(self.ceEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.ceTarget = round(self.ceEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)
            self.ceReEntry -= 1

        if self.peReEntry > 0 and (not self.pePosition):
            self.pePosition = self.enterShort(self.peSymbol, self.quantity)
            # self.pePosition.getEntryOrder().getExecutionInfo().getPrice()
            self.peEnteredPrice = self.peLTP
            self.peSL = round(self.peEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.peTarget = round(self.peEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)
            self.peReEntry -= 1

        if (dateTime.hour >= self.exitHour and dateTime.minute >= self.exitMinute and dateTime.second >= self.exitSecond):
            if self.cePosition:
                self.cePosition = self.cePosition.exitMarket()
                self.cePnl = ((self.ceEnteredPrice - self.ceLTP)
                              * self.quantity)

            if self.pePosition:
                self.pePosition = self.pePosition.exitMarket()
                self.pePnl = ((self.peEnteredPrice - self.peLTP)
                              * self.quantity)

        if self.cePosition:
            self.cePnl = ((self.ceEnteredPrice - self.ceLTP) * self.quantity)

        if self.pePosition:
            self.pePnl = ((self.peEnteredPrice - self.peLTP) * self.quantity)

        self.pnl = self.cePnl + self.pePnl

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        logger.info(
            f"===== Entered {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} {execInfo.getQuantity()} =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        logger.info(
            f"===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")
