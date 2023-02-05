import logging
from pyalgotrade import strategy
from pyalgotrade import bar

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__file__)


def findOTMStrikes(ltp, strikeDifference, nStrikesAway):
    closestStrike = int(round((ltp / strikeDifference), 0) * strikeDifference)
    ceStrike = closestStrike + (nStrikesAway * strikeDifference)
    peStrike = closestStrike - (nStrikesAway * strikeDifference)

    return ceStrike, peStrike


def findOptionSymbols(underlyingInstrument, ceStrikePrice, peStrikePrice):
    return underlyingInstrument + str(ceStrikePrice) + "CE", underlyingInstrument + str(peStrikePrice) + "PE"
    expiry = "23"+"FEB"+"23"

    return "NFO|" + "BANKNIFTY" + expiry + "C"+str(ceStrikePrice), "NFO" + "BANKNIFTY" + expiry+"P"+str(peStrikePrice)


class OptionsStrangleIntraday(strategy.BaseStrategy):
    def __reset__(self):
        self.entryHour = 9
        self.entryMinute = 17
        self.entrySecond = 0
        self.exitHour = 15
        self.exitMinute = 15
        self.exitSecond = 0

        self.underlyingInstrument = 'BANKNIFTY'
        self.strikeDifference = 100
        self.nStrikesAway = 3
        self.quantity = 25
        self.slPercentage = 25
        self.targetPercentage = 80
        self.pnl = 0
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

    def __init__(self, feed, broker, resampleFrequency=None):
        super(OptionsStrangleIntraday, self).__init__(feed, broker)
        self.__reset__()
        self.currentDate = None
        if resampleFrequency:
            self.resampleBarFeed(resampleFrequency, self.resampledOnBars)

    def resampledOnBars(self, bars):
        logger.info("Resampled {0} {1}".format(bars.getDateTime(),
                                               bars[self.underlyingInstrument].getClose()))

    def onBars(self, bars):
        bar = bars.getBar(self.underlyingInstrument)
        if not bar:
            return

        dateTime = bar.getDateTime()

        if dateTime.date() != self.currentDate:
            self.__reset__()
            self.currentDate = dateTime.date()

        if (dateTime.hour >= self.entryHour and dateTime.minute >= self.entryMinute and dateTime.second >= self.entrySecond) is not True:
            return

        if not (self.ceSymbol or self.peSymbol):
            ceStrike, peStrike = findOTMStrikes(
                bar.getClose(), self.strikeDifference, self.nStrikesAway)

            self.ceSymbol, self.peSymbol = findOptionSymbols(
                self.underlyingInstrument, ceStrike, peStrike)

        if (not bars.getBar(self.ceSymbol)) or (not bars.getBar(self.peSymbol)):
            return

        ceLTP = bars[self.ceSymbol].getClose()
        peLTP = bars[self.peSymbol].getClose()

        if self.cePosition and ((ceLTP > self.ceSL) or (ceLTP < self.ceTarget)):
            self.cePosition = self.cePosition.exitMarket()
            self.pnl += ((self.ceEnteredPrice - ceLTP) * self.quantity)

        if self.pePosition and ((peLTP > self.peSL) or (peLTP < self.peTarget)):
            self.pePosition = self.pePosition.exitMarket()
            self.pnl += ((self.peEnteredPrice - peLTP) * self.quantity)

        if self.ceReEntry > 0 and (not self.cePosition):
            self.cePosition = self.enterShort(self.ceSymbol, self.quantity)
            # self.cePosition.getEntryOrder().getExecutionInfo().getPrice()
            self.ceEnteredPrice = ceLTP
            self.ceSL = round(self.ceEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.ceTarget = round(self.ceEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)
            self.ceReEntry -= 1

        if self.peReEntry > 0 and (not self.pePosition):
            self.pePosition = self.enterShort(self.peSymbol, self.quantity)
            # self.pePosition.getEntryOrder().getExecutionInfo().getPrice()
            self.peEnteredPrice = peLTP
            self.peSL = round(self.peEnteredPrice *
                              (1 + (self.slPercentage / 100)), 1)
            self.peTarget = round(self.peEnteredPrice *
                                  (1 - (self.targetPercentage / 100)), 1)
            self.peReEntry -= 1

        if (dateTime.hour >= self.exitHour and dateTime.minute >= self.exitMinute and dateTime.second >= self.exitSecond):
            if self.cePosition:
                self.cePosition = self.cePosition.exitMarket()
                self.pnl += ((self.ceEnteredPrice - ceLTP) * self.quantity)

            if self.pePosition:
                self.pePosition = self.pePosition.exitMarket()
                self.pnl += ((self.peEnteredPrice - peLTP) * self.quantity)

            # logger.info(f"===== PnL for {dateTime} is {self.pnl} =====")

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        # logger.info(
        #     f"===== Entered {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} {execInfo.getQuantity()} =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        # logger.info(
        #     f"===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")
