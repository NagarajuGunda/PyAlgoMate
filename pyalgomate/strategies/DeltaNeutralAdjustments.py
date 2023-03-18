import logging

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

    def resampledOnBars(self, bars):
        pass

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        logger.info(f"{execInfo.getDateTime()} ===== Position opened: {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} {execInfo.getQuantity()} =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at {execInfo.getPrice()} =====")

    def __haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def onBars(self, bars):
        logger.info(f"Datetime {bars.getDateTime()}")

        print(len(self.getOptionData(bars)))
