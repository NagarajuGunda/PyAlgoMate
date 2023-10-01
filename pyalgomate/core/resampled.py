import datetime
from pyalgotrade.dataseries import resampled
from pyalgotrade import resamplebase
from pyalgotrade import bar


class BarsGrouper(resamplebase.Grouper):
    def __init__(self, groupDateTime, bars, frequency):
        resamplebase.Grouper.__init__(self, groupDateTime)
        self.__barGroupers = {}
        self.__frequency = frequency

        # Initialize BarGrouper instances for each instrument.
        for instrument, bar_ in bars.items():
            barGrouper = resampled.BarGrouper(groupDateTime, bar_, frequency)
            self.__barGroupers[instrument] = barGrouper

    def addValue(self, value):
        # Update or initialize BarGrouper instances for each instrument.
        for instrument, bar_ in value.items():
            barGrouper = self.__barGroupers.get(instrument)
            if barGrouper:
                barGrouper.addValue(bar_)
            else:
                barGrouper = resampled.BarGrouper(
                    self.getDateTime(), bar_, self.__frequency)
                self.__barGroupers[instrument] = barGrouper

    def getGrouped(self):
        bar_dict = {}
        for instrument, grouper in self.__barGroupers.items():
            bar_dict[instrument] = grouper.getGrouped()
        return bar.Bars(bar_dict)


class ResampledBars():
    def __init__(self, barFeed, frequency, callback):
        self.__barFeed = barFeed
        self.__frequency = frequency
        self.__callback = callback
        self.__values = []
        self.__grouper = None
        self.__range = None

    def getFrequency(self):
        return self.__frequency

    def addBars(self, dateTime, value):
        if self.__range is None:
            self.__range = resamplebase.build_range(
                dateTime, self.getFrequency())
            self.__grouper = BarsGrouper(
                self.__range.getBeginning(), value, self.getFrequency())
        elif self.__range.belongs(dateTime):
            self.__grouper.addValue(value)

        barFeedFrequency = self.__barFeed.getFrequency()
        nextDateTime = dateTime + datetime.timedelta(
            seconds=barFeedFrequency if barFeedFrequency is not None and barFeedFrequency > 0 else 0)

        if not self.__range.belongs(nextDateTime):
            self.__values.append(self.__grouper.getGrouped())
            self.__range = resamplebase.build_range(
                nextDateTime, self.getFrequency())
            self.__grouper = BarsGrouper(
                self.__range.getBeginning(), value, self.getFrequency())

        if len(self.__values):
            self.__callback(self.__values.pop(0))
