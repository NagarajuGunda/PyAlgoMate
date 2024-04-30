import datetime
from pyalgotrade.dataseries import resampled
from pyalgotrade import resamplebase
from pyalgotrade.resamplebase import DayRange, MonthRange, TimeRange
from pyalgotrade import bar


class IntraDayRange(TimeRange):
    def __init__(self, dateTime, frequency, startTime: datetime.time = datetime.time(hour=9, minute=15)):
        super(IntraDayRange, self).__init__()
        assert isinstance(frequency, int)
        assert frequency > 1
        assert frequency < bar.Frequency.DAY

        self.startTime = startTime
        self.frequency = frequency
        self.__begin, self.__end = self.calculateTimeRange(dateTime)

    def calculateTimeRange(self, dateTime: datetime.datetime):
        secondsSinceStart = ((dateTime.hour - self.startTime.hour) * 60 * 60) + (
            (dateTime.minute - self.startTime.minute) * 60) + (dateTime.second - self.startTime.second)

        slotStartTime = (dateTime -
                         datetime.timedelta(seconds=secondsSinceStart % self.frequency)).replace(microsecond=0)
        slotEndTime = slotStartTime + \
            datetime.timedelta(seconds=self.frequency)

        return slotStartTime, slotEndTime

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


def build_range(dateTime, frequency):
    assert (isinstance(frequency, int))
    assert (frequency > 1)

    if frequency < bar.Frequency.DAY:
        ret = IntraDayRange(dateTime, frequency)
    elif frequency == bar.Frequency.DAY:
        ret = DayRange(dateTime)
    elif frequency == bar.Frequency.MONTH:
        ret = MonthRange(dateTime)
    else:
        raise Exception("Unsupported frequency")
    return ret


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
        for instrument, grouper in self.__barGroupers.copy().items():
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

    def getBar(self, instrument) -> bar.Bar:
        if self.__grouper is not None:
            return self.__grouper.getGrouped().getBar(instrument)

        return None

    def addBars(self, dateTime, value):
        if self.__range is None:
            self.__range = build_range(
                dateTime, self.getFrequency())
        if self.__grouper is None:
            self.__grouper = BarsGrouper(
                self.__range.getBeginning(), value, self.getFrequency())
        if self.__range is not None and self.__range.belongs(dateTime):
            self.__grouper.addValue(value)

        barFeedFrequency = self.__barFeed.getFrequency()
        nextDateTime = dateTime + datetime.timedelta(
            seconds=barFeedFrequency if barFeedFrequency is not None and barFeedFrequency > 0 else 0)

        if (self.__grouper is not None) and (self.__range is not None) and (not self.__range.belongs(nextDateTime)):
            self.__values.append(self.__grouper.getGrouped())
            self.__grouper = None
            self.__range = None

        if len(self.__values):
            self.__callback(self.__values.pop(0))

    def checkNow(self, dateTime):
        if (self.__grouper is not None) and (self.__range is not None) and (not self.__range.belongs(dateTime)):
            self.__values.append(self.__grouper.getGrouped())
            self.__grouper = None
            self.__range = None

        if len(self.__values):
            self.__callback(self.__values.pop(0))

if __name__ == "__main__":
    dateTime = datetime.datetime.now()
    intradayRange = IntraDayRange(dateTime, 75 * bar.Frequency.MINUTE)
    print(f'Frequency: {intradayRange.frequency // 60}mins. Datetime: {dateTime}. Beggining: {intradayRange.getBeginning()}. Ending: {intradayRange.getEnding()}')

    intradayRange = IntraDayRange(dateTime, 15 * bar.Frequency.MINUTE)
    print(f'Frequency: {intradayRange.frequency // 60}mins. Datetime: {dateTime}. Beggining: {intradayRange.getBeginning()}. Ending: {intradayRange.getEnding()}')

    intradayRange = IntraDayRange(dateTime, 5 * bar.Frequency.MINUTE)
    print(f'Frequency: {intradayRange.frequency // 60}mins. Datetime: {dateTime}. Beggining: {intradayRange.getBeginning()}. Ending: {intradayRange.getEnding()}')
