"""
.. moduleauthor:: Nagaraju Gunda
"""

import six
import glob
import pandas as pd

from pyalgotrade.utils import csvutils
from pyalgotrade import bar
from pyalgotrade.barfeed.csvfeed import BarFeed
from pyalgotrade.barfeed.csvfeed import GenericRowParser


class CustomRowParser(GenericRowParser):
    def __init__(self, columnNames, dateTimeFormat, dailyBarTime, frequency, timezone, barClass=bar.BasicBar):
        super(CustomRowParser, self).__init__(columnNames,
                                              dateTimeFormat, dailyBarTime, frequency, timezone, barClass)
        self.__instrument = columnNames["ticker"]
        self.__dateTimeColName = columnNames["datetime"]
        self.__openColName = columnNames["open"]
        self.__highColName = columnNames["high"]
        self.__lowColName = columnNames["low"]
        self.__closeColName = columnNames["close"]
        self.__volumeColName = columnNames["volume"]
        self.__openInterestColName = columnNames["open_interest"]
        self.__adjCloseColName = None
        self.__frequency = frequency
        self.__barClass = barClass
        self.__columnNames = columnNames

    def parseBar(self, csvRowDict):
        instrument = csvRowDict[self.__instrument]
        dateTime = self._parseDate(csvRowDict[self.__dateTimeColName])
        open_ = float(csvRowDict[self.__openColName])
        high = float(csvRowDict[self.__highColName])
        low = float(csvRowDict[self.__lowColName])
        close = float(csvRowDict[self.__closeColName])
        volume = float(csvRowDict[self.__volumeColName])
        adjClose = None
        if self.__adjCloseColName is not None:
            adjCloseValue = csvRowDict.get(self.__adjCloseColName, "")
            if len(adjCloseValue) > 0:
                adjClose = float(adjCloseValue)
                self.__haveAdjClose = True

        # Process extra columns.
        extra = {}
        for k, v in six.iteritems(csvRowDict):
            if k not in self.__columnNames.values():
                extra[k] = csvutils.float_or_string(v)

        return instrument, self.__barClass(
            dateTime, open_, high, low, close, volume, adjClose, self.__frequency, extra=extra
        )


class CustomCSVBarFeed(BarFeed):
    """A BarFeed that loads bars from CSV files that have the following format:
    ::

        Date Time,Open,High,Low,Close,Volume,Adj Close
        2013-01-01 13:59:00,13.51001,13.56,13.51,13.56,273.88014126,13.51001

    :param frequency: The frequency of the bars. Check :class:`pyalgotrade.bar.Frequency`.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        * The CSV file **must** have the column names in the first row.
        * It is ok if the **Adj Close** column is empty.
        * When working with multiple instruments:

         * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
         * If any of the instruments loaded are in different timezones, then the timezone parameter should be set.
    """

    def __init__(self, frequency, timezone=None, maxLen=None):
        super(CustomCSVBarFeed, self).__init__(frequency, maxLen)

        self.__frequency = frequency
        self.__timezone = timezone
        # Assume bars don't have adjusted close. This will be set to True after
        # loading the first file if the adj_close column is there.
        self.__haveAdjClose = False

        self.__barClass = bar.BasicBar

        self.__dateTimeFormat = "%d-%m-%Y %H:%M:%S"

        self.__columnNames = {
            "ticker": "Ticker",
            "datetime": "Date/Time",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "open_interest": "Open Interest",
        }
        # self.__dateTimeFormat expects time to be set so there is no need to
        # fix time.
        self.setDailyBarTime(None)

    def barsHaveAdjClose(self):
        return self.__haveAdjClose

    def setNoAdjClose(self):
        self.__columnNames["adj_close"] = None
        self.__haveAdjClose = False

    def setColumnName(self, col, name):
        self.__columnNames[col] = name

    def setDateTimeFormat(self, dateTimeFormat):
        """
        Set the format string to use with strptime to parse datetime column.
        """
        self.__dateTimeFormat = dateTimeFormat

    def setBarClass(self, barClass):
        self.__barClass = barClass

    def getCurrentDateTime(self):
        return super().getCurrentDateTime() if super().getCurrentDateTime() is not None else self.peekDateTime()
    
    def addBarsFromDataframe(self, dataframe, ticker=None, timezone=None):
        """Loads bars for a given instrument from a parquet file.
        The instrument gets registered in the bar feed.

        :param path: The path to the parquet file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        :param skipMalformedBars: True to skip errors while parsing bars.
        :type skipMalformedBars: boolean.
        """
        def parse_bar_skip_malformed(row):
            ret = None
            try:
                ret = self.__barClass(row[self.__columnNames['datetime']],
                                      row[self.__columnNames['open']],
                                      row[self.__columnNames['high']],
                                      row[self.__columnNames['low']],
                                      row[self.__columnNames['close']],
                                      row[self.__columnNames['volume']],
                                      None,
                                      self.__frequency,
                                      extra={
                    self.__columnNames['open_interest']: row[self.__columnNames['open_interest']]}
                )
            except Exception:
                pass
            return ret

        if timezone is None:
            timezone = self.__timezone

        if ticker:
            dataframe = dataframe[dataframe[self.__columnNames['ticker']].str.startswith(ticker)]

        for name, group in dataframe.groupby(self.__columnNames['ticker']):
            bars = []
            for row in group.to_dict('records'):
                bar_ = parse_bar_skip_malformed(row)

                if bar_ is not None and (self.getBarFilter() is None or self.getBarFilter().includeBar(bar_)):
                    bars.append(bar_)

            super(CustomCSVBarFeed, self).addBarsFromSequence(name, bars)

    def addBarsFromParquet(self, path, ticker=None, timezone=None):
        # Load the parquet file
        df = pd.read_parquet(path)
        self.addBarsFromDataframe(df, ticker, timezone)

    def addBarsFromParquets(self, dataFiles, ticker=None, startDate=None, endDate=None, timezone=None):
        self.addBarsFromDataframe(self.getDataFrameFromParquets(dataFiles, startDate, endDate), ticker, timezone)

    def getDataFrameFromParquets(self, dataFiles, startDate=None, endDate=None):
        df = None
        for files in dataFiles:
            for file in glob.glob(files):
                if df is None:
                    df = pd.read_parquet(file)
                else:
                    df = pd.concat([df, pd.read_parquet(file)],
                                   ignore_index=True)

        df = df.sort_values([self.__columnNames['ticker'], self.__columnNames['datetime']]).drop_duplicates(
            subset=[self.__columnNames['ticker'], self.__columnNames['datetime']], keep='first')

        if startDate:
            df = df[df[self.__columnNames['datetime']].dt.date >= startDate]
        if endDate:
            df = df[df[self.__columnNames['datetime']].dt.date <= endDate]

        return df

    def addBarsFromCSV(self, path, timezone=None, skipMalformedBars=False):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        :param skipMalformedBars: True to skip errors while parsing bars.
        :type skipMalformedBars: boolean.
        """
        def parse_bar_skip_malformed(row):
            ret = None, None
            try:
                ret = rowParser.parseBar(row)
            except Exception:
                pass
            return ret

        if timezone is None:
            timezone = self.__timezone

        rowParser = CustomRowParser(
            self.__columnNames, self.__dateTimeFormat, self.getDailyBarTime(), self.getFrequency(),
            timezone, self.__barClass
        )

        if skipMalformedBars:
            parse_bar = parse_bar_skip_malformed
        else:
            parse_bar = rowParser.parseBar

        # Load the csv file
        loadedBarsByInstrument = {}
        reader = csvutils.FastDictReader(open(
            path, "r"), fieldnames=rowParser.getFieldNames(), delimiter=rowParser.getDelimiter())
        for row in reader:
            instrument_, bar_ = parse_bar(row)
            if bar_ is not None and (self.getBarFilter() is None or self.getBarFilter().includeBar(bar_)):
                if instrument_ not in loadedBarsByInstrument:
                    loadedBarsByInstrument[instrument_] = []

                loadedBarsByInstrument[instrument_].append(bar_)

        # super(CustomCSVFeed, self).addBarsFromCSV(instrument, path, rowParser, skipMalformedBars=skipMalformedBars)
        for key, value in loadedBarsByInstrument.items():
            super(CustomCSVBarFeed, self).addBarsFromSequence(key, value)

        if rowParser.barsHaveAdjClose():
            self.__haveAdjClose = True
        elif self.__haveAdjClose:
            raise Exception(
                "Previous bars had adjusted close and these ones don't have.")


class CustomCSVFeed(CustomCSVBarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Quandl.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.MINUTE, timezone=None, maxLen=None):
        if frequency not in [bar.Frequency.MINUTE, bar.Frequency.DAY]:
            raise Exception("Invalid frequency")

        super(CustomCSVFeed, self).__init__(frequency, timezone, maxLen)
        self.setNoAdjClose()
