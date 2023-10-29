"""
.. moduleauthor:: Nagaraju Gunda
"""

import pandas as pd
from pyalgotrade import barfeed
from pyalgotrade import bar


class DataFrameFeed(barfeed.BaseBarFeed):
    def __init__(self, df: pd.DataFrame, tickers=[], startDate=None, endDate=None, frequency=bar.Frequency.MINUTE, maxLen=None):
        super(DataFrameFeed, self).__init__(frequency, maxLen)

        self.__df = df
        self.__nextPos = 0
        self.__currDateTime = None
        self.__frequency = frequency
        self.__haveAdjClose = False

        if df.empty:
            return

        for ticker in tickers:
            self.__df = self.__df[self.__df['Ticker'].str.startswith(ticker)]

        if startDate:
            self.__df = self.__df[self.__df['Date/Time'].dt.date >= startDate]
        if endDate:
            self.__df = self.__df[self.__df['Date/Time'].dt.date <= endDate]

        self.__df = self.__df.drop_duplicates(
            subset=['Ticker', 'Date/Time'], keep='first').sort_values(['Ticker', 'Date/Time'])

        self.__dateTimes = sorted(self.__df['Date/Time'].unique().tolist())
        self.__instruments = self.__df['Ticker'].unique().tolist()

        for instrument in self.__instruments:
            self.registerInstrument(instrument)

    def reset(self):
        self.__nextPos = 0
        self.__currDateTime = None
        super(DataFrameFeed, self).reset()

    def getApi(self):
        return None

    def barsHaveAdjClose(self):
        return self.__haveAdjClose

    def peekDateTime(self):
        return self.__dateTimes[self.__nextPos] if self.__nextPos < len(self.__dateTimes) else None

    def getCurrentDateTime(self):
        return self.__currDateTime

    def start(self):
        super(DataFrameFeed, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        return self.__nextPos >= len(self.__dateTimes)

    def getNextBars(self):

        currentDateTime = self.peekDateTime()

        if currentDateTime is None:
            return None

        ret = {}

        df = self.__df[self.__df['Date/Time']
                       == currentDateTime]

        columnIndexMapping = {columnName: df.columns.get_loc(
            columnName) + 1 for columnName in df.columns}

        for row in df.itertuples():
            ticker = row[columnIndexMapping['Ticker']]
            dateTime = row[columnIndexMapping['Date/Time']]
            open = row[columnIndexMapping['Open']]
            high = row[columnIndexMapping['High']]
            low = row[columnIndexMapping['Low']]
            close = row[columnIndexMapping['Close']]
            volume = row[columnIndexMapping['Volume']]
            openInterest = row[columnIndexMapping['Open Interest']]
            ret[ticker] = bar.BasicBar(dateTime, open, high, low, close, volume, None, self.__frequency, extra={
                                       'Open Interest': openInterest})

        self.__nextPos += 1
        self.__currDateTime = currentDateTime

        return bar.Bars(ret)
