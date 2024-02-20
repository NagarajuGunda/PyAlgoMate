import json
import datetime

from pyalgotrade import bar

class BasicBarEx(bar.Bar):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__adjClose',
        '__frequency',
        '__useAdjustedValue',
        '__extra',
    )

    def __init__(self, dateTime, open_, high, low, close, volume, adjClose, frequency, extra={}):
        if high < low:
            raise Exception("high < low on %s" % (dateTime))
        elif high < open_:
            raise Exception("high < open on %s" % (dateTime))
        elif high < close:
            raise Exception("high < close on %s" % (dateTime))
        elif low > open_:
            raise Exception("low > open on %s" % (dateTime))
        elif low > close:
            raise Exception("low > close on %s" % (dateTime))

        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__adjClose = adjClose
        self.__frequency = frequency
        self.__useAdjustedValue = False
        self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__adjClose,
            self.__frequency,
            self.__useAdjustedValue,
            self.__extra) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__adjClose,
            self.__frequency,
            self.__useAdjustedValue,
            self.__extra
        )

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted and self.__adjClose is None:
            raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__open / float(self.__close)
        else:
            return self.__open

    def getHigh(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__high / float(self.__close)
        else:
            return self.__high

    def getLow(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__low / float(self.__close)
        else:
            return self.__low

    def getClose(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose
        else:
            return self.__close

    def getVolume(self):
        return self.__volume

    def getAdjClose(self):
        return self.__adjClose

    def getFrequency(self):
        return self.__frequency

    def getPrice(self):
        if self.__useAdjustedValue:
            return self.__adjClose
        else:
            return self.__close

    def getExtraColumns(self):
        return self.__extra

    def getInstrument(self):
        return self.__extra.get('Instrument', None)

    def to_json(self):
        return json.dumps({
            'dateTime': self.__dateTime.strftime('%Y-%m-%d %H:%M:%S'),
            'open': self.__open,
            'high': self.__high,
            'low': self.__low,
            'close': self.__close,
            'volume': self.__volume,
            'adjClose': self.__adjClose,
            'frequency': self.__frequency,
            'useAdjustedValue': self.__useAdjustedValue,
            'extra': self.__extra
        })

    @classmethod
    def from_json(cls, json_data):
        # Deserialize JSON data to a dictionary
        data = json.loads(json_data)

        # Convert the string representation of datetime and frequency back to their original types
        data['dateTime'] = datetime.datetime.strptime(data['dateTime'], '%Y-%m-%d %H:%M:%S')
        data['frequency'] = data['frequency']

        # Create a BasicBarEx instance
        return cls(
            dateTime=data['dateTime'],
            open_=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close'],
            volume=data['volume'],
            adjClose=data['adjClose'],
            frequency=data['frequency'],
            extra=data['extra']
        )
