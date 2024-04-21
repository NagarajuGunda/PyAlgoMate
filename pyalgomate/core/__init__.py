"""
.. moduleauthor:: Nagaraju Gunda
"""

from enum import IntEnum


class State(IntEnum):
    UNKNOWN = 0
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4
    SQUARING_OFF = 5
    MANUAL = 6

    def __str__(self):
        return self.name


class Expiry(IntEnum):
    WEEKLY = 1
    MONTHLY = 2

    def __str__(self):
        return self.name


class UnderlyingIndex(IntEnum):
    NIFTY = 0
    BANKNIFTY = 1
    FINNIFTY = 3
    MIDCPNIFTY = 4
    SENSEX = 5
    BANKEX = 6
    NOT_INDEX = 7

    def __str__(self):
        return self.name


if __name__ == "__main__":
    print(UnderlyingIndex.NIFTY)
    print(UnderlyingIndex.NIFTY.value)
