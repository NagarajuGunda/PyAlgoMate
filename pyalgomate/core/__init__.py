"""
.. moduleauthor:: Nagaraju Gunda
"""

from enum import IntEnum


class UnderlyingIndex(IntEnum):
    NIFTY = 0
    BANKNIFTY = 1
    FINNIFTY = 3
    MIDCAPNIFTY = 4
    SENSEX = 5

    def __str__(self):
        return self.name


if __name__ == "__main__":
    print(UnderlyingIndex.NIFTY)
    print(UnderlyingIndex.NIFTY.value)
