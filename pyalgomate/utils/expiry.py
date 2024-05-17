import pendulum
import datetime

from pyalgomate.core import UnderlyingIndex

listOfNseHolidays = set([
    pendulum.Date(2022, 1, 26),  # Republic Day
    pendulum.Date(2022, 3, 1),  # Maha Shivaratri
    pendulum.Date(2022, 3, 18),  # Holi
    pendulum.Date(2022, 4, 14),  # Dr.Baba Saheb Ambedkar Jayanti
    pendulum.Date(2022, 4, 15),  # Good friday
    pendulum.Date(2022, 5, 3),  # Id-ul-Fitr
    pendulum.Date(2022, 8, 9),  # Moharram
    pendulum.Date(2022, 8, 15),  # Independence Day
    pendulum.Date(2022, 8, 31),  # Ganesh Chaturthi
    pendulum.Date(2022, 10, 5),  # Vijaya Dashami
    pendulum.Date(2022, 10, 24),  # Diwali-Laxmi Pujan
    pendulum.Date(2022, 10, 26),  # Diwali-Balipratipada
    pendulum.Date(2022, 11, 8),   # Guru Nanak Jayanti

    pendulum.Date(2023, 1, 26),  # Republic Day
    pendulum.Date(2023, 3, 7),  # Holi
    pendulum.Date(2023, 3, 30),  # Ram Navami
    pendulum.Date(2023, 4, 4),  # Mahavir Jayanti
    pendulum.Date(2023, 4, 7),  # Good friday
    pendulum.Date(2023, 4, 14),  # Dr.Baba Saheb Ambedkar Jayanti
    pendulum.Date(2023, 4, 21),  # Id-ul-Fitr
    pendulum.Date(2023, 5, 1),  # Maharashtra Day
    pendulum.Date(2023, 6, 29),  # Id-ul-adha (Bakri Id)
    pendulum.Date(2023, 8, 15),  # Independence Day
    pendulum.Date(2023, 9, 19),  # Ganesh Chaturthi
    pendulum.Date(2023, 10, 2),  # Mahatma Gandhi Jayanti
    pendulum.Date(2023, 10, 24),  # Dussehra
    pendulum.Date(2023, 11, 14),  # Diwali Balipratipada
    pendulum.Date(2023, 11, 27),  # Gurunanak Jayanti
    pendulum.Date(2023, 12, 25),   # Christmas

    pendulum.Date(2024, 1, 26),  # Republic Day
    pendulum.Date(2024, 3, 8),  # Maha Shivaratri
    pendulum.Date(2024, 3, 25),  # Holi
    pendulum.Date(2024, 3, 29),  # Good Friday
    pendulum.Date(2024, 4, 11),  # Eid-Ul-Fitr (Ramzan Eid)
    pendulum.Date(2024, 4, 17),  # Ram Navami
    pendulum.Date(2024, 5, 1),  # Maharashtra Day
    pendulum.Date(2024, 5, 20),  # Election day
    pendulum.Date(2024, 6, 17),  # Bakri Eid
    pendulum.Date(2024, 7, 17),  # Moharram
    pendulum.Date(2024, 8, 15),  # Independence Day
    pendulum.Date(2024, 10, 2),  # Mahatma Gandhi Jayanti
    pendulum.Date(2024, 11, 1),  # Diwali-Laxmi Pujan*
    pendulum.Date(2024, 11, 15),  # Gurunanak Jayanti
    pendulum.Date(2024, 12, 25)   # Christmas
])

expiryDays = {
    UnderlyingIndex.NIFTY: {
        (datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.THURSDAY
        }
    },
    UnderlyingIndex.BANKNIFTY: {
        (datetime.date(1900, 1, 1), datetime.date(2023, 9, 2)): {
            "weekly": pendulum.THURSDAY
        },
        (datetime.date(2023, 9, 2), datetime.date(2024, 3, 18)): {
            "weekly": pendulum.WEDNESDAY,
            "monthly": pendulum.THURSDAY
        },
        (datetime.date(2024, 3, 18), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.WEDNESDAY,
            "monthly": pendulum.WEDNESDAY
        }
    },
    UnderlyingIndex.FINNIFTY: {
        (datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.TUESDAY
        }
    },
    UnderlyingIndex.MIDCPNIFTY: {
        (datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.MONDAY
        }
    },
    UnderlyingIndex.SENSEX: {
        (datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.FRIDAY
        }
    },
    UnderlyingIndex.BANKEX: {
        (datetime.date(1900, 1, 1), datetime.date(2100, 1, 1)): {
            "weekly": pendulum.MONDAY
        }
    }
}


def _getExpiryDay(date: datetime.date, index: UnderlyingIndex):
    expiryDay = expiryDays.get(index)

    if expiryDay:
        for date_range, settings in expiryDay.items():
            start_date, end_date = date_range
            if start_date <= date < end_date:
                if "monthly" in settings:
                    return settings["weekly"], settings["monthly"]
                else:
                    return settings["weekly"], settings["weekly"]
        else:
            raise ValueError("Index found, but no matching date range.")
    else:
        raise ValueError("Invalid index")


def __considerHolidayList(expiryDate: pendulum.Date) -> datetime.date:
    ret = None
    if (expiryDate in listOfNseHolidays) or (expiryDate.day_of_week in [pendulum.SATURDAY, pendulum.SUNDAY]):
        ret = __considerHolidayList(expiryDate.subtract(days=1))
    else:
        ret = expiryDate

    return datetime.date(ret.year, ret.month, ret.day)


def __isSameWeek(date1: datetime.date, date2: datetime.date):
    return date1.month == date2.month and pendulum.date(date1.year, date1.month, date1.day).week_of_month == pendulum.date(date2.year, date2.month, date2.day).week_of_month


def getNearestWeeklyExpiryDate(date: datetime.date = None, index: UnderlyingIndex = UnderlyingIndex.BANKNIFTY):
    currentDate = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDay, monthlyExpiryDay = _getExpiryDay(currentDate, index)

    while True:
        if currentDate.day_of_week == expiryDay:
            expiryDate = currentDate
        else:
            expiryDate = currentDate.next(expiryDay)

        expiryDate = __considerHolidayList(expiryDate)

        if expiryDay != monthlyExpiryDay:
            monthlyExpiryDate = getNearestMonthlyExpiryDate(date, index)
            if __isSameWeek(expiryDate, monthlyExpiryDate) or monthlyExpiryDate < expiryDate:
                expiryDate = monthlyExpiryDate

        if expiryDate >= datetime.date(currentDate.year, currentDate.month, currentDate.day):
            break

        currentDate = currentDate.add(days=1)

    return expiryDate


def getNextWeeklyExpiryDate(date: datetime.date = None, index: UnderlyingIndex = UnderlyingIndex.BANKNIFTY):
    currentDate = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDate = getNearestWeeklyExpiryDate(currentDate, index)

    return getNearestWeeklyExpiryDate(expiryDate + datetime.timedelta(days=5), index)


def getNearestMonthlyExpiryDate(date: datetime.date = None, index: UnderlyingIndex = UnderlyingIndex.BANKNIFTY):
    currentDate = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDay, monthlyExpiryDay = _getExpiryDay(currentDate, index)
    expiryDate = currentDate.last_of('month', monthlyExpiryDay)
    if (currentDate > expiryDate):
        expiryDate = currentDate.add(months=1).last_of(
            'month', monthlyExpiryDay)
    return __considerHolidayList(expiryDate)


def getNextMonthlyExpiryDate(date: datetime.date = None, index: UnderlyingIndex = UnderlyingIndex.BANKNIFTY):
    currentDate = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDay, monthlyExpiryDay = _getExpiryDay(currentDate, index)
    expiryDate = currentDate.last_of('month', monthlyExpiryDay)
    if (currentDate > expiryDate):
        expiryDate = currentDate.add(months=2).last_of(
            'month', monthlyExpiryDay)
    else:
        expiryDate = currentDate.add(months=1).last_of(
            'month', monthlyExpiryDay)

    return __considerHolidayList(expiryDate)


if __name__ == '__main__':
    print(f"Today is\t\t\t{pendulum.now().date()}\n")
    print()
    print(f"NIFTY\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.NIFTY)}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.NIFTY)}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.NIFTY)}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.NIFTY)}")
    print()
    print(f"BANKNIFTY\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date())}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date())}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date())}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date())}")
    print()
    print(f"MIDCPNIFTY\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.MIDCPNIFTY)}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.MIDCPNIFTY)}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.MIDCPNIFTY)}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.MIDCPNIFTY)}")
    print()
    print(f"FINNIFTY\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.FINNIFTY)}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.FINNIFTY)}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.FINNIFTY)}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.FINNIFTY)}")
    print()
    print(f"SENSEX\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.SENSEX)}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.SENSEX)}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.SENSEX)}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.SENSEX)}")
    print()
    print(f"BANKEX\n"
          f"Nearest Weekly expiry is\t{getNearestWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.BANKEX)}\n"
          f"Next Weekly expiry is\t\t{getNextWeeklyExpiryDate(pendulum.now().date(), UnderlyingIndex.BANKEX)}\n"
          f"Nearest Monthly expiry is\t{getNearestMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.BANKEX)}\n"
          f"Next Month expiry is\t\t{getNextMonthlyExpiryDate(pendulum.now().date(), UnderlyingIndex.BANKEX)}")
    print()
