import pendulum
import datetime

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
    pendulum.Date(2023, 12, 25)   # Christmas
])


def getNearestWeeklyExpiryDate(date: datetime.date = None):
    currentData = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDate = None
    if (currentData.day_of_week is pendulum.THURSDAY):
        expiryDate = currentData
    else:
        expiryDate = currentData.next(pendulum.THURSDAY)
    return __considerHolidayList(expiryDate)


def getNextWeeklyExpiryDate(date: datetime.date = None):
    currentData = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDate = None
    if (currentData.day_of_week is pendulum.THURSDAY):
        expiryDate = currentData.next(pendulum.THURSDAY)
    else:
        expiryDate = currentData.next(
            pendulum.THURSDAY).next(pendulum.THURSDAY)
    return __considerHolidayList(expiryDate)


def getNearestMonthlyExpiryDate(date: datetime.date = None):
    currentData = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDate = currentData.last_of('month', pendulum.THURSDAY)
    if (currentData > expiryDate):
        expiryDate = currentData.add(months=1).last_of(
            'month', pendulum.THURSDAY)
    return __considerHolidayList(expiryDate)


def getNextMonthlyExpiryDate(date: datetime.date = None):
    currentData = pendulum.now().date() if date is None else pendulum.date(
        date.year, date.month, date.day)
    expiryDate = currentData.last_of('month', pendulum.THURSDAY)
    if (currentData > expiryDate):
        expiryDate = currentData.add(months=2).last_of(
            'month', pendulum.THURSDAY)
    else:
        expiryDate = currentData.add(months=1).last_of(
            'month', pendulum.THURSDAY)
    return __considerHolidayList(expiryDate)


# utility method to be used only by this module
def __considerHolidayList(expiryDate: pendulum.Date) -> datetime.date:
    ret = None
    if (expiryDate in listOfNseHolidays):
        ret = __considerHolidayList(expiryDate.subtract(days=1))
    else:
        ret = expiryDate

    return datetime.date(ret.year, ret.month, ret.day)


if __name__ == '__main__':
    print('Today is\t\t\t'+str(pendulum.now().date()))
    print('Nearest Weekly expiry is\t' +
          str(getNearestWeeklyExpiryDate(datetime.date(2023, 2, 9))))
    print('Next Weekly expiry is\t\t' +
          str(getNextWeeklyExpiryDate(datetime.date(2023, 2, 9))))
    print('Nearest Monthly expiry is\t' +
          str(getNearestMonthlyExpiryDate(datetime.date(2023, 2, 9))))
    print('Next Month expiry is\t\t' +
          str(getNextMonthlyExpiryDate(datetime.date(2023, 2, 9))))
