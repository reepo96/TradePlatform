from datetime import datetime,timedelta
import math

def CurrentDate():
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    return (year * 10000 + month * 100 + day)

def CurrentTime():
    now = datetime.now()
    hour = now.hour
    min = now.min
    sec = now.second
    return (hour / 100 + min / 10000 + sec / 1000000)

def DateAdd(date,num):
    year = date / 10000
    mon = (date % 10000) / 100
    day = date % 100

    date = datetime(year,mon,day)
    new_date = date + timedelta(days=num)
    year = new_date.year
    month = new_date.month
    day = new_date.day
    return (year * 10000 + month * 100 + day)

def DateDiff(date1,date2):
    year1 = date1 / 10000
    mon1 = (date1 % 10000) / 100
    day1 = date1 % 100

    year2 = date2 / 10000
    mon2 = (date2 % 10000) / 100
    day2 = date2 % 100

    dt1 = datetime(year1, mon1, day1)
    dt2 = datetime(year2, mon2, day2)

    delta = dt2 - dt1
    return delta.days

def DateTimeToString(date_time,showMs):
    d, t = math.modf(date_time)
    d = int(d)
    year = d / 10000
    mon = (d % 10000) / 100
    day = d % 100
    hour = int( t*100)
    m = int( t*10000) % 100
    sec = int(t * 1000000) % 100
    msec = int(t * 1000000000) % 1000

    if showMs:
        return "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}".format(year,mon,day,hour,m,sec,msec)
    else:
        return "{}-{:02}-{:02} {:02}:{:02}:{:02}".format(year,mon,day,hour,m,sec)

def DateToString(date):
    year = date / 10000
    mon = (date % 10000) / 100
    day = date % 100

    return "{}-{:02}-{:02}".format(year, mon, day)

def DayFromDateTime(date_time):
    day = int(date_time)
    return day % 100

def HourFromDateTime(date_time):
    d, t = math.modf(date_time)
    hour = int(t * 100)
    return hour

def MilliSecondFromDateTime(date_time):
    d, t = math.modf(date_time)
    msec = int(t * 1000000000) % 1000
    return msec

def MinuteFromDateTime(date_time):
    d, t = math.modf(date_time)
    m = int( t*10000) % 100
    return m

def MinuteFromDateTime(date_time):
    d, t = math.modf(date_time)
    m = int( t*10000) % 100
    return m

def SecondFromDateTime(date_time):
    d, t = math.modf(date_time)
    sec = int(t * 1000000) % 100
    return sec

def MonthFromDateTime(date_time):
    d, t = math.modf(date_time)
    mon = (d % 10000) / 100
    return mon

def YearFromDateTime(date_time):
    d, t = math.modf(date_time)
    year = d / 10000
    return year

def SystemDateTime():
    now = datetime.now()

    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    m = now.min
    sec = now.second
    return (year * 10000 + month * 100 + day + hour / 100 + m / 10000 + sec / 1000000)
