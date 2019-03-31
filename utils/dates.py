import datetime
import pytz
def first_isoweek_datetime(date_time):
    isoweekday = date_time.isoweekday()
    first_time = date_time - datetime.timedelta(days=isoweekday - 1)
    return first_time

if __name__ == "__main__":
    date_time=datetime.datetime(2017,1,1).replace(tzinfo=pytz.timezone(("UTC")))
    first_date=first_isoweek_datetime(date_time)
    next_first_date=first_date+datetime.timedelta(days=7)
    print("%s weekday %s, %s is %s" %(date_time,date_time.isoweekday(),first_date,first_date.isoweekday()))
    print("next_first_date %s, week day is %s" % (next_first_date, next_first_date.isoweekday()))