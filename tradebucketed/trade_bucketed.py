import isodate
import math
import time
import datetime
import logging
import settings
import pytz
from collections import OrderedDict
class TradeBucketed(object):
    def __init__(self,collection,symbol,bin_size_list):
        self.collection=collection
        self.symbol=symbol
        self.base_bin_size=settings.BASE_BIN_SIZE
        self.bin_sizes=OrderedDict(bin_size_list)
    def _get_trade_bucketed_last_one(self,bin_size):
        cursor=self.collection.find({"symbol":self.symbol,"binSize":bin_size}).sort("timestamp", -1).limit(1)
        for doc in cursor:
            doc["timestamp"]=doc["timestamp"].replace(tzinfo=pytz.utc)

            return doc
    def _get_trade_bucketed_first_one(self,bin_size):
        cursor=self.collection.find({"symbol":self.symbol,"binSize":bin_size}).sort("timestamp", 1).limit(1)
        for doc in cursor:
            doc["timestamp"] = doc["timestamp"].replace(tzinfo=pytz.utc)
            return doc

    def increase_create_trade_bucketed(self):

        for k,v in self.bin_sizes.items():
            done = False
            while not done:
                done=self._increase_create_trade_bucketed(k,v)

    def find(self,bin_size,start_time,end_time):
        result=[]
        for item in self.collection.find({"symbol":self.symbol,"binSize":bin_size,"timestamp":{"$gte":start_time,"$lt": end_time}}):
            result.append(item)
        return result

    def save_or_update(self,item):
        self.collection.update_one(
            {"symbol": item["symbol"],"binSize":item["binSize"],"timestamp":item["timestamp"]},
            {"$set":item},
            upsert=True)

    def _end_time(self,start_time,bin_size,bin_size_params):
        """创建binSize期间结束的时间"""
        if bin_size=="1M":
            if start_time.month==12:
                end_time=start_time.replace(year=start_time.year+1,month=1,day=1)
            else:
                end_time = start_time.replace(month=start_time.month+1,day=1)
        elif bin_size=="1y":
            end_time=start_time.replace(year=start_time.year+1)
        elif bin_size=="1w":
            end_time=start_time + datetime.timedelta(days=7)
        else:
            end_time = start_time + datetime.timedelta(minutes=bin_size_params[1])
        return end_time

    def _create_first_time(self,bin_size,bin_size_params):
        """创建binsize第一条记录的时间"""
        _interval_minutes = bin_size_params[1]
        first = self._get_trade_bucketed_first_one(bin_size="1m")
        first_time = first["timestamp"]
        if bin_size=="1M":
            first_time=first_time.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        elif bin_size=="1y":
            first_time=first_time.replace(month=1,day=1, hour=0, minute=0, second=0, microsecond=0)
        elif bin_size =="1w":
            isoweekday=first_time.isoweekday()
            first_time=first_time - datetime.timedelta(days=isoweekday - 1)
            first_time=first_time.replace(hour=0, minute=0, second=0, microsecond=0)
        elif bin_size in ("1d","2d","3d","4d","5d","6d","7d","8d","9d","10d","15d"):
            first_time = first_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            minutes=first_time.timestamp()%(60*_interval_minutes)
            logging.debug(" first_time:%s, delta minutes:%s" % (first_time,minutes))
            first_time = first_time - datetime.timedelta(minutes=minutes)

        return first_time

    def _create_next_timestamp(self,start_time,bin_size,bin_size_params):
        """创建下一条记录的时间"""

        if bin_size=="1M":
            if start_time.month==12:
                start_time=start_time.replace(month=start_time.month+1,day=1,hour=0,minute=0,second=0,microsecond=0)
            else:
                start_time=start_time.replace(month=start_time.month+1,day=1,hour=0,minute=0,second=0,microsecond=0)
        elif bin_size=="1y":
            start_time=start_time.replace(year=start_time.year+1,month=1,day=1, hour=0, minute=0, second=0, microsecond=0)
        elif bin_size =="1w":
            start_time=start_time + datetime.timedelta(days=7)
            start_time=start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            if start_time.isoweekday()!=1:
                raise  Exception("1w的开始时期计算错误了。")
        else:
            _interval_minutes = bin_size_params[1]
            if _interval_minutes==0:
                raise Exception("binSize时间长度设置出错了。")
            start_time = start_time + datetime.timedelta(minutes=_interval_minutes)
        return start_time

    def _increase_create_trade_bucketed(self,bin_size,bin_size_params):
        logging.info("start create bin size %s params:%s" % (bin_size,bin_size_params))
        _done=True
        first=False #是否第一条记录
        start = self._get_trade_bucketed_last_one(bin_size=bin_size)
        if not start:
            start_time = self._create_first_time(bin_size,bin_size_params)
            first=True
        else:
            start_time = start["timestamp"]
        end_time=self._end_time(start_time,bin_size,bin_size_params)

        origin_bin_last_one = self._get_trade_bucketed_last_one(bin_size="1m")
        orgin_bin_last_one_time=origin_bin_last_one["timestamp"]
        if orgin_bin_last_one_time >= end_time and not first:
            start_time = self._create_next_timestamp(start_time=start_time,bin_size=bin_size,bin_size_params=bin_size_params)
            end_time=self._end_time(start_time,bin_size,bin_size_params)
        if orgin_bin_last_one_time>end_time:
            _done=False
        elif end_time>orgin_bin_last_one_time:
            return _done

        _base_bin_size=bin_size_params[0]
        items= self.find(bin_size=_base_bin_size,start_time=start_time,end_time=end_time)

        if len(items)==0:
            _msg="error at symbol:%s,binSize:%s,timestamp:%s,start time:%s,end_time:%s,base binSize:%s, bin_size_params:%s" % \
                 (self.symbol,bin_size,start_time,start_time,end_time,_base_bin_size,bin_size_params)
            logging.error(_msg)
            raise Exception(_msg)
        else:
            item=self._trade_bucketed_aggregation(items)
            item["timestamp"]=start_time
            item["symbol"]=self.symbol
            item["binSize"]=bin_size
        self.save_or_update(item)
        return _done


    def _trade_bucketed_aggregation(self,items):
        """"""
        high=items[0]["high"]
        low=items[0]["low"]
        open=items[0]["open"]
        close=items[-1]["close"]
        volume=0
        trades=0
        for item in items:
            if item["high"]>high:
                high=item["high"]
            if item["low"]<low:
                low=item["low"]
            volume=volume+item["volume"]
            trades=trades+item["trades"]
        return {"high":high,"low":low,"open":open,"close":close,"volume":volume,"trades":trades}

    def delete_many(self,bin_size):

        _items=list(self.bin_sizes.keys())
        index=list(self.bin_sizes.keys()).index(bin_size)
        for i in range(0,len(_items)):
            if i >= index:
                logging.info("delete bin size %s" % _items[i])
                self.collection.delete_many({"symbol":self.symbol,"binSize":bin_size})

class TradeBucketedChecker(object):
    def __init__(self,symbol,bin_size,first_time,last_time):
        self.symbol=symbol
        self.bin_size=bin_size
        self.first_time=first_time
        self.last_time=last_time
        self.delta=self.last_time -self.first_time

    def expected_count(self):
        """计算一段时间的bin size记录正确数量"""
        return getattr(self,"_expected_count_%s" % (self.bin_size))()

    def _expected_count_1m(self):
        return int((self.delta.days * 86400 + self.delta.seconds) / 60)
    def _expected_count_3m(self):
        return math.floor(self.delta.total_seconds() / (60 * 3))
    def _expected_count_5m(self):
        return math.floor(self.delta.total_seconds() / (60 * 5))
    def _expected_count_15m(self):
        return math.floor(self.delta.total_seconds()/ (60 * 15))
    def _expected_count_30m(self):
        return math.floor(self.delta.total_seconds() / (60 * 30))
    def _expected_count_1h(self):
        return math.floor(self.delta.total_seconds() / (60 * 60))
    def _expected_count_2h(self):
        return math.floor(self.deltatotal_seconds() / (60 * 60*2))
    def _expected_count_4h(self):
        return math.floor(self.delta.total_seconds() / (60 * 60*4))
    def _expected_count_6h(self):
        return math.floor(self.delta.total_seconds() / (60 * 60*6))
    def _expected_count_12h(self):
        return math.floor(self.delta.total_seconds()/ (60 * 60*12))
    def _expected_count_1d(self):
        return self.delta.days
    def _expected_count_5d(self):
        return self.delta.days/5
    def  _expected_count_1w(self):
        return self.delta.days / 7
    def  _expected_count_1M(self):
        _last_year=self.last_time.year
        _first_year=self.first_time.year
        _last_month=self.last_time.month
        _first_month=self.first_time.month
        _num = (_last_year - _first_year) * 12 + (_last_month - _first_month)
        return _num
    def  _expected_count_1y(self):
        return self.last_time.year-self.first_time.year

    def expected_list(self):
        return getattr(self, "_expected_list_%s" % (self.bin_size))()
    def _expected_list_1m(self):
        pass