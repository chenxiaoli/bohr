import isodate
import math
import time
import datetime
import logging
import pymongo
import numpy as np
import talib
import settings
from collections import OrderedDict

class TradeBucketedIndicator(object):
    def __init__(self,source_collection,db,symbol,bin_size_list):
        self._collection=source_collection
        self.symbol=symbol
        self.base_bin_size=settings.BASE_BIN_SIZE

    def _save_or_update(self,key,values):
        logging.info("save or update, key:%s,values:%s" % (key,values))
        self.collection.update_one(
            key,
            {"$set":values},
            upsert=True)

    def _last_one(self):
        _qq=self.collection.find(self.filter_keys).sort("timestamp",-1).limit(1)
        logging.info("get last one %s" % self.filter_keys)
        for i in _qq:
            return i

    def _last_source(self,):
        _qq=self.source_collection.find(self.source_filter_keys).sort("timestamp",-1).limit(1)
        for i in _qq:
            return i

class EMAIndicator(TradeBucketedIndicator):
    def __init__(self,source_collection,collection,symbol,bin_size,length,source):
        self.source_collection=source_collection
        self.symbol=symbol
        self.bin_size=bin_size
        self.length=length
        self.source=source
        self.collection=collection
        self.collection.create_index([
                             ("symbol", pymongo.ASCENDING),("binSize",1),("length",1),("source",1),("timestamp", pymongo.DESCENDING)],unique=True)
        if self.source not in ("open","close","high","low","volume"):
            raise Exception("unsuport source %s" % self.source)

        self.source_filter_keys = {"symbol": self.symbol, "binSize": self.bin_size }
        self.filter_keys={"symbol":self.symbol,"binSize":self.bin_size,"length":self.length,"source":self.source}

    def _find_source(self,end_time):
        """获取数据集"""
        logging.info("%s %s %s" % (self.symbol,end_time))
        rr=self._collection.find({"symbol":self.symbol,"binSize":self.bin_size,"timestamp":{"$lte":end_time}}).sort("timestamp",-1).limit(self.length)
        result=[]
        for r in rr:
            result.append(r[self.source])
        return result

    def _create_first_one(self):
        logging.info("create first one %s %s %s %s" % (self.symbol,self.bin_size,self.length,self.source))
        rr=self.source_collection.find({"symbol":self.symbol,"binSize":self.bin_size}).sort("timestamp",1).limit(self.length)
        result=[]
        _timestamps=[]
        for r in rr:
            result.append(r[self.source])
            _timestamps.append(r["timestamp"])
        sss=np.array(result)
        logging.debug("first one %s " % sss)
        output = talib.EMA(sss,self.length)
        price=output[-1]
        timestamp= _timestamps[-1]
        key = {"timestamp":timestamp, "symbol": self.symbol, "binSize": self.bin_size, "source": self.source,
               "length": self.length}
        self._save_or_update(key, {"price":price})
        return key.update({"price":price})

    def _create_one(self,trade_buketed_item):
        _timestamp=trade_buketed_item["timestamp"]
        key = {"timestamp": _timestamp, "symbol": self.symbol, "binSize": self.bin_size, "source": self.source,
               "length": self.length}
        today_price=0
        _last_ema= self._last_one()
        if _last_ema:
            K = 2 / (self.length + 1)
            _ema = today_price * K + _last_ema["price"] * (1 - K)
            price={"price":_ema}
            self._save_or_update(key,price)
        else:
            raise Exception("last one is none")

    def increase_create(self,step=1):
        done=False
        while not done:
            done=self._increase_create(step)

    def _increase_create(self,step=1):
        _done=False
        _last_source_item=self._last_source()
        _last_one_item=self._last_one()
        if not _last_one_item:
            logging.debug("need to create first one")
            self._create_first_one()
        elif _last_source_item["timestamp"]==_last_one_item["timestamp"]:
            self._create_one(_last_source_item)
            _done=True
        else:
            from_timestamp=_last_one_item["timestamp"]
            self._create_many_next(from_timestamp,step)
        return _done

    def _create_many_next(self,from_timestamp,step):
        rr=self.source_collection.find({"symbol":self.symbol,"binSize":self.bin_size,"timestamp":{"$gt":from_timestamp}}).sort("timestamp",1).limit(step)
        # result=[]
        for r in rr:
            self._create_one(r)



class MACDIndicator(TradeBucketedIndicator):
    def __init__(self,source_collection,collection,symbol,bin_size):
        self.source_collection=source_collection
        self.symbol=symbol
        self.bin_size=bin_size
        self.source_keys=("symbol","bin_size")
        self.indicator_keys=("fast","slow","signal","source")
        self.collection=collection
        self.collection.create_index([("timestamp", pymongo.DESCENDING),
                             ("symbol", pymongo.ASCENDING),("binSize",1),("length",1),("source",1)],unique=True)
        if self.source not in ("open","close","high","low","volume"):
            raise Exception("unsuport source %s" % self.source)

        self.source_filter_keys = {"symbol": self.symbol, "binSize": self.bin_size }
        self.filter_keys={"symbol":self.symbol,"binSize":self.bin_size,"length":self.length,"source":self.source}

    def cal_MACD(self):
        fast=None
        slow=None
        diff=None
        return fast,slow,diff