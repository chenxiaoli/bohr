import isodate
import math
import time
import datetime
import logging
import pymongo
import numpy as np
import talib
import settings
import pytz
from tradebucketed.trade_bucketed import BinSizeTimestamp


class TradeBucketedIndicator(object):
    def __init__(self, source_collection, db, symbol, bin_size_list):
        self._collection = source_collection
        self.symbol = symbol
        self.base_bin_size = settings.BASE_BIN_SIZE

    def _save_or_update(self, key, values):
        logging.info("save or update, key:%s,values:%s" % (key, values))
        self.collection.update_one(
            key,
            {"$set": values},
            upsert=True)

    def _last_one(self):
        _qq = self.collection.find(self.filter_keys).sort("timestamp", -1).limit(1)
        logging.info("get last one %s" % self.filter_keys)
        for i in _qq:
            return i

    def _last_source(self, ):
        _qq = self.source_collection.find(self.source_filter_keys).sort("timestamp", -1).limit(1)
        for i in _qq:
            return i

    def get_EMA(self, yesterday_price, today_price, length):
        K = 2 / (length + 1)
        _ema = today_price * K + yesterday_price * (1 - K)
        return _ema


class EMAIndicator(TradeBucketedIndicator):
    def __init__(self, source_collection, collection, node, symbol, bin_size, length, source):
        self.source_collection = source_collection
        self.node = node
        self.symbol = symbol
        self.bin_size = bin_size
        self.length = length
        self.source = source
        self.collection = collection
        self.collection.create_index([
            ("node", pymongo.ASCENDING), ("symbol", pymongo.ASCENDING), ("binSize", 1), ("length", 1), ("source", 1),
            ("timestamp", pymongo.DESCENDING)], unique=True)
        if self.source not in ("open", "close", "high", "low", "volume"):
            raise Exception("unsuport source %s" % self.source)

        self.source_filter_keys = {"symbol": self.symbol, "binSize": self.bin_size}
        self.filter_keys = {"symbol": self.symbol, "binSize": self.bin_size, "length": self.length,
                            "source": self.source}

    def _find_source(self, end_time):
        """获取数据集"""
        logging.info("%s %s %s" % (self.symbol, end_time))
        rr = self._collection.find(
            {"symbol": self.symbol, "binSize": self.bin_size, "timestamp": {"$lte": end_time}}).sort("timestamp",
                                                                                                     -1).limit(
            self.length)
        result = []
        for r in rr:
            result.append(r[self.source])
        return result



    def _create_first_one(self):
        logging.info("create first one %s %s %s %s" % (self.symbol, self.bin_size, self.length, self.source))
        rr = self.source_collection.find({"symbol": self.symbol, "binSize": self.bin_size}).sort("timestamp", 1).limit(
            self.length)
        result = []
        _timestamps = []
        for r in rr:
            result.append(r[self.source])
            _timestamps.append(r["timestamp"])
        sss = np.array(result)
        logging.debug("first one %s " % sss)
        output = talib.EMA(sss, self.length)
        price = output[-1]
        timestamp = _timestamps[-1]
        key = {"timestamp": timestamp, "symbol": self.symbol, "binSize": self.bin_size, "source": self.source,
               "length": self.length}
        self._save_or_update(key, {"price": price})
        return key.update({"price": price})

    def _create_one(self, trade_buketed_item):
        _timestamp = trade_buketed_item["timestamp"]
        key = {"timestamp": _timestamp, "symbol": self.symbol, "binSize": self.bin_size, "source": self.source,
               "length": self.length}
        today_price = 0
        _last_ema = self._last_one()
        if _last_ema:
            K = 2 / (self.length + 1)
            _ema = today_price * K + _last_ema["price"] * (1 - K)
            price = {"price": _ema}
            self._save_or_update(key, price)
        else:
            raise Exception("last one is none")

    def increase_create(self, step=1):
        done = False
        while not done:
            done = self._increase_create(step)

    def _increase_create(self, step=1):
        _done = False
        _last_source_item = self._last_source()
        _last_one_item = self._last_one()
        if not _last_one_item:
            logging.debug("need to create first one")
            self._create_first_one()
        elif _last_source_item["timestamp"] == _last_one_item["timestamp"]:
            self._create_one(_last_source_item)
            _done = True
        else:
            from_timestamp = _last_one_item["timestamp"]
            self._create_many_next(from_timestamp, step)
        return _done

    def _create_many_next(self, from_timestamp, step):
        rr = self.source_collection.find(
            {"symbol": self.symbol, "binSize": self.bin_size, "timestamp": {"$gt": from_timestamp}}).sort("timestamp",
                                                                                                          1).limit(step)
        # result=[]
        for r in rr:
            self._create_one(r)


class MACDIndicator(TradeBucketedIndicator):
    def __init__(self, source_collection, collection, node, symbol, bin_size, short, long, signal, source):
        self.source_collection = source_collection
        self.node = node
        self.symbol = symbol
        self.bin_size = bin_size
        self.source_keys = ("symbol", "bin_size")
        self.indicator_keys = ("short", "long", "signal", "source")
        self.collection = collection
        self.short = short
        self.long = long
        self.signal = signal
        self.source = source
        self.collection.create_index([
            ("symbol", pymongo.ASCENDING), ("binSize", 1), ("timestamp", pymongo.DESCENDING), ], unique=True)
        if self.source not in ("open", "close", "high", "low", "volume"):
            raise Exception("unsuport source %s" % self.source)

        self.source_filter_keys = {"symbol": self.symbol, "binSize": self.bin_size}
        self.filter_keys = {"symbol": self.symbol, "binSize": self.bin_size}

    def create_MACD(self, trade_bucketed):
        _last_macd_item = self.last_macd()
        if not _last_macd_item or _last_macd_item["timestamp"] < trade_bucketed["timestamp"]:
            self.increase_history(_last_macd_item, trade_bucketed)
        elif _last_macd_item["timestamp"] == trade_bucketed["timestamp"]:
            pre_timestamp=BinSizeTimestamp().get_prev_timestamp(_last_macd_item["timestamp"],self.bin_size)
            pre_one=self.find_one(pre_timestamp)
            self._create_one(pre_one,trade_bucketed)

    def find_one(self,timestamp):
        _one = self.collection.find_one(
            {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size, "timestamp": timestamp},
            {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, "macd": {
                "$elemMatch": {"short": self.short, "long": self.long, "signal": self.signal,
                               "source": self.source}}}
        )
        return _one


    def macd_push(self, key, values):
        trade_buccketed_key = dict(key)
        macd_key = {
            "$elemMatch": {"short": values["short"], "long": values["long"], "signal": values["signal"],
                           "source": values["source"]}
        }
        key.update({"macd": macd_key})
        _cursor = self.collection.find(key)
        exit = None
        for i in _cursor:
            exit = i
        if exit:
            return self.macd_update(key, values)
        else:
            return self.macd_addToSet(trade_buccketed_key, values)

    def macd_addToSet(self, key, values):

        result= self.collection.update_one(
            key,
            {"$addToSet": {"macd": values},},
            upsert=True)
        return result

    def macd_update(self, key, values):
        result = self.collection.update_one(
            key
            ,
            {"$set": {
                "macd.$.short": values["short"],
                "macd.$.long": values["long"],
                "macd.$.signal": values["signal"],
                "macd.$.source": values["source"],
                "macd.$.shortEma": values["shortEma"],
                "macd.$.longEma": values["longEma"],
                "macd.$.diff": values["diff"],
                "macd.$.dea": values["dea"]},
            },
            upsert=True)
        return result

    def increase_history(self, last_macd, trade_bucketed):
        done = False
        while not done:
            if last_macd:
                last_macd = self.create_next(last_macd, step=500)
            else:
                last_macd = self.create_first_one()
            delta=trade_bucketed["timestamp"] - last_macd["timestamp"].replace(tzinfo=pytz.utc)
            print(delta)
            if delta.total_seconds()<=0:
                done = True
        return last_macd

    def last_macd(self):
        _cursor = self.collection.find({"node": self.node, "symbol": self.symbol, "binSize": self.bin_size,
                                        "macd":
                                            {"$elemMatch": {"short": self.short, "long": self.long,
                                                            "signal": self.signal,
                                                            "source": self.source}}},
                                       {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, "macd": {
                                           "$elemMatch": {"short": self.short, "long": self.long, "signal": self.signal,
                                                          "source": self.source}}}
                                       ).sort("timestamp", -1).limit(1)

        for doc in _cursor:
            doc["timestamp"] = doc["timestamp"].replace(tzinfo=pytz.utc)
            return doc

    def create_first_one(self):
        """使用numpy创建第一条记录"""
        logging.info("create first macd one %s %s %s " % (self.symbol, self.bin_size, self.source))
        datas = self.source_collection.find({"symbol": self.symbol, "binSize": self.bin_size}).sort("timestamp",
                                                                                                    1).limit(
            self.long + self.signal)
        result = []
        _timestamps = []
        for r in datas:
            result.append(r[self.source])
            _timestamps.append(r["timestamp"])

        arry = np.array(result)
        logging.debug("first one %s " % arry)
        macd, signal, hist = talib.MACD(arry, fastperiod=self.short, slowperiod=self.long, signalperiod=self.signal)
        short_ema = talib.EMA(arry, self.short)
        long_ema = talib.EMA(arry, self.long)

        key = {
            "node": self.node,
            "symbol": self.symbol,
            "binSize": self.bin_size,
            "timestamp": _timestamps[-1],
        }
        values = {
            "short": self.short,
            "long": self.long,
            "signal": self.signal,
            "source": self.source,
            "shortEma": short_ema[-1],
            "longEma": long_ema[-1],
            "diff": macd[-1],
            "dea": signal[-1],
        }
        self.macd_push(key, values)
        key.update(key)
        macd_list=[values,]
        key.update({"macd":macd_list})
        return key

    def create_next(self, last_one, step=200):
        from_timestamp = last_one["timestamp"]
        rr = self.source_collection.find(
            {"symbol": self.symbol, "binSize": self.bin_size, "timestamp": {"$gt": from_timestamp}}).sort("timestamp",
                                                                                                          1).limit(step)
        for trade_bucketed in rr:
            last_one = self._create_one(last_one, trade_bucketed)
        return last_one

    def _create_one(self, last_one, trade_bucketed):
        print("last one---------------------------:", last_one)
        _last_macd=last_one["macd"][0]
        today_price = trade_bucketed[_last_macd["source"]]
        key = {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size,
               "timestamp": trade_bucketed["timestamp"]}
        _macd = self._get_MACD(_last_macd, today_price)
        _macd.update({"short": _last_macd["short"], "long": _last_macd["long"], "signal": _last_macd["signal"],
                      "source": _last_macd["source"]})
        self.macd_push(key, _macd)
        last_one.update(key)
        macd_list=[_macd,]
        last_one.update({"macd":macd_list})
        return last_one

    def _get_MACD(self, last_one, today_price):
        yesterday_short_ema = last_one["shortEma"]
        yesterday_long_ema = last_one["longEma"]
        short = last_one["short"]
        short_ema = self.get_EMA(yesterday_short_ema, today_price, short)
        long_ema = self.get_EMA(yesterday_long_ema, today_price, last_one["long"])
        diff = short_ema - long_ema
        dea = self.get_EMA(last_one["dea"], diff, last_one["signal"])
        return {"shortEma": short_ema, "longEma": long_ema, "diff": diff, "dea": dea}


class EfiIndicator(TradeBucketedIndicator):

    def __init__(self, source_collection, collection, node, symbol, bin_size,length):
        self.source_collection = source_collection
        self.node = node
        self.symbol = symbol
        self.bin_size = bin_size
        self.length=length
        self.source_keys = ("symbol", "bin_size")
        self.indicator_keys = ("short", "long", "signal", "source")
        self.collection = collection
        self.collection.create_index([("node", pymongo.ASCENDING),
            ("symbol", pymongo.ASCENDING), ("binSize", 1), ("timestamp", pymongo.DESCENDING), ], unique=True)

        self.source_keys = {"node":self.node,"symbol": self.symbol, "binSize": self.bin_size}
        self.keys = {"node":self.node,"symbol": self.symbol, "binSize": self.bin_size,"efi":{"$elemMatch": {"length":self.length }}}

    def _get_EFI(self,pre_one,trade_bucketed):
        today_price=trade_bucketed["volume"]*(trade_bucketed["close"]-pre_one["close"])
        _ema=self.get_EMA(pre_one["ema"],today_price,pre_one["length"])
        return {"length":pre_one["length"],"ema":_ema,"close":trade_bucketed["close"]}

    def create_first_one(self):
        """使用numpy创建第一条记录"""
        logging.info("create first efi one %s %s " % (self.symbol, self.bin_size))
        datas = self.source_collection.find({"symbol": self.symbol, "binSize": self.bin_size}).sort("timestamp",
                                                                                                    1).limit(self.length)
        result = []
        _timestamps = []
        for r in datas:
            result.append(r["close"])
            _timestamps.append(r["timestamp"])
        arry = np.array(result)
        logging.debug("first one %s " % arry)
        _ema = talib.EMA(arry, self.length)
        efi={"length":self.length,"ema":_ema[-1],"close":result[-1]}
        trade_bucketed_key={"node":self.node,"symbol":self.symbol,"binSize":self.bin_size,"timestamp":_timestamps[-1]}
        self.efi_push(trade_bucketed_key,efi)

        first_one={"node":self.node,"symbol":self.symbol,"binSize":self.bin_size,"timestamp":_timestamps[-1]}
        first_one.update({"efi":[efi,]})
        return first_one

    def create_next(self, last_one, step=200):
        from_timestamp = last_one["timestamp"]
        rr = self.source_collection.find(
            {"symbol": self.symbol, "binSize": self.bin_size, "timestamp": {"$gt": from_timestamp}}).sort("timestamp",
                                                                                                          1).limit(step)
        for trade_bucketed in rr:
            last_one = self._create_one(last_one, trade_bucketed)
        return last_one

    def _create_one(self, last_one, trade_bucketed):
        print("last one---------------------------:", last_one)
        _last_efi=last_one["efi"][0]
        key = {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size,
               "timestamp": trade_bucketed["timestamp"]}
        _efi = self._get_EFI(_last_efi, trade_bucketed)
        self.efi_push(key, _efi)
        last_one.update(key)
        _list=[_efi,]
        last_one.update({"efi":_list})
        return last_one

    def efi_push(self, key, values):
        trade_buccketed_key = dict(key)
        efi_key = {
            "$elemMatch": {"length":values["length"]}
        }
        key.update({"efi": efi_key})
        _cursor = self.collection.find(key)
        exit = None
        for i in _cursor:
            exit = i
        if exit:
            return self.efi_update(key, values)
        else:
            return self.efi_addToSet(trade_buccketed_key, values)

    def efi_addToSet(self, key, values):

        result= self.collection.update_one(
            key,
            {"$addToSet": {"efi": values},},
            upsert=True)
        return result

    def efi_update(self, key, values):
        result = self.collection.update_one(
            key
            ,
            {"$set": {
                "efi.$.short": values["short"],
                "efi.$.long": values["long"],
                "efi.$.signal": values["signal"],
                "efi.$.source": values["source"],
                "efi.$.shortEma": values["shortEma"],
                "efi.$.longEma": values["longEma"],
                "efi.$.diff": values["diff"],
                "efi.$.dea": values["dea"]},
            },
            upsert=True)
        return result

    def increase_history(self, last_one, trade_bucketed):
        done = False
        while not done:
            if last_one:
                last_one = self.create_next(last_one, step=500)
            else:
                last_one = self.create_first_one()
            delta=trade_bucketed["timestamp"] - last_one["timestamp"].replace(tzinfo=pytz.utc)
            print(delta)
            if delta.total_seconds()<=0:
                done = True
        return last_one


    def create(self, trade_bucketed):
        _lastindicator = self.last_one()
        if not _lastindicator or _lastindicator["timestamp"] < trade_bucketed["timestamp"]:
            self.increase_history(_lastindicator, trade_bucketed)
        elif _lastindicator["timestamp"] == trade_bucketed["timestamp"]:
            pre_timestamp=BinSizeTimestamp().get_prev_timestamp(_lastindicator["timestamp"],self.bin_size)
            pre_one=self.find_one(pre_timestamp)
            self._create_one(pre_one,trade_bucketed)


    def last_one(self):
        _cursor = self.collection.find({"node": self.node,
                                        "symbol": self.symbol,
                                        "binSize": self.bin_size,
                                        "efi":{"$elemMatch": {"length": self.length}}},
                                       {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, "efi": {
                                           "$elemMatch": {"length": self.length}}}
                                       ).sort("timestamp", -1).limit(1)

        for doc in _cursor:
            doc["timestamp"] = doc["timestamp"].replace(tzinfo=pytz.utc)
            return doc