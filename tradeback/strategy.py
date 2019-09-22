from functools import reduce
import logging
from tradeback import operators
import copy
import time

from tradebucketed.trade_bucketed_indicator import IndicatorSummary


def flat_trade_bucketed(data, indicators):
    """把数据变成向量"""
    result = []
    t = time.mktime(data["timestamp"].timetuple())
    result.append(int(t))
    for indicator in indicators:
        name = indicator[0]
        if name == "macd":
            val = data[indicator[0]][0]["diff"] - data[indicator[0]][0]["dea"]
            result.append(val)
        elif name == "ema":
            val = data[name][0]["price"]
            result.append(val)
    print("float trade bucketed:", result)
    return tuple(result)


class DataSource(object):
    def __init__(self, collection, node, symbol, bin_size, indicators):
        self.collection = collection
        self.node = node
        self.symbol = symbol
        self.bin_size = bin_size
        self.indicators = indicators

    def fetch(self, batch_size=10, timestamp=None):
        _filter = {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size}
        _fields = {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, }
        if timestamp:
            _filter.update({"timestamp": {"$gte": timestamp}})
        for indicator in self.indicators:
            name = indicator[0]
            if name == "macd":
                elem_match = {"short": indicator[1][0], "long": indicator[1][1], "signal": indicator[1][2],
                              "source": indicator[1][3]}
            elif name == "ema":
                elem_match = {"length": indicator[1][0], "source": indicator[1][1]}
            _filter.update({name: {"$elemMatch": elem_match}})
            _fields.update({name: {"$elemMatch": elem_match}})
        data_set = self.collection.find(_filter, _fields).sort("timestamp", 1).limit(batch_size)
        for data in data_set:
            tensor = flat_trade_bucketed(data, self.indicators)
            yield tensor

    def first_timestamp(self):
        c = self.collection.find({"node": self.node, "symbol": self.symbol, "binSize": self.bin_size}).sort("timestamp",
                                                                                                            1).limit(1)
        for i in c:
            return i["timestamp"]

    def next_timestamp(self, timestamp):
        c = self.collection.find(
            {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size, "timestamp": {"$gt": timestamp}}).sort(
            "timestamp", 1).limit(1)
        for i in c:
            return i["timestamp"]

    def _get_macd_data(self, timestamp, args):
        """
        获取数据，并且处理数据成macd结构
        [{"diff":333,"dea":3334}]
        """
        length = abs(args[-1])
        _kk = {"node": self.node,
               "symbol": self.symbol,
               "binSize": self.bin_size,
               "macd": {"$elemMatch": {"short": args[0][0], "long": args[0][1], "signal": args[0][2],
                                       "source": args[0][3]}
                        }
               }
        fields = {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, "macd": {
            "$elemMatch": {"short": args[0][0], "long": args[0][1], "signal": args[0][2],
                           "source": args[0][3]}}}
        _kk.update({"timestamp": {"$lte": timestamp}})

        data = self.collection.find(_kk,
                                    fields,
                                    ).sort("timestamp", -1).limit(length)
        logging.debug("filter:%s,fields:%s" % (_kk, fields))

        result = []
        for item in data:
            logging.debug("%s,%s,%s" % (_kk, fields, item))
            result.append(item["macd"][0]["diff"] - item["macd"][0]["dea"])
        return result[args[-1]]

    def _get_efi_data(self, timestamp, args):
        params = args["params"]
        length = abs(args[-1])
        _kk = {"node": self.node,
               "symbol": self.symbol,
               "binSize": params["binSize"]

               }
        fields = {"node": 1, "symbol": 1, "binSize": 1, "timestamp": 1, "efi": {
            "$elemMatch": {"length": params["length"]}}}

        _kk.update({"timestamp": {"$lte": timestamp}})
        data = self.collection.find(_kk,
                                    fields,
                                    ).sort("timestamp", -1).limit(length)
        result = []
        for item in data:
            result.append(item["efi"][0]["ema"])
        return result


class BaseStrategy(object):
    def _indicactor_up(self, args):
        data = self.data_resource.fetch(timestamp=self.timestamp, args=args[0])
        if data and len(data) == 2:
            return operators.up(data)
        return False

    def _indicactor_cross(self, args):
        """cross("macd",(9,10,50,"close"))"""

        if type(args[0]) == tuple:
            data1 = self.data_resource.fetch(timestamp=self.timestamp, args=args[0])
        else:
            data1 = args[0]
        if type(args[1]) == tuple:
            data2 = self.data_resource.fetch(timestamp=self.timestamp, args=args[1])
        else:
            data2 = args[1]
        data = (data1, data2)
        return operators.cross(data)

    def _indicactor_down(self, args):
        if type(args[0]) == tuple:
            data1 = self.data_resource.fetch(timestamp=self.timestamp, args=args[0])
        else:
            data1 = args[0]
        if type(args[1]) == tuple:
            data2 = self.data_resource.fetch(timestamp=self.timestamp, args=args[1])
        else:
            data2 = args[1]
        data = (data1, data2)
        if data and len(data) == 2:
            return operators.down(data)
        return False

    def get_price(self, timestamp):
        self.data_resource.get_price(timestamp)


class Strategy(BaseStrategy):
    def __init__(self, factors, collection):
        self.factors = factors
        self.collection = collection
        self.timestamp = None

    def first_timestamp(self):
        return self.data_resource.first_timestamp()

    def apply(self, step=100, start_timestamp=None):
        for factor in self.factors:
            data_set = factor["dataSet"]
            self.data_resource = DataSource(collection=self.collection, node=data_set["node"],
                                            symbol=data_set["symbol"], bin_size=data_set["binSize"])
            factors = copy.copy(factor["apply"])
            factor_results = []
            for op in factors:
                factor_results.append(getattr(self, "_indicactor_%s" % op[0])(op[1:]))
            result = reduce(lambda x, y: x and y, factor_results)

    def walk(self, timestamp_from=None):
        fixed = []
        if timestamp_from:
            self.timestamp = self.data_resource.next_timestamp(timestamp_from)
        else:
            self.timestamp = self.data_resource.first_timestamp()
        while self.timestamp:
            result = True
            for condition in self.conditions:
                t = getattr(self, condition["method"])(**condition["params"])
                if not t:
                    result = False
                    continue
            if result:
                fixed.append(self.timestamp)
                return fixed
            self.timestamp = self.data_resource.next_timestamp(self.timestamp)


class BackTester(object):
    def __init__(self, strategy_entity,account):
        self.strategy_entity = strategy_entity
        self.indicators = {}
        self.timestamp = None
        self.account=account
        self._init_data_set()

    def _init_data_set(self):
        _factors = []
        _factors.extend(self.strategy_entity.buy_trend_factors)
        _factors.extend(self.strategy_entity.buy_creator_factors)
        _factors.extend(self.strategy_entity.sell_trend_factors)
        _factors.extend(self.strategy_entity.sell_creator_factors)
        _factors.extend(self.strategy_entity.stop_loss_factors)
        _factors.extend(self.strategy_entity.stop_profit_factors)
        self.data_set=[]
        for _factor in _factors:
            _indicators = _factor.get_indicators()
            self.data_set.extend(_indicators)






    def check_data_set(self, min_count=1000):
        pass

    # def prepare_data_set(self):
    #     for factors in buy_trend_factor.expressions:
    #         _data_set = factors["dataSet"]
    #         _exps = factors["apply"]
    #         for _exp in _exps:
    #             for _s in _exp:
    #                 if type(_s) is tuple and _s[0] in ("macd", "ema", "efi"):
    #                     _summary = IndicatorSummary(db=db, data_set=_data_set, data=_s)
    #                     _summary.add_or_update()

    def forward(self):
        self.timestamp = self.buy_strategy.first_timestamp()
        while self.timestamp:
            fixed = self.buy_strategy.walk(self.timestamp)
            if fixed:
                _buy = self.buy_factors.walk(timestamp_from=fixed[0])
                if _buy:
                    price = self.get_price(_buy[0])["close"]
                    self.buy(price)
                    sell_fixed = self.sell_strategy.walk(self.timestamp)
                    if sell_fixed:
                        _sell = self.sell_factors.walk(sell_fixed[0])
                        if _sell:
                            price = self.get_price(_sell[0])["close"]
                            self.sell(price)
                            self.timestamp = _sell[0]
            print("date:%s,money:%s,position:%s" % (self.timestamp, self.money, self.position))

    def get_price(self, timestamp):
        return self.trade_bucketed_collection.find_one({"symbol": "XBTUSD", "binSize": "3m", "timestamp": timestamp})

    def get_cut_loss_timestamp(self):
        pass

    def buy(self, price):
        self.position = self.money / price + self.position
        self.money = 0

    def sell(self, price):
        self.money = self.position * price
        self.position = 0
