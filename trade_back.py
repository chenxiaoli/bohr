import logging
from urllib.parse import quote_plus
from pymongo import MongoClient
import settings
from tradeback.strategy import Strategy,TripleShortTrade
from tradeback.strategy import DataSource
from tradebucketed.trade_bucketed_indicator import IndicatorSummary


def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger



if __name__ == '__main__':
    setup_logger()

    buy_strategy_factors=[{
            "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"30m",
                },
            "apply":[("cross",("macd",(13,30,9,"close"),-1),0),("up",("macd",(13,30,9,"close"),-1)),("up",("ema",(8,"close"),-2),("ema",(8,"close"),-1))]
          },]
    buy_action_factors={
        "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"3m",
                },
        "apply": [("up",("efi", 2),2), "and", ("cross",("efi",2),0)]
    }

    uri = "mongodb://%s:%s@%s" % (
        quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)

    uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")
    client = MongoClient(uri)
    db = client["trade_data"]
    collection=db["trade_bucketed_indicator"]
    buy_strategy=Strategy(factors=buy_strategy_factors,collection=collection)
    buy=Strategy(factors=buy_action_factors,collection=collection)

    money=100
    max_loss=0

    sell_strategy_factors=[{
            "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"30m",
                },
            "apply":[("cross",("macd",12,30,9,"close"),0),"and",("down",("macd",12,30,9,"close"),2),"and",("up",("ema",8,"close"),2)]
          },]


    sell_action_factors = [{"method":"cross",
            "params":{"indicator": "efi", "params": {"binSize": "3m", "length": 2}},
            },
           {"method": "up",
            "params": {"indicator": "efi", "params": {"binSize": "3m", "length": 2}},
            }
           ]
    sell_strategy = Strategy(factors=sell_strategy_factors, collection=collection)
    sell=Strategy(factors=sell_action_factors,collection=collection)

    trade_bucketed_collection = db["bitmex_trade_bucketed"]

    # triple=TripleShortTrade(trade_bucketed_collection,buy_strategy,buy,sell_strategy,sell,money,max_loss)
    # triple.forward()
    # buy_strategy.apply()
    data_set={
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"30m",
                }

    indicators=[("ema",(9,"close")),("macd",(13,30,9,"close"))]
    for indicator in indicators:
        summary=IndicatorSummary(db=db,data_set=data_set,data=indicator)
        summary.add_or_update()

    ds=DataSource(collection=collection, node="bitmex.com", symbol="XBTUSD", bin_size=data_set["binSize"],indicators=indicators)



