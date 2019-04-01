import logging
from urllib.parse import quote_plus
from time import sleep
from pymongo import MongoClient
import talib

import settings

from tradebucketed.trade_bucketed_indicator import EMAIndicator

# Basic use of websocket.
def run():
    logger = setup_logger()

    # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.

    uri = "mongodb://%s:%s@%s" % (
        quote_plus("admin"), quote_plus("416211"), "192.168.1.2:27017")
    client = MongoClient(uri)
    collection=client["trade_data"]["bitmex_trade_bucketed"]
    symbol="XBTUSD"
    base_bin_size="1m"
    # trade_bucketed=TradeBucketed(collection,symbol,base_bin_size,settings.BIN_SIZE_LIST)
    # # Run forever
    # while True:
    #     trade_bucketed.increase_create_trade_bucketed()



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


if __name__ == "__main__":
    setup_logger()

    uri = "mongodb://%s:%s@%s" % (
        quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)
    client = MongoClient(uri)
    db=client["trade_data"]
    symbol = "XBTUSD"
    source_collection=db["bitmex_trade_bucketed"]
    collection = db["bitmex_trade_bucketed_EMA"]
    bin_size_list=(
        ("1m",(9,26)),
        ("3m",(9, 26)),
        ("5m", (9, 26)),
        ("15m", (9, 26)),
        ("30m", (9, 26)),
        ("1h", (9, 26)),
        ("2h", (9, 26)),
        ("4h", (9, 26)),
        ("12h", (9, 26)),
        ("1d", (9, 26)),
        ("2d", (9, 26)),
    )
    bin_sizes=dict(bin_size_list)
    source="close"
    for k,v in bin_sizes.items():
        bin_size=k
        lengths=v
        for length in lengths:
            indicator=EMAIndicator(source_collection=source_collection,
                                   collection=collection,
                                   symbol=symbol,
                                   bin_size=bin_size,
                                   length=length,
                                   source=source)
            indicator.increase_create(step=100)

