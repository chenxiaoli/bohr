import logging
import pika
from urllib.parse import quote_plus
from time import sleep
from pymongo import MongoClient
import talib
import threading
import settings
from mongoengine import connect
from tradebucketed.trade_bucketed_indicator import MACDIndicator,EfiIndicator
from tradebucketed.trade_bucketed import TradeBucketed

# Basic use of websocket.

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


uri = "mongodb://%s:%s@%s" % (
    quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)

uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")

connect(username=settings.DB_USER, password=settings.DB_PASSWORD, host=uri2, port=27017, authentication_source='admin')

client = MongoClient(uri)
db = client["trade_data"]

def callback(ch, method, properties, body):
    logging.info("ch:%s,method:%s,properties:%s,body:%s" %(ch,method,properties,body))
    # symbol = "XBTUSD"
    # source_collection = db["bitmex_trade_bucketed"]
    # collection = db["trade_bucketed_indicator"]
    # node = "bitmex.com"
    # bin_size = "30m"
    # source = "close"
    # short = 13
    # long = 30
    # signal = 9
    # trade_bucketed_collection = TradeBucketed(source_collection, symbol=symbol, bin_size_list=())
    # trade_bucketed = trade_bucketed_collection.get_trade_bucketed_last_one(bin_size)
    # macd = MACDIndicator(source_collection, collection, node, symbol, bin_size, short=short, long=long, signal=signal,
    #                      source=source)
    # macd.create_MACD(trade_bucketed)
    # efi = EfiIndicator(source_collection, collection, node, symbol, bin_size="3m", length=2)
    # efi.create(trade_bucketed)

if __name__ == "__main__":
    setup_logger()
    credentials = pika.PlainCredentials(settings.MQ_USER, settings.MQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=settings.MQ_HOST, port=settings.MQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange='trade_bucketed', exchange_type='fanout')
    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='trade_bucketed', queue=queue_name)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, consumer_callback=callback)
    channel.start_consuming()



