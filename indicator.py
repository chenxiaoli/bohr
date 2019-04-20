import logging
import pika
from urllib.parse import quote_plus
from time import sleep
from pymongo import MongoClient
import threading
import settings
from mongoengine import connect
from tradebucketed.trade_bucketed_indicator import IndicatorSummary,get_indicator_instance
import queue
import json


uri = "mongodb://%s:%s@%s" % (
    quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)

uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")
client = MongoClient(uri)
db = client["trade_data"]
source_collection = db["bitmex_trade_bucketed"]
collection = db["trade_bucketed_indicator"]
summary = IndicatorSummary(db=db)
indicators_queue={}
indicator_handle_threads={}


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


def indicator_run_loop(item,instance):
    _queue = indicators_queue.get(item["key"])
    while True:
        if not _queue.empty():
            _item=_queue.get()
            logging.debug("get queue %s" % item["key"])
            instance.increase_create()
        logging.debug("%s heart beat" % item["key"])
        sleep(1)


def callback(ch, method, properties, body):
    _item=json.loads(body)
    logging.info("item:%s" % _item)
    _indicators = summary.fetch(node=_item["node"],symbol=_item["symbol"])
    logging.debug("indicator to generate :%s" % _indicators)
    for _indicator in _indicators:
        _queue=indicators_queue.get(_indicator["key"],None)
        if _queue:
            logging.info("put indicator to queue:%s" % _indicator)
            _queue.put(_item)
        else:
            indicators_queue[_indicator["key"]] = queue.Queue(maxsize=1000)
            _instance = get_indicator_instance(source_collection, collection, _indicator["name"], _indicator)
            t = threading.Thread(target=indicator_run_loop, args=(_indicator, _instance))
            t.setDaemon(True)
            indicator_handle_threads[_indicator["key"]] = t
            logging.info("queue %s not exist" % _indicator["key"])




# def run_etcd_watch():
#     client = etcd.Client(host=settings.ETCD_HOST, port=settings.ETCD_PORT)
#     indicators=client.read('/bohr/trade_bucketed_indicators').value
#     while True:
#         try:
#             indicators = client.read('/bohr/trade_bucketed_indicators', wait=True,timeout=3600).value
#         except etcd.EtcdWatchTimedOut:
#             logging.debug("except watch time out")
#         sleep(5)


def test_callback(ch, method, properties, body):
    logging.info("%s,%s,%s,%s" % (ch,method,properties,body))

if __name__ == "__main__":
    setup_logger()


    _indicators=summary.fetch(node="bitmex.com")

    for _indicator in _indicators:
        indicators_queue[_indicator["key"]]=queue.Queue(maxsize=1000)
        _instance=get_indicator_instance(source_collection,collection,_indicator["name"],_indicator)
        t = threading.Thread(target=indicator_run_loop, args=(_indicator,_instance))
        t.setDaemon(True)
        indicator_handle_threads[_indicator["key"]]=t

    for k,v in indicator_handle_threads.items():
        v.start()

    credentials = pika.PlainCredentials(settings.MQ_USER, settings.MQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=settings.MQ_HOST, port=settings.MQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange='trade_bucketed.topic', exchange_type='topic')

    result = channel.queue_declare("", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='trade_bucketed.topic',routing_key="bitmex.com", queue=queue_name)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=queue_name, on_message_callback=callback,auto_ack=True)
    channel.start_consuming()