import tensorflow as  tf
import itertools
import logging
from urllib.parse import quote_plus
from pymongo import MongoClient
import settings

from tradeback.strategy import DataSource


if __name__ == "__main__":
    tf.enable_eager_execution()

    uri = "mongodb://%s:%s@%s" % (
        quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)
    uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")
    client = MongoClient(uri)
    db = client["trade_data"]
    collection=db["trade_bucketed_indicator"]
    data_source = DataSource(collection=collection, node="bitmex.com", symbol="XBTUSD", bin_size="30m",indicators=[("ema",(9,"close")),("macd",(13,30,9,"close"))])
    sess=tf.InteractiveSession()
    output_types=(tf.int64,tf.float64,tf.float64)
    ds=tf.data.Dataset.from_generator(
        data_source.fetch,
        output_types,
    )
    for value in ds.take(100):
        print(value)
    # sess.run(fetches=ds)
