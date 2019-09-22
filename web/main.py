import os
import sys
import isodate
from urllib.parse import quote_plus
from pymongo import MongoClient
import json
from flask_cors import CORS

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
from eve import Eve
from flask import request, after_this_request
from web import settings
from utils import serializer

uri = "mongodb://%s:%s@%s" % (
    quote_plus(settings.MONGO_USERNAME), quote_plus(settings.MONGO_PASSWORD), settings.MONGO_HOST)
client = MongoClient(uri)
db = client["trade_data"]

app = Eve()
CORS(app)


@app.route('/api/v1/data/trade/kline/<string:node>')
def trade_kline_data(node):
    collection = db["%s_trade_bucketed" % node]
    symbol = request.args.get("symbol")
    bin_size = request.args.get("binSize")
    start_time = request.args.get("startTime")
    end_time = request.args.get("endTime")
    count = request.args.get("count", 200)
    filters = {"symbol": symbol, "binSize": bin_size}
    sort = request.args.get("sort")
    if sort == "-timestamp":
        sort = -1
    else:
        sort = 1
    timestamp = {}
    if start_time:
        datetime = isodate.parse_datetime(start_time)
        timestamp.update({"$gte": datetime})
    if end_time:
        datetime = isodate.parse_datetime(end_time)
        timestamp.update({"$lte": datetime})
    if start_time or end_time:
        filters.update({"timestamp": timestamp})

    required_args = []
    if not symbol:
        required_args.append("symbol")
    if not bin_size:
        required_args.append("bin_size")
    if required_args:
        return "required argument: %s" % ",".join(required_args)
    print(filters)
    qs = collection.find(filters, {"symbol": 1, "binSize": 1, "timestamp": 1, "low": 1, "high": 1, "open": 1,
                                   "close": 1, "volume": 1}).sort("timestamp", sort).limit(count)
    items = []
    for item in qs:
        # 数据意义：开盘(open)，收盘(close)，最低(lowest)，最高(highest)
        print(item)
        values = [item["timestamp"], item["open"], item["close"], item["low"], item["high"], item["volume"]]
        items.append(values)
    # result.update({"_items": items})
    return json.dumps(items, cls=serializer.MongoEncoder)


@app.route('/api/v1/data/trade/bucketed/<string:node>')
def trade_buckted_data(node):
    collection = db["%s_trade_bucketed" % node]
    symbol = request.args.get("symbol")
    bin_size = request.args.get("binSize")
    start_time = request.args.get("startTime")
    end_time = request.args.get("endTime")
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("pageSize", 10))
    sort = request.args.get("sort")
    if sort == "-timestamp":
        sort = -1
    else:
        sort = 1

    filters = {"symbol": symbol, "binSize": bin_size}
    timestamp = {}
    if start_time:
        datetime = isodate.parse_datetime(start_time)
        timestamp.update({"$gte": datetime})
    if end_time:
        datetime = isodate.parse_datetime(end_time)
        timestamp.update({"$lte": datetime})
    if start_time or end_time:
        filters.update({"timestamp": timestamp})

    required_args = []
    if not symbol:
        required_args.append("symbol")
    if not bin_size:
        required_args.append("bin_size")
    if required_args:
        return "required argument: %s" % ",".join(required_args)
    print(filters)
    count = collection.count_documents(filters)
    qs = collection.find(filters, {"symbol": 1, "binSize": 1, "timestamp": 1, "low": 1, "high": 1, "open": 1,
                                   "close": 1, "volume": 1}).sort([
        ('timestamp', sort), ]).skip((page - 1) * page_size).limit(page_size)
    result = {"_meta": {"page": page, "pageSize": page_size, "total": count}}
    items = []
    for item in qs:
        items.append(item)
    result.update({"_items": items})
    return json.dumps(result, cls=serializer.MongoEncoder)


def pre_bitmex_trade_bucketed_get_callback(request, lookup):
    node = request.args.get("node")
    symbol = request.args.get("symbol")
    bin_size = request.args.get("binSize")
    start_time = request.args.get("startTime")
    end_time = request.args.get("endTime")
    if node:
        lookup["node"] = node
    if symbol:
        lookup["symbol"] = symbol
    if bin_size:
        lookup["binSize"] = bin_size
    timestamp = {}
    if start_time:
        datetime = isodate.parse_datetime(start_time)
        timestamp.update({"$gte": datetime})
    if end_time:
        datetime = isodate.parse_datetime(end_time)
        timestamp.update({"$lte": datetime})
    if start_time or end_time:
        lookup["timestamp"] = timestamp


app.on_pre_GET_bitmex_trade_bucketed += pre_bitmex_trade_bucketed_get_callback
if __name__ == '__main__':
    app.run(debug=True)
