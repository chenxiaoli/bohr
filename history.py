import sys
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from urllib.parse import quote_plus
import time
from time import sleep
import pymongo
import requests
import isodate
import datetime
import json
import logging
from tradebucketed.trade_bucketed import TradeBucketed
import settings
BITMEX_TRADE_BUCKETED="bitmex_trade_bucketed"

import pika
import sys

credentials = pika.PlainCredentials("bitmex", "416211")
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=settings.MQ_HOST, port=5672, credentials=credentials)
)

channel = connection.channel()
channel.exchange_declare(exchange='trade_bucketed', exchange_type='fanout')

class HttpBitmex(object):
    """BitMEX API Connector."""

    def __init__(self, db,base_url, symbol,
                 bin_size, count=750, apiKey=None,apiSecret=None,postOnly=False,timeout=7):
        """Init connector."""
        self.logger = logging.getLogger('root')
        self.base_url = base_url
        self.symbol = symbol
        self.postOnly = postOnly
        self.timeout=timeout
        self.count=count
        self.bin_size=bin_size
        self.session = requests.Session()
        # These headers are always sent
        self.session.headers.update({'content-type': 'application/json'})
        self.session.headers.update({'accept': 'application/json'})
        self.db=db
        self.collection=self.db[BITMEX_TRADE_BUCKETED]
        self.db[BITMEX_TRADE_BUCKETED].create_index([("symbol",1),("binSize",1),("timestamp",-1)], unique= True)
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self._trade_bucketed=TradeBucketed(collection=self.db[BITMEX_TRADE_BUCKETED],symbol=self.symbol,bin_size_list=settings.BIN_SIZE_LIST)


    def _curl_bitmex(self, path, query=None, postdict=None, timeout=None, verb=None, rethrow_errors=False,
                     max_retries=None):
        """Send a request to BitMEX Servers."""
        # Handle URL
        url = self.base_url + path

        if timeout is None:
            timeout = self.timeout

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if postdict else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        # Auth: API Key/Secret
        # auth = APIKeyAuthWithExpires(self.apiKey, self.apiSecret)
        auth=None

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(postdict or '')))
            return self._curl_bitmex(path, query, postdict, timeout, verb, rethrow_errors, max_retries)

        # Make the request
        response = None
        try:
            self.logger.info("sending req to %s: %s" % (url, json.dumps(postdict or query or '')))
            req = requests.Request(verb, url, json=postdict, auth=auth, params=query)
            prepped = self.session.prepare_request(req)
            response = self.session.send(prepped, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if response is None:
                raise e

            # 401 - Auth error. This is fatal.
            if response.status_code == 401:
                self.logger.error("API Key or Secret incorrect, please check and restart.")
                self.logger.error("Error: " + response.text)
                if postdict:
                    self.logger.error(postdict)
                # Always exit, even if rethrow_errors, because this is fatal
                exit(1)

            # 404, can be thrown if order canceled or does not exist.
            elif response.status_code == 404:
                if verb == 'DELETE':
                    self.logger.error("Order not found: %s" % postdict['orderID'])
                    return
                self.logger.error("Unable to contact the BitMEX API (404). " +
                                  "Request: %s \n %s" % (url, json.dumps(postdict)))
                exit_or_throw(e)

            # 429, ratelimit; cancel orders & wait until X-RateLimit-Reset
            elif response.status_code == 429:
                self.logger.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " +
                                  "order pairs or contact support@bitmex.com to raise your limits. " +
                                  "Request: %s \n %s" % (url, json.dumps(postdict)))

                # Figure out how long we need to wait.
                ratelimit_reset = response.headers['X-RateLimit-Reset']
                to_sleep = int(ratelimit_reset) - int(time.time())
                reset_str = datetime.datetime.fromtimestamp(int(ratelimit_reset)).strftime('%X')

                # We're ratelimited, and we may be waiting for a long time. Cancel orders.
                self.logger.warning("Canceling all known orders in the meantime.")
                self.cancel([o['orderID'] for o in self.open_orders()])

                self.logger.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return retry()

            # 503 - BitMEX temporary downtime, likely due to a deploy. Try again
            elif response.status_code == 503:
                self.logger.warning("Unable to contact the BitMEX API (503), retrying. " +
                                    "Request: %s \n %s" % (url, json.dumps(postdict)))
                time.sleep(3)
                return retry()

            elif response.status_code == 400:
                error = response.json()['error']
                message = error['message'].lower() if error else ''

                # Duplicate clOrdID: that's fine, probably a deploy, go get the order(s) and return it
                if 'duplicate clordid' in message:
                    orders = postdict['orders'] if 'orders' in postdict else postdict

                    IDs = json.dumps({'clOrdID': [order['clOrdID'] for order in orders]})
                    orderResults = self._curl_bitmex('/order', query={'filter': IDs}, verb='GET')

                    for i, order in enumerate(orderResults):
                        if (
                                order['orderQty'] != abs(postdict['orderQty']) or
                                order['side'] != ('Buy' if postdict['orderQty'] > 0 else 'Sell') or
                                order['price'] != postdict['price'] or
                                order['symbol'] != postdict['symbol']):
                            raise Exception('Attempted to recover from duplicate clOrdID, but order returned from API ' +
                                            'did not match POST.\nPOST data: %s\nReturned order: %s' % (
                                                json.dumps(orders[i]), json.dumps(order)))
                    # All good
                    return orderResults

                elif 'insufficient available balance' in message:
                    self.logger.error('Account out of funds. The message: %s' % error['message'])
                    exit_or_throw(Exception('Insufficient Funds'))


            # If we haven't returned or re-raised yet, we get here.
            self.logger.error("Unhandled Error: %s: %s" % (e, response.text))
            self.logger.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(postdict)))
            exit_or_throw(e)

        except requests.exceptions.Timeout as e:
            # Timeout, re-run this request
            self.logger.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(postdict or '')))
            return retry()

        except requests.exceptions.ConnectionError as e:
            self.logger.warning("Unable to contact the BitMEX API (%s). Please check the URL. Retrying. " +
                                "Request: %s %s \n %s" % (e, url, json.dumps(postdict)))
            time.sleep(1)
            return retry()

        # Reset retry counter on success
        self.retries = 0

        return response.json()

    def get_trade_bucketed(self,start=None,start_time=None,end_time=None,count=0,reverse="false"):
        """binSize=1d&partial=false&symbol=XBTUSD&count=100"""
        _count=self.count
        if count>0:
            _count=count
        query={"binSize":self.bin_size,"partial":'false',"symbol":self.symbol,'count':_count,"start":start,"startTime":start_time,"endTime":end_time,"reverse":reverse}
        respone=self._curl_bitmex( "/api/v1/trade/bucketed", query=query, postdict=None, timeout=None, verb=None)
        return respone


    def count_db_trade_bucketed(self,start_time,end_time):
        return self.collection.find({"symbol":self.symbol,"binSize":self.bin_size,"timestamp":{"$gte": isodate.datetime_isoformat(start_time),"$lt": isodate.datetime_isoformat(end_time)}}).count()

    def run_loop(self):
        while True:
            self.increase_trade_bucketed_history()
            sleep(5)

    def increase_trade_bucketed_history(self):
        last_one = self.get_trade_bucketed_last_one()
        db_last_one=self.get_db_trade_bucketed_last_one()
        if db_last_one:
            first_one_timestamp=db_last_one["timestamp"]
        else:
            first_one_timestamp=self.get_trade_bucketed_first_one()["timestamp"]
            first_one_timestamp=isodate.parse_datetime(first_one_timestamp)

        last_one_timestamp=last_one["timestamp"]
        if last_one_timestamp == first_one_timestamp:
            logging.info("no more data")
            return
        start = 0
        _more=True
        while _more:
            logging.info("start time:%s,start:%s" % (first_one_timestamp,start))
            result = self.get_trade_bucketed(start=start, start_time=isodate.datetime_isoformat(first_one_timestamp))
            if len(result) == 0:
                print(result)
                print("no more")
                _more = False
            for item in result:
                if type(item) == str:
                    print("error:", result)
                    exit()
                else:
                    item["node"]="bitmex.com"


                _key={"symbol":self.symbol,"binSize":self.bin_size,"timestamp":isodate.parse_datetime(item["timestamp"])}
                del item["timestamp"]
                self.db["bitmex_trade_bucketed"].update_one(_key,{"$set":item},upsert=True)
                self._trade_bucketed.increase_create_trade_bucketed()
            channel.basic_publish(exchange='trade_bucketed', routing_key='bitmex', body=json.dumps(result))
            start = self.count + start
            if len(result)>5:
                sleep(1)
            else:
                sleep(5)

    def get_db_trade_bucketed_last_one(self):
        collection = self.db["bitmex_trade_bucketed"]
        result=collection.find({"symbol":self.symbol,"binSize":self.bin_size}).sort("timestamp", -1).limit(1)
        for i in result:
            return i

    def get_trade_bucketed_first_one(self,):
        return self.get_trade_bucketed(count=1)[0]

    def get_trade_bucketed_last_one(self):
        return self.get_trade_bucketed(count=1,reverse="true")[0]


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
    base_url = "https://www.bitmex.com"
    uri = "mongodb://%s:%s@%s" % (
        quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)
    client = MongoClient(uri)
    db=client["trade_data"]
    symbol = "XBTUSD"
    bin_size = '1m'
    bitmex=HttpBitmex(db,base_url,symbol,bin_size)
    bitmex.run_loop()



