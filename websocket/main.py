import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.options
from tornado.options import define, options
from tornado.websocket import WebSocketHandler
import json
import logging
from websocket import settings
import threading
import pika

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
define("port", default=4000, type=int)

class PushHandler(WebSocketHandler):
    topics = settings.realtime_topics

    def open(self):
        subscribe = self.request.query_arguments.get("subscribe")
        if subscribe and len(subscribe) == 1:
            subtopics = subscribe[0].decode("utf8").split(",")
            for topic in subtopics:
                try:
                    self.topics[topic].add(self)
                except KeyError:
                    print("topic not exist :%s" % topic)

        print(self.topics)
        self.write_message("hello")

    def push(self, message):
        """推送消息给客户端"""
        topic = message.get("topic")
        body = message.get("body")
        users = self.topics.get(topic)
        for user in users:
            user.write_message(body)

    def on_message(self, message):
        """
        命令格式：
        {"op": "<command>", "args": ["arg1", "arg2", "arg3"]}
        订阅：
            subscribe
            unsubscribe
        心跳
            Ping

        要订阅主题，请发送逗号分隔的主题列表。例如︰ 例如：
                wss://www.bitmex.com/realtime?subscribe=instrument,orderBook:XBTUSD
        如果您已连接，并且想要订阅一个新主题，请发送以下格式的请求︰
                {"op": "subscribe", "args": [<SubscriptionTopic>]}
        """
        try:
            msg = json.loads(message)
            op = msg.get("op")
            args = msg.get("args")
        except json.decoder.JSONDecodeError:
            print("json.decoder.JSONDecodeError %s" % message)
        print("on message:", message)

    def on_close(self):
        print("on_close:", self)
        print("onclose:", self.topics)
        for k, v in self.topics.items():
            try:
                v.remove(self)
            except KeyError:
                pass

    def check_origin(self, origin):
        return True  # 允许WebSocket的跨域请求



def callback(ch, method, properties, body):
    LOGGER.info("%s,%s,%s,%s" % (ch, method, properties, body))
def pika_run_loop():
    print("start pika_run_loop")
    credentials = pika.PlainCredentials(settings.MQ_USER, settings.MQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=settings.MQ_HOST, port=settings.MQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange='trade_bucketed.topic', exchange_type='topic')

    result = channel.queue_declare("", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='trade_bucketed.topic', routing_key="bitmex.com-XBTUSD", queue=queue_name)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()




if __name__ == '__main__':
    t = threading.Thread(target=pika_run_loop, args=())
    t.setDaemon(True)
    t.start()
    tornado.options.parse_command_line()
    app = tornado.web.Application([
        (r"/realtime", PushHandler),
    ],
        debug=True
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
