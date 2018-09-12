import threading
import tornado.ioloop
import tornado.web
from tornado import gen
import asyncio
from apps.zhuge_borough.controller.TestController import route as route1



class WebServer(threading.Thread):
    def run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        application = tornado.web.Application(route1.urls)
        application.listen(3301)
        tornado.ioloop.IOLoop.instance().start()

WebServer().start()