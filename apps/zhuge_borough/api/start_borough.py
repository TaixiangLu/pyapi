#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 上午11:51
# @Author  : jianguo@zhugefang.com
# @Desc    :
import sys
sys.path.append("../../../")
import tornado.ioloop
from apps.zhuge_borough.controller.TestController import route as route1
import asyncio
if __name__ == '__main__':
    asyncio.set_event_loop(asyncio.new_event_loop())
    port = 3301
    if len(sys.argv) > 1:
        port = sys.argv[1]
    routes =  route1.urls
    application = tornado.web.Application(routes)
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()


