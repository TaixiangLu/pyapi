#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :

from controller.BaseController import Route
import json,tornado,os
import functools,datetime

route = Route()
from controller.BaseController import catch
from controller.BaseController import BaseController
import json,asyncio

@route('/hello')
class hello(BaseController):
    # @Time    : 18-4-14 下午2:30
    # @Author  : jianguo@zhugefang.com
    # @Desc    : 查询经纪公司列表
    # @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        self.write("ok")
        # return {"data": "欢迎使用诸葛找房小区基础数据api服务!!!"}

@route('/hello0')
class hello(BaseController):
    # @Time    : 18-4-14 下午2:30
    # @Author  : jianguo@zhugefang.com
    # @Desc    : 查询经纪公司列表
    def get(self, *args, **kwargs):
        # 耗时的代码
        os.system("ping -n 2 www.baidu.com")
        self.finish('It works')
@route('/hello1')
class hello(BaseController):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):

        tornado.ioloop.IOLoop.instance().add_timeout(1, callback=functools.partial(self.ping, 'www.baidu.com'))

        # do something others

        self.finish('It works')

    @tornado.gen.coroutine
    def ping(self, url):
        os.system("ping -n 2 {}".format(url))
        return 'after'

@route('/hello2')
class hello(BaseController):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        # yield 结果
        response = yield tornado.gen.Task(self.ping, ' www.baidu.com')
        print('response', response)
        self.finish('hello')

    @tornado.gen.coroutine
    def ping(self, url):
        os.system("ping -n 2 {}".format(url))
        return 'after'

from concurrent.futures import ThreadPoolExecutor

@route('/hello3')
class hello(BaseController):
    executor = ThreadPoolExecutor(10)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):

        url = 'www.baidu.com'
        tornado.ioloop.IOLoop.instance().add_callback(functools.partial(self.ping, url))
        self.finish('It works')

    @tornado.concurrent.run_on_executor
    def ping(self, url):
        os.system("ping -n 2 {}".format(url))

class Executor(ThreadPoolExecutor):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not getattr(cls, '_instance', None):
            cls._instance = ThreadPoolExecutor(max_workers=50)
        return cls._instance

@route('/hello4')
class hello(BaseController):
    executor = Executor()

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        # future = Executor().submit(self.ping, 'www.baidu.com')
        try:
            # response = yield tornado.gen.with_timeout(datetime.timedelta(10), future,
            #                                           quiet_exceptions=tornado.gen.TimeoutError)

            response = yield self.ping('www.baidu.com')
        except Exception as e:
            print (e)
        if response:
            print('response', response)

    @tornado.concurrent.run_on_executor
    def ping(self, url):
        asyncio.set_event_loop(asyncio.new_event_loop())
        os.system("ping -n 2 {}".format(url))
        return 'after'
