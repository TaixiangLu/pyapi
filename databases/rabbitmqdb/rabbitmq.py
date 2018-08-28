#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-20 上午10:56
# @Author  : tanghan@zhugefang.com
# @Desc    :
from apps.config.Config import rabbitmq_conf
import contextlib
import pika
import logging

class Rabbitmq(object):
    def __init__(self, *args, **kwargs):
        self.conf_name = kwargs.get("conf_name")
        self.mq_config = rabbitmq_conf.get(self.conf_name)

    @contextlib.contextmanager
    def get_conn(self, *args, **kwargs):
        # 获取主从连接
        conf = self.mq_config.get(kwargs.get("link_type", 'default'))
        host = conf.get("host")
        port = conf.get("port")
        user = conf.get("user")
        passwd = conf.get("passwd")
        credentials = pika.PlainCredentials(user, passwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
        channel = connection.channel()

        try:
            yield channel
        except Exception as e:
            logging.error("连接rabbitmq失败:", e)
        finally:
            channel.close()

if __name__ == '__main__':
    # print(Rabbitmq().get_conn(conf_name="mq_id"))
    RM = Rabbitmq(conf_name='sell')
    print(RM.get_conn())