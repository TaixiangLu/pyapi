# -*- coding: utf-8 -*-
from databases.mysqldb.mysqldb import MysqlDB
from databases.pikadb.pikadb import PikaDB
from databases.redisdb.redisdb import RedisDB
from databases.mongodb.mongodb import MongoDB
from databases.rabbitmqdb.rabbitmq import Rabbitmq
from databases.esdb.esdb import EsDB


class dbfactory():
    @staticmethod
    def create_db(**kwargs):
        db_type = kwargs.get("db_type")
        return getattr(dbfactory, db_type)(**kwargs)

    @staticmethod
    def db_mysql(*args, **kwargs):
        return MysqlDB(**kwargs)

    @staticmethod
    def db_mongo(*args, **kwargs):
        return MongoDB(**kwargs)

    @staticmethod
    def db_es(*args, **kwargs):
        return EsDB(**kwargs)

    @staticmethod
    def db_redis(*args, **kwargs):
        return RedisDB(**kwargs).getRedisConn(**kwargs)

    @staticmethod
    def db_pika(*args, **kwargs):
        return PikaDB(**kwargs).getPikaConn(**kwargs)

    @staticmethod
    def db_rabbitmq(*args, **kwargs):
          return Rabbitmq(**kwargs)
