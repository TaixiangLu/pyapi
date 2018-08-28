# -*- coding: utf-8 -*-s
import redis
from apps.config.Config import redis_conf
class RedisDB(object):
    def __init__(self, *args, **kwargs):
        conf_name = kwargs.get("conf_name")
        conf = redis_conf.get(conf_name, {})
        self.db_url = conf.get("db_url", "")
        self.host = conf.get("host", "127.0.0.1")
        self.port = conf.get("port", 6379)
        self.db = conf.get("db", 0)
        self.max_connections = conf.get("max_connections", 10)

    def getRedisConn(self, **kwargs):
        if self.db_url:
            self.server = redis.StrictRedis.from_url(url=self.db_url)
        else:
            rdp = redis.ConnectionPool(host=self.host, port=self.port, db=self.db,max_connections=self.max_connections)
            self.server = redis.StrictRedis(connection_pool=rdp)
        return self.server


if __name__ == '__main__':

    RB = RedisDB(conf_name="plathouse")
    con = RB.getRedisConn()
    con.set("name","888")

