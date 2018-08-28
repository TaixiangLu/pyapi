#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:40
# @Author  : jianguo@zhugefang.com
# @Desc    :



log_config = {
    "path": "/home/zhuge/project/zhuge-etl2/logs/",
    "level": "DEBUG"  #INFO ERROR DEBUG
}

mongo_conf = {
    "borough": {
        "default": {
            # 主库
            'host': 'dds-2ze2f6d07e237dd41500-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
        "slave_1": {
            'host': 'mongo.zhugefang.com',
            'port': 9603,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
    },
    "hidden_etl": {
        "default": {
            # 主库
            'host': 'dds-2ze2f6d07e237dd41500-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
        "slave_1": {
            'host': 'mongo.zhugefang.com',
            'port': 9603,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
    },
    "hidden_new": {
        "default": {
            # 主库
            'host': 'dds-2ze2f6d07e237dd41500-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
        "slave_1": {
            'host': 'mongo.zhugefang.com',
            'port': 9603,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },
    },
    "brokerbd": {
        "default": {
            'host': 'dds-2ze2b1d1c958b0f41116-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'root',
            'password': 'HDP4xQvlUPDprhMjkw6SWErgkWCAZCDb',
        },
    },
    "building": {
        "default": {
            # 主库
            'host': 'mongo.zhugefang.com',
            'port': 9601,
            'username': 'zhuge',
            'password': '7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck',
        },

    },
    "ccmag": {
        "default": {
            'host': 'dds-2ze2b1d1c958b0f41116-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'root',
            'password': 'HDP4xQvlUPDprhMjkw6SWErgkWCAZCDb',
        }
    },
    "rules_template": {
        "default": {
            'host': 'dds-2ze2b1d1c958b0f41116-pub.mongodb.rds.aliyuncs.com',
            'port': 3717,
            'username': 'root',
            'password': 'HDP4xQvlUPDprhMjkw6SWErgkWCAZCDb',
        },
    },
}

redis_conf = {
    "brokers_api": {
        "db_url": "redis://127.0.0.1:6379/5"
    },

    "borough_api": {
        "db_url": "redis://root:vTTKyUsFFbjIgZGw3lf229iyJ1U@16c51b2287ed4bd2.m.cnbja.kvstore.aliyuncs.com:6379/15",
    },

    "sell": {
        "db_url": "redis://127.0.0.1:6379/1",
    },

    "complex": {
        "db_url": "redis://root:zhugeZHAOFANG1116@r-2zefc71473d249c4.redis.rds.aliyuncs.com:6379/1",
    },
    "complex_cityarea": {
        "db_url": "redis://root:zhugeZHAOFANG1116@r-2zefc71473d249c4.redis.rds.aliyuncs.com:6379/4",
    },
    "complex_api": {
        "db_url": "redis://root:vTTKyUsFFbjIgZGw3lf229iyJ1U@16c51b2287ed4bd2.m.cnbja.kvstore.aliyuncs.com:6379/3",
    },
    "plathouse": {
        "db_url": "redis://root:zhugeZHAOFANG1116@redis.zhugefang.com:9431/7",
    },
    "ccmag_cache": {
        "db_url": "redis://127.0.0.1:6379/0",
    }
}

pika_conf = {
    "sell_api": {
        "host": "39.107.35.217",
        "port": 9221,
        "pre":"D-sell-"
    }
}

# rabbitmq_conf = {"mq_ip": "101.201.119.110"}

rabbitmq_conf = {
    "sell": {
        "default": {
            # "host": "101.201.119.110",
            "host": "10.45.146.41",
            "user": "zhuge",
            "passwd": "data",
            "port": "5672"
        },
        "slave_1": {
            "host": "101.201.103.13",
            "user": "admin",
            "passwd": "admin",
            "port": "5672"
        }
    }
}

mysql_conf = {
    "sell": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            # "host": "127.0.0.1",
            # "user": "root",
            # "passwd": "123456",
            "port": 3306,
            "charset": "utf8",
        },
        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            # "host": "127.0.0.1",
            # "user": "root",
            # "passwd": "123456",
            "port": 3306,
            "charset": "utf8"
        },
    },
    "new_sell": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "port": 3306,
            "charset": "utf8",
        },
        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8"
        }
    },
    "brokerhouse": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        },
        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        }
    },
    "broker": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "rm-2ze40n630wgnd2fgc.mysql.rds.aliyuncs.com",
            # "host":"mysql.zhugefang.com",
            "user": "data_rw",
            # "port": 9511,
            "port": 3306,
            "passwd": "O$le6OAPc!4s*l3WUNrngHnm#W5*CP6R",
            "charset": "utf8",
        },
        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        }
    },
    "brokers": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        },

        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        }
    },
    "new_brokers": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "port": 3306,
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "charset": "utf8",
        }
    },

    "complex": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "port": 3306,
            # "host": "mysql.zhugefang.com",
            # "user": "data_r",
            # "passwd": "ugtQiLyMAgBUf81tyOoMcRgzIzYOszjL",
            # "port": 9541,
            "charset": "utf8",
        },
        "slave_1": {

        }
    },
    "rent": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "port": 3306,
            "charset": "utf8",
        },
        "slave_1": {

        }
    },

    "plathouse": {
        # "default": {
        #     "max_connections": 20,
        #     "wait_connection_timeout": 3,
        #     "host": "rm-2zegcz5fcw1k13su1.mysql.rds.aliyuncs.com",
        #     "user": "data_rw",
        #     "passwd": "O$le6OAPc!4s*l3WUNrngHnm#W5*CP6R",
        #     # "host": "127.0.0.1",
        #     # "user": "root",
        #     "port": 3306,
        #     # "passwd": "root",
        #     "charset": "utf8"
        #  },
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "101.201.119.240",
            "user": "afe-rw",
            "passwd": "HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ",
            "port": 3306,
            "charset": "utf8"
        },
        "slave_1": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "127.0.0.1",
            "user": "root",
            "port": 3306,
            "passwd": "root",
            "charset": "utf8"
        }
    },

    "zhuge_dm": {
        "default": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "mysql.zhugefang.com",
            "user": "dm_rw",
            "passwd": "CszwRk3breCsM5BCH0yDfHLorJM5QB5T",
            "port": 9571,
            "charset": "utf8"
        },
        "dev": {
            "max_connections": 20,
            "wait_connection_timeout": 3,
            "host": "rm-2zed5a6vhd5qk06z5.mysql.rds.aliyuncs.com",
            "user": "dm_rw",
            "passwd": "CszwRk3breCsM5BCH0yDfHLorJM5QB5T",
            "port": 3306,
            "charset": "utf8"
        }
    }
}

es_config = {
    "index_name_pre": "online_",
    "sell": {
        "host": [
            # "101.201.119.240:9200"
            "101.201.119.240:9200"
        ],
        "maxsize": 25
    },

    "brokerhouse": {
        "host": [
            "101.201.119.240:9200"
        ],
        "maxsize": 25
    },

    "plathouse": {
        "host": [
            "101.201.119.240:9200"
        ],
        "maxsize": 25
    },
    "house_new_online": {
        "host": [
            'http://dataes.zhugefang.com:9701',
        ],
        "maxsize": 25
    },
}
