#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:liuquan
# make_time:2023/01/09
from dbutils.pooled_db import PooledDB, SharedDBConnection
# from DBUtils.PooledDB import PooledDB
import db_config as config

class MyConnectionPoolNew(object):
    """
    @功能：创建数据库连接池
    """
    __pool = None

    # 创建数据库连接conn和游标cursor
    def __enter__(self):
        self.conn = self.__getconn()
        self.cursor = self.conn.cursor()

    # 创建数据库连接池
    def __getconn(self):
        if self.__pool is None:
            self.__pool = PooledDB(
                creator=config.DB_CREATOR,
                mincached=config.DB_MIN_CACHED,
                maxcached=config.DB_MAX_CACHED,
                maxshared=config.DB_MAX_SHARED,
                maxconnections=config.DB_MAX_CONNECYIONS,
                blocking=config.DB_BLOCKING,
                maxusage=config.DB_MAX_USAGE,
                setsession=config.DB_SET_SESSION,
                host=config.DB_TEST_HOST_new,
                port=config.DB_TEST_PORT_new,
                user=config.DB_TEST_USER_new,
                passwd=config.DB_TEST_PASSWORD_new,
                db=config.DB_TEST_DBNAME_new,
                use_unicode=False,
                charset=config.DB_CHARSET
            )
        return self.__pool.connection()

    # 释放连接池资源
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    # 从连接池中取出一个连接
    def getconn(self):
        """
        @功能：从数据库连接池中获取1个连接
        """
        conn = self.__getconn()
        cursor = conn.cursor()
        return cursor, conn

# 获取连接池,实例化
def get_my_connection_new():
    """
    @功能：获取数据库连接池
    """
    return MyConnectionPoolNew()
