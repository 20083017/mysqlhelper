#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:liuquan
# make_time:2023/01/09
#from tkinter.tix import ROW
from db_dbutils_init import get_my_connection
from db_dbutils_init_new import get_my_connection_new
import typing
import threading

class MySqLHelper(object):
    """执行语句查询有结果返回结果没有返回0；增/删/改返回变更数据条数，没有返回0"""
    def __init__(self):
        self.db = get_my_connection()  # 从数据池中获取连接

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'inst'):  # 单例
            cls.inst = super(MySqLHelper, cls).__new__(cls, *args, **kwargs)
        return cls.inst

    # 封装执行命令
    def execute(self, sql, param=None, autoclose=False):
        """
        【主要判断是否有参数和是否执行完就释放连接】
        :param sql: 字符串类型，sql语句
        :param param: sql语句中要替换的参数"select %s from tab where id=%s" 其中的%s就是参数
        :param autoclose: 是否关闭连接
        :return: 返回连接conn和游标cursor
        """
        cursor, conn = self.db.getconn()  # 从连接池获取连接
        count = 0
        try:
            # count : 为改变的数据条数
            if param:
                count = cursor.execute(sql, param)
            else:
                count = cursor.execute(sql)
            conn.commit()
            if autoclose:
                self.close(cursor, conn)
        except Exception as e:
            pass
        return cursor, conn, count

    # 执行多条命令
    # def executemany(self, lis):
    #     """
    #     :param lis: 是一个列表，里面放的是每个sql的字典'[{"sql":"xxx","param":"xx"}....]'
    #     :return:
    #     """
    #     cursor, conn = self.db.getconn()
    #     try:
    #         for order in lis:
    #             sql = order['sql']
    #             param = order['param']
    #             if param:
    #                 cursor.execute(sql, param)
    #             else:
    #                 cursor.execute(sql)
    #         conn.commit()
    #         self.close(cursor, conn)
    #         return True
    #     except Exception as e:
    #         print(e)
    #         conn.rollback()
    #         self.close(cursor, conn)
    #         return False

    # 释放连接
    def close(self, cursor, conn):
        """释放连接归还给连接池"""
        cursor.close()
        conn.close()

    # 查询单条
    def selectone(self, sql, param=None):
        """查询某1条数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            res = cursor.fetchone()
            self.close(cursor, conn)
            return res
        except Exception as e:
            print("error_msg:", e.args)
            self.close(cursor, conn)
            return count

    # 增加
    def insertone(self, sql, param):
        """插入某1条数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            # _id = cursor.lastrowid()  # 获取当前插入数据的主键id，该id应该为自动生成为好
            conn.commit()
            self.close(cursor, conn)
            return count

        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 增加多行
    def insertmany(self, sql, param):
        """
        :param sql:
        :param param: 必须是元组或列表[(),()]或（（），（））
        :return:
        """
        cursor, conn, count = self.db.getconn()
        try:
            cursor.executemany(sql, param)
            conn.commit()
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 删除
    def delete(self, sql, param=None):
        """删除数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            self.close(cursor, conn)
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 更新
    def update(self, sql, param=None):
        """更新数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            conn.commit()
            self.close(cursor, conn)
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count


class MySqLHelperNew(object):
    """执行语句查询有结果返回结果没有返回0；增/删/改返回变更数据条数，没有返回0"""
    def __init__(self):
        self.db = get_my_connection_new()  # 从数据池中获取连接

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'inst'):  # 单例
            cls.inst = super(MySqLHelperNew, cls).__new__(cls, *args, **kwargs)
        return cls.inst

    # 封装执行命令
    def execute(self, sql, param=None, autoclose=False):
        """
        【主要判断是否有参数和是否执行完就释放连接】
        :param sql: 字符串类型，sql语句
        :param param: sql语句中要替换的参数"select %s from tab where id=%s" 其中的%s就是参数
        :param autoclose: 是否关闭连接
        :return: 返回连接conn和游标cursor
        """
        cursor, conn = self.db.getconn()  # 从连接池获取连接
        count = 0
        try:
            # count : 为改变的数据条数
            if param:
                count = cursor.execute(sql, param)
            else:
                count = cursor.execute(sql)
            conn.commit()
            if autoclose:
                self.close(cursor, conn)
        except Exception as e:
            pass
        return cursor, conn, count

    # 执行多条命令
    # def executemany(self, lis):
    #     """
    #     :param lis: 是一个列表，里面放的是每个sql的字典'[{"sql":"xxx","param":"xx"}....]'
    #     :return:
    #     """
    #     cursor, conn = self.db.getconn()
    #     try:
    #         for order in lis:
    #             sql = order['sql']
    #             param = order['param']
    #             if param:
    #                 cursor.execute(sql, param)
    #             else:
    #                 cursor.execute(sql)
    #         conn.commit()
    #         self.close(cursor, conn)
    #         return True
    #     except Exception as e:
    #         print(e)
    #         conn.rollback()
    #         self.close(cursor, conn)
    #         return False

    # 释放连接
    def close(self, cursor, conn):
        """释放连接归还给连接池"""
        cursor.close()
        conn.close()

    # 查询单条
    def selectone(self, sql, param=None):
        """查询某1条数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            # res = cursor.fetchone()
            self.close(cursor, conn)
            return count
        except Exception as e:
            print("error_msg:", e.args)
            self.close(cursor, conn)
            return count

    # 增加
    def insertone(self, sql, param):
        """插入某1条数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            # _id = cursor.lastrowid()  # 获取当前插入数据的主键id，该id应该为自动生成为好
            conn.commit()
            self.close(cursor, conn)
            return count

        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 增加多行
    def insertmany(self, sql, param):
        """
        :param sql:
        :param param: 必须是元组或列表[(),()]或（（），（））
        :return:
        """
        cursor, conn, count = self.db.getconn()
        try:
            cursor.executemany(sql, param)
            conn.commit()
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 删除
    def delete(self, sql, param=None):
        """删除数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            self.close(cursor, conn)
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count

    # 更新
    def update(self, sql, param=None):
        """更新数据"""
        try:
            cursor, conn, count = self.execute(sql, param)
            conn.commit()
            self.close(cursor, conn)
            return count
        except Exception as e:
            print(e)
            conn.rollback()
            self.close(cursor, conn)
            return count


class DataSource:
    """
    :param data.txt:
    :功能:多线程读取data.txt,并进行sql查询:
    """
    def __init__(self, dataFileName, startLine=0, maxcount=None):
        """
        初始化
        """
        self.dataFileName = dataFileName
        self.startLine = startLine  # 第一行行号为1
        self.line_index = startLine # 当前读取位置
        self.maxcount = maxcount  # 读取最大行数
        self.lock = threading.RLock() # 同步锁

        self.__data__ = open(self.dataFileName, 'r', encoding= 'utf-8')
        for i in range(self.startLine):
            l = self.__data__.readline()

    def getLine(self):
        """
        获取1行data.txt中的数据
        """
        self.lock.acquire()
        try:
            if self.maxcount is None or self.line_index < (self.startLine + self.maxcount):
                line = self.__data__.readline()
                if line:
                    self.line_index += 1
                    return True, line
                else:
                    return False, None
            else:
                return False, None

        except Exception as e:
            return False, "处理出错:" + e.args
        finally:
            self.lock.release()

    def __del__(self):
        if not self.__data__.closed:
            self.__data__.close()
            print("关闭数据源:", self.dataFileName)

import time

def process(worker_id):
    """
    开始处理数据
    """
    count = 0
    # db = MySqLHelper()
    # app_agent_count = 0
    # app_agent_error_count = 0
    # while True:
        # if(count > 1000):
        #     break
        # status, data = datasource.getLine()
        # print(type(data))
        # print(data)
        # msg1 = ""
        # if status:
    print(">>> 线程[%d] 获得数据， 正在处理……" % worker_id)
    # print("hello world")
    db = MySqLHelper()
    db_new = MySqLHelperNew()
    # data_list = data.split("\t")
    # print(type(data_list[0]))
    # print("data_list[2]=" + data_list[2])
    #     # # 查询单条
    sql = "show tables"
    cursor, conn, num = db.execute(sql)
    result = cursor.fetchall()
    results = []
    #print(result)
    for i in range(len(result)):
        #print(result[i][0])
        #print(type(result[i][0]))
        if(result[i][0].startswith(b'app_agent_msgroam')):
            results.append(result[i][0].decode("utf-8"))
            print(results[i])
    error_count = 0
    for table_name in results:
        sql1 = "select count(*) from " + table_name
        print("sql1 = " + sql1)
        cursor, conn, num = db.execute(sql1)
        all_count = cursor.fetchall()
        print(all_count)
        num = all_count[0][0]
        print("num = " + str(num))
        for j in range(num):
            sql4 = "select * from " + table_name + " where id=" + str(j+1)
            print("sql4 = " + sql4)
            row = db.selectone(sql4)
            print(row)
            sql3 = "select * from new_app_agent_msgroaming where comuid = " + str(row[1]) + " and s_msgid2= " + str(row[10]) + " and s_basemsgid = " + str(row[11])
            ret = db_new.selectone(sql3)
            if(ret > 0):
                continue
            sql2 = "insert into " + "new_app_agent_msgroaming" +  "(`comuid`,`uid_from`, `uid_to`, `isvisible_min`, `isvisible_max`, `timestamp`, `messageid`,`ttl`, `message_type`, `s_msgid2`, `mid`, `message`) "
            sql2 += "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            #sql2 += "values (" + str(row[1]) + "," + str(row[2]) + ", " + str(row[3]) + ", " + str(row[4]) + ", " + str(row[5]) + ", " + str(row[6]) + ", " + str(row[7]) + ", " + str(row[8]) + ", " + str(row[9]) + ", " + str(row[10]) + ", " + str(row[11]) + ", " + str(row[12]) +  " )"
            print("sql2 = " + sql2)
            row_new=[]
            row_new.append(row[1]) 
            row_new.append(row[2])
            row_new.append(row[3])
            row_new.append(row[4])
            row_new.append(row[5])
            row_new.append(row[6])
            row_new.append(row[7])
            row_new.append(row[8])
            row_new.append(row[9])
            row_new.append(row[10])
            row_new.append(row[11])
            row_new.append(row[12])
            #row_new.append(row[12].decode("utf-8"))
            #row_new[12] = row_new[12].decode("utf-8")
            print(row_new)
            ret = db_new.insertone(sql2,row_new)
            if(ret == 0):
                error_count += 1
    print("error_count= " + str(error_count))
    print(">>> 线程[%d] 结束， 共处理[%d]条数据" % (worker_id, count))

if __name__ == '__main__':
    # datasource = DataSource('data.txt')
    workercount = 1 # 开启的线程数，注意：并非越多越快哦
    workers = []
    for i in range(workercount):
        worker = threading.Thread(target = process, args=[i + 1])
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
