# -*- coding: utf-8 -*-
import sys
import json
import pymysql
import datetime

def config(self):
    conf = open(self, 'r')
    return json.load(conf)
def connectDB(conf):
    conn = pymysql.connect(host=conf['url'], user=conf['user'], password=conf['password'], database=conf['db'], port=conf['port'])
    return conn
####删除数据odps_dataworks_api_flow并且生成日期切片表
def delFlow(conn):
    # 获取今日的日期
    cur_Day = datetime.datetime.now().strftime("%Y-%m-%d")
    # 获取前一天的日期
    d1 = datetime.datetime.now() + datetime.timedelta(days=-1)
    last_day = d1.strftime("%Y-%m-%d")
    lastday = d1.strftime("%Y%m%d")
    # 一周前时间
    d2 = datetime.datetime.now() + datetime.timedelta(days=-7)
    last_7day = d2.strftime("%Y-%m-%d")
    cursor = conn.cursor()
    # 操作数据表
    dropsql = "drop table if exists odps_dataworks_api_flow_{}".format(lastday)
    cursor.execute(dropsql)
    print "删除表语句：{}，执行成功！".format(dropsql)
    # 备份数据表
    baksql = "create table odps_dataworks_api_flow_{} like odps_dataworks_api_flow".format(lastday)
    cursor.execute(baksql)
    print "建表语句：{}，执行成功！".format(baksql)
    # 插入数据表
    insertsql = "insert into odps_dataworks_api_flow_{} select * from odps_dataworks_api_flow".format(lastday)
    cursor.execute(insertsql)
    print "插入表语句：{}，执行成功！".format(insertsql)
    # 删除数据表的数据,重新计数
    resql = "truncate odps_dataworks_api_flow"
    cursor.execute(resql)
    print "删除数据语句：{}，执行成功！".format(resql)
    conn.commit()
    cursor.close()
    conn.close()
def dropTable(self):

if __name__ == '__main__':
    conf = config("database.config")
    conn = connectDB(conf)
    delFlow(conn)