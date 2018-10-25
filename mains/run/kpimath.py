# -*- coding: utf-8 -*-
import sys
import requests
import json
import time
import datetime
from mains.pyutils.DataBaseSync import config, connect_db, insertAll
from mains.pyutils.DataBaseUpdate import backUpData, updateData
import re

def init(self):
    conf = config(self)
    conn = connect_db(conf)
    cursor = conn.cursor()
    # 获取今日的日期
    curDay = datetime.datetime.now().strftime("%Y%m%d")
    # 本月月初
    monthFirstDay = datetime.datetime.now().strftime("%Y-%m") + "-01"
    # 七天前的日期
    last7day = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime("%Y%m%d")
    # 前一天的日期
    lastday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    # 前一天的日期
    last_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    sql = "SELECT table_name FROM information_schema.TABLES  WHERE table_schema='{}'" \
          " AND table_name LIKE 'dim_table_catalog_2%' ORDER BY 1 DESC".format(conf['dbName'])
    print sql
    cursor.execute(sql)
    one = cursor.fetchone()
    lasttable = one[0]
    print lasttable
    if lasttable is None:
       createsql = "create table dim_table_catalog_{} like dim_table_catalog".format(lastday)
       cursor.execute(createsql)
       conn.commit()

    # 创建临时表
    sql1 = "DROP TABLE IF EXISTS temp_table_catalog"
    cursor.execute(sql1)
    sql2 = "CREATE TABLE temp_table_catalog as SELECT * FROM dim_table_catalog_{}".format(lastday)
    cursor.execute(sql2)
    # 删除指标计算总表当日数据
    sql3 = "DELETE FROM det_metadata_statistics WHERE  stat_dt='{}'".format(last_date)
    cursor.execute(sql3)
    # 插入kpi数据
    sql4 = "INSERT INTO  det_metadata_statistics SELECT IFNULL(T1.stat_dt,T2.stat_dt)  AS stat_dt, " \
           "T1.totalTableNum,T1.totalDBNum,T1.totalclusterNum,T1.totalvolume,T1.totalrecord_count," \
           "T1.AVGrecord_count,T1.MAXrecord_count,T3.table_name AS MAXRecordTable,T1.MINrecord_count," \
           "T4.table_name AS MINRecordTable,T1.recordcount0,T1.downrecord_count10000,T1.downrecordcount100000," \
           "T1.downrecordcount1000000,T1.downrecordcount10000000,T1.uprecord_count10000000," \
           "T1.totalRecordattribute_num,T1.structure_alter_num,T1.record_count_alter_num,T1.add_table_num," \
           "T2.drop_table_num,T5.structure_alter_num+T1.structure_alter_num,T1.record_count_alter_num+T5.record_count_alter_num," \
           "T1.add_table_num+T5.add_table_num,T2.drop_table_num+T5.drop_table_num,NOW() FROM( " \
           "/*用T-1的数据与T-2的数据做对比，统计出T-1的增量表，表结构变化表，数据量变化表，删除表的总量*/" \
           "SELECT '{}' AS stat_dt,COUNT(DISTINCT dim_table_catalog.data_dictionary_key) AS totalTableNum," \
           "COUNT(DISTINCT dim_table_catalog.data_base) AS totalDBNum,COUNT(DISTINCT dim_table_catalog.cluster) AS totalclusterNum," \
           "SUM(dim_table_catalog.volume)  AS totalvolume,SUM(dim_table_catalog.record_count) AS totalrecord_count," \
           "AVG(dim_table_catalog.record_count) AS  AVGrecord_count,MAX(dim_table_catalog.record_count) AS  MAXrecord_count," \
           "MIN(dim_table_catalog.record_count) AS  MINrecord_count,COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count=0  THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS recordcount0," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count>0  AND dim_table_catalog.record_count<=10000 THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS downrecord_count10000," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count>10000  AND dim_table_catalog.record_count<=100000 THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS downrecordcount100000," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count>100000  AND dim_table_catalog.record_count<=1000000 THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS downrecordcount1000000," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count>1000000 AND dim_table_catalog.record_count<=10000000 THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS downrecordcount10000000," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count>10000000   THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS uprecord_count10000000," \
           "SUM(dim_table_catalog.attribute_num)  AS totalRecordattribute_num,COUNT(DISTINCT(CASE WHEN dim_table_catalog.attribute_num<>temp_table_catalog.attribute_num " \
           "OR dim_table_catalog.table_key<>temp_table_catalog.table_key THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS structure_alter_num," \
           "COUNT(DISTINCT(CASE WHEN dim_table_catalog.record_count<>temp_table_catalog.record_count THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS record_count_alter_num," \
            "COUNT(DISTINCT(CASE WHEN  dim_table_catalog.data_dictionary_key IS NULL THEN dim_table_catalog.data_dictionary_key ELSE NULL END ))  AS add_table_num" \
            " FROM dim_table_catalog" \
            " LEFT JOIN temp_table_catalog  ON dim_table_catalog.cluster=temp_table_catalog.cluster  AND dim_table_catalog.data_base=temp_table_catalog.data_base AND dim_table_catalog.table_name=temp_table_catalog.table_name) T1" \
            " LEFT JOIN (/*当天删除量，需要单独统计*/ SELECT '{}' AS stat_dt," \
            " COUNT(DISTINCT(CASE WHEN  dim_table_catalog.data_dictionary_key IS NULL THEN temp_table_catalog.data_dictionary_key ELSE NULL END ))  AS drop_table_num" \
            " FROM temp_table_catalog" \
            " LEFT JOIN  dim_table_catalog ON dim_table_catalog.cluster=temp_table_catalog.cluster  AND dim_table_catalog.data_base=temp_table_catalog.data_base AND dim_table_catalog.table_name=temp_table_catalog.table_name) T2" \
            " ON T1.stat_dt=T2.stat_dt" \
            " LEFT JOIN (/*数据量最大的表，需要单独统计*/ SELECT '{}' AS stat_dt,table_name" \
            " FROM  dim_table_catalog ORDER BY record_count DESC LIMIT 1) T3 " \
            " ON T1.stat_dt=T3.stat_dt LEFT JOIN (/*数据量最小的表，需要单独统计*/ " \
            " SELECT '{}' AS stat_dt,table_name FROM  dim_table_catalog" \
            " ORDER BY record_count  LIMIT 1) T4 ON T1.stat_dt=T4.stat_dt" \
            " LEFT JOIN (/*统计月累计数据*/ " \
            " SELECT '{}' as stat_dt,SUM(structure_alter_num) AS structure_alter_num," \
            " SUM(record_count_alter_num) AS record_count_alter_num," \
            " SUM(add_table_num) AS add_table_num," \
            " SUM(drop_table_num) AS drop_table_num" \
            " FROM det_metadata_statistics WHERE stat_dt BETWEEN '{}' AND '{}'" \
            " ) T5 ON T1.stat_dt=T5.stat_dt".format(last_date, last_date, last_date, last_date, last_date, monthFirstDay, last_date)
    print sql4

    cursor.execute(sql4)
    conn.commit()
    print "指标计算成功！"
    cursor.close()
    conn.close()
if __name__ == '__main__':
    init('D:/project/dbsync/test.json')