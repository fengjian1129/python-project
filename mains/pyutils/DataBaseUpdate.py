# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from warnings import filterwarnings
import pymysql
import json
from DataBaseSync import config, connect_db
import datetime
# 更改mysql warning级别，防止drop table if not exists 时报错
filterwarnings('ignore', category=pymysql.Warning)


def dbConfig(self):
    conf = config(self)
    # 建立数据库链接
    conn = connect_db(conf)
    return conn

def closeConn(conn):
    conn.close()

def backUpData(conn):
        # 获取今日的日期
        curDay = datetime.datetime.now().strftime("%Y%m%d")
        d1 = datetime.datetime.now() + datetime.timedelta(days=-7)
        last7day = d1.strftime("%Y%m%d")
        # 建立游标
        cursor = conn.cursor()
        # 先备份数据dim_table_catalog
        dropsql = "drop table if exists dim_table_catalog_{}".format(curDay)
        cursor.execute(dropsql)
        print "删除dim_table_catalog_{}表成功".format(curDay)
        dpsql = "drop table if exists dim_table_catalog_{}_tmp".format(curDay)
        cursor.execute(dpsql)
        print "删除dim_table_catalog_{}_tmp表成功".format(curDay)
        # 创建dim_table_catalog_yyyymmdd
        createsql = "create table dim_table_catalog_{}_tmp like dim_table_catalog_sample".format(curDay)
        cursor.execute(createsql)
        print "创建表dim_table_catalog_{}_tmp表成功".format(curDay)

        # 先备份数据dim_data_dictionary_tmp
        dicsql = "drop table if exists dim_data_dictionary_tmp"
        tmpsql = "drop table if exists dim_data_dictionary_bak"
        cursor.execute(dicsql)
        cursor.execute(tmpsql)
        print "删除dim_data_dictionary_tmp表和dim_data_dictionary_bak表成功"

        # 创建dim_data_dictionary_tmp
        dictsql = "create table {} like {}".format("dim_data_dictionary_tmp", "dim_data_dictionary_sample")

        cursor.execute(dictsql)
        conn.commit()
        cursor.close()
        print "创建表dim_data_dictionary_tmp表成功"
        cursor = conn.cursor()
        # 删除备用表
        delsql = "drop table if exists dim_table_catalog_{}".format(last7day)

        cursor.execute(delsql)
        print "删除7天前的表dim_table_catalog_{}".format(last7day)
        conn.commit()
        cursor.close()
def updateData(conn):
        # 获取今日的日期
        curDay = datetime.datetime.now().strftime("%Y%m%d")
        # 一周前时间
        d1 = datetime.datetime.now() + datetime.timedelta(days=-7)
        last7day = d1.strftime("%Y%m%d")
        cursor = conn.cursor()
        # 往今日目标表里插入昨日存量数据
        catalogsql = "insert into dim_table_catalog_{}_tmp(data_dictionary_key,table_name,table_desc,product_line,data_base," \
                     "cluster,instance_name,volume,table_status,is_partition,life_cycle,data_union,record_count,attribute_num," \
                     "create_by,update_by,enabled,create_time,update_time,duty_user_id,db_type,table_key,table_json,sync_time) " \
                     " SELECT a.data_dictionary_key,a.table_name,b.service_desc,a.product_line,a.data_base" \
                     ",a.cluster,a.instance_name,b.data_size,a.table_status,a.is_partition,a.life_cycle,b.data_union,b.data_count" \
                     ",b.attribute_num,a.create_by,a.update_by,a.enabled,a.create_time,a.update_time,a.duty_user_id" \
                     ",a.db_type,a.table_key,b.table_json,a.sync_time " \
                     "FROM dim_table_catalog a " \
                     "INNER JOIN datasource_all b ON a.cluster=b.instance_id AND a.table_name=b.table_name AND a.data_base=b.db_name".format(curDay)
        print catalogsql
        cursor.execute(catalogsql)
        print "存量数据同步已完成,目标表dim_table_catalog_{}_tmp".format(curDay)

        # 往今日目标表里插入新增的数据
        datasourcesql = "insert into dim_table_catalog_{}_tmp(data_dictionary_key,table_name,table_desc,product_line,data_base," \
                        "cluster,instance_name,volume,table_status,is_partition,life_cycle,data_union,record_count,attribute_num," \
                        "create_by,update_by,enabled,create_time,update_time,duty_user_id,db_type,table_key,table_json,sync_time) " \
                        "SELECT a.table_id,a.table_name,a.service_desc,'',a.db_name" \
                        ",a.instance_id,a.instance_name,a.data_size,a.status,0,'',a.data_union,a.data_count" \
                        ",a.attribute_num,'admin','admin',0,a.create_dt,a.update_dt,''" \
                        ",a.db_type,a.`key`,a.table_json,CURRENT_TIMESTAMP " \
                        "FROM datasource_all a " \
                        "LEFT JOIN dim_table_catalog b ON a.instance_id=b.cluster AND a.table_name=b.table_name AND a.db_name=b.data_base WHERE b.table_name IS NULL".format(curDay)
        print datasourcesql
        cursor.execute(datasourcesql)
        print "新增数据同步已完成,目标表dim_table_catalog_{}_tmp".format(curDay)

        # 处理目标表内的数据
        datasql = "SELECT table_json,data_dictionary_key,table_name,cluster,data_base,table_key,table_desc,product_line," \
                  "instance_name,volume,table_status,is_partition,life_cycle,data_union,record_count," \
                  "attribute_num,create_by,update_by,enabled,create_time,update_time,duty_user_id," \
                  "db_type,sync_time " \
                  "FROM dim_table_catalog_{}_tmp".format(curDay)
        cursor.execute(datasql)
        result = cursor.fetchall()
        # 拼接插入语句SQL，优化写入数据库效率

        for i in range(len(result)):
            jsondata = json.loads(result[i][0])
            table_id = result[i][1]
            table_name = result[i][2]
            table_key = result[i][5]
            dicationsql = "INSERT INTO dim_data_dictionary_tmp(data_dictionary_key,table_name,`field`," \
                          "`type`,table_key,default_value,field_comment,create_by,update_by) values "
            sql = dicationaryOnece(jsondata, table_id, table_name, table_key)
            dicationsql += sql
            cursor.execute(dicationsql[:-1])
            conn.commit()
        # 目标表替换正式表
        nor1sql = "RENAME TABLE dim_table_catalog TO dim_table_catalog_{}".format(curDay)
        nor2sql = "RENAME TABLE dim_table_catalog_{}_tmp TO dim_table_catalog".format(curDay)
        nor3sql = "RENAME TABLE dim_data_dictionary TO dim_data_dictionary_bak"
        nor4sql = "RENAME TABLE dim_data_dictionary_tmp TO dim_data_dictionary"

        cursor.execute(nor1sql)
        cursor.execute(nor2sql)
        cursor.execute(nor3sql)
        cursor.execute(nor4sql)
        conn.commit()
        print "备份表数据dim_table_catalog_{}表和dim_data_dictionary表成功".format(curDay)

        cursor.close()


def dicationaryOnece(jsondata, table_id, table_name, table_key):
    oncesql = ""
    for i in range(len(jsondata)):
            filed = jsondata[i]['fields_name']
            comment = jsondata[i]['comment']
            filedtype = jsondata[i]['fields_type']
            key = ''
            if table_key == filed:
                key = table_key
            insertsql = "('{}','{}','{}','{}','{}','{}','{}','{}','{}'),"\
                        .format(table_id, table_name, filed, filedtype, key, '', comment, 'admin', 'admin')
            oncesql += insertsql
    return oncesql
# 此处为增量更新dicationary表的table_id，由于数据表不支持这么做，后期再说
def dicationaryDays(cursor, jsondata, table_id, table_name, dbname, table_key):
    daysql = "select data_dictionary_key, table_name, `field`, " \
             "`type`, table_key, default_value, field_comment, create_by, update_by " \
             "from dim_data_dictionary"
def run(self):
    # 获取数据库链接
    conn = dbConfig(self)
    # 备份数据
    backUpData(conn)
    # 更新字典表
    updateData(conn)

    closeConn(conn)
    print "结束字典表的更新"
