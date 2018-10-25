# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from warnings import filterwarnings
import pymysql
import requests
import json
import uuid
import time

# 更改mysql warning级别，防止drop table if not exists 时报错
filterwarnings('ignore', category=pymysql.Warning)
def getData(self,page):
    p = page
    url = self['url']
    token = self['token']
    post = {'token': token,
            'page': p}
    result = requests.post(url, data=post)
    return json.loads(result.content)
####获取数据库链接
def connect_db(self):
    conn = pymysql.connect(host=self['dbUrl'], port=self['dbPort'], user=self['dbUser'], passwd=self['dbPass'], db=self['dbName'], charset="utf8")
    print("获取数据库{}:{}链接成功".format(self['dbUrl'],self['dbPort']))
    return conn
####读取文件配置信息
def config(self):
    conf = open(self, 'r')
    return json.load(conf)

####删除DataSource_all数据
def delAll(conn, cursor):
    delsql = "truncate datasource_all"
    cursor.execute(delsql)
    conn.commit()
    print "删除表语句：{}，执行成功！".format(delsql)
    cursor.close()
    conn.close()
def insertAll(conn, cursor, conf, page):
    start = time.time()
    ####获取post数据，并插入
    data = getData(conf, page=page)
    end = time.time()
    if isinstance(data, list):
        print("获取第{}页抽取数据消耗时间{}秒".format(page, round(end-start, 2)))

    else:
        print("获取第{}页数据失败".format(page))
        return 1

    # 操作数据行数起始值
    count = 0
    for i in range(len(data)):

        # 表名
        table_name = data[i]['table_name'].replace("`", "")

        if "DEFINER=" in table_name:
            continue

        # 表id（系统分配）
        table_id = uuid.uuid1()

        # 实例id
        instance_id = data[i]['instance_id']
        # 数据库名称
        db_name = data[i]['db_name']
        if "DEFINER=" in db_name or db_name == 'performance_schema' or db_name == 'mysql':
            continue

        # 数据库类型
        db_type = data[i]['db_type']
        # 创建时间
        create_dt = data[i]['create_time']
        if "None" in create_dt:
            create_dt = '1900-01-01 00:00:00'
        # 更新时间
        update_dt = data[i]['update_time']
        if "None" in update_dt:
            update_dt = '1900-01-01 00:00:00'
        # 所属业务线（系统分配）
        service_type = ''
        # 数据量大小
        data_size = data[i]['table_size']
        if "None" in data_size:
            data_size = 0
        # 业务描述
        # 表的comment
        service_desc = data[i]['comment'].replace("\'", "")

        # 责任人id
        author_id = ''
        # 责任人名称
        author_name = ''
        # 表状态
        status = '1'
        # 表的描述json信息
        filed_json = json.dumps(data[i]['fields']).replace("`", "").replace("''", "").replace("\\", "\\\\")

        if 'sql_mode' in filed_json:
            continue
        # 字段数
        attribute_num = len(data[i]['fields'])
        # 主键
        key = data[i]['primary_key'].replace("`", "")
        # 实例名称
        instance_name = data[i]['instance_name']
        # 表中数据量行数
        table_row = data[i]['table_row']

        if "None" in table_row:
            table_row = 0
        data_union = "{}(MB)/{}(条)".format(data_size, table_row)

        insertsql = "insert into datasource_all (`table_name`,table_id,instance_id,instance_name," \
                    "db_name,db_type,create_dt,update_dt,service_type,data_size,data_count,data_union,attribute_num," \
                    "service_desc,author_id,author_name,`key`,table_json,`status`) " \
                    "values('%s','%s' ,'%s' ,'%s' ," \
                    "'%s','%s','%s','%s','%s','%s', '%s','%s','%s'," \
                    "'%s','%s','%s','%s', '%s','%s')" % (table_name, table_id, instance_id, instance_name,
                                                         db_name, db_type, create_dt, update_dt, service_type,
                                                         data_size, table_row, data_union, attribute_num,
                                                         service_desc, author_id, author_name, key, filed_json, status)

        try:
            cursor.execute(insertsql)
            conn.commit()
            count = count+1
        except Exception, e:
           print e.message
    print("第{}页插入{}条数据".format(page, count))
    cursor.close()
    conn.close()

####执行所有模块
def run(self):
   conf = config(self)
   print "数据加载成功"
   conn = connect_db(conf)
   cursor = conn.cursor()
   delAll(conn, cursor)

   count = 1
   while 1 == 1:
       conn = connect_db(conf)
       cursor = conn.cursor()
       flag = insertAll(conn, cursor, conf, page=count)
       count=count+1
       if flag == 1:
           print "抽取数据结束！"
           return