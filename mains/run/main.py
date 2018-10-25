# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from mains.pyutils.DataBaseSync import config, connect_db, delAll, insertAll
from mains.pyutils.DataBaseUpdate import backUpData, updateData, closeConn
from mains.pyutils.tableIdUpdate import tableidexists

def sync(self):
    conf = config(self)
    print "配置文件数据加载成功"
    conn = connect_db(conf)
    cursor = conn.cursor()
    delAll(conn, cursor)
    count = 1
    print "开始抽取数据加载datasource_all表"
    while 1 == 1:
        conn = connect_db(conf)
        cursor = conn.cursor()
        flag = insertAll(conn, cursor, conf, page=count)
        count = count + 1
        if flag == 1:
            print "抽取数据结束！"
            break

    print "开始同步数据dim_table_catalog和dim_datadictionary"
    conn = connect_db(conf)
    # 备份数据
    backUpData(conn)
    # 更新字典表
    updateData(conn)
    closeConn(conn)
    print "结束字典表的更新"
    print "开始更新table_id........."
    conn = connect_db(conf)
    tableidexists(conn)
    conn.close()

if __name__ == '__main__':
    sync('D:/project/dbsync/test.json')