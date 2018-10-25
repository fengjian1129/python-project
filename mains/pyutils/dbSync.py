# -*- coding: utf-8 -*-
import sys
import requests
import json
import time
import datetime
from DataBaseSync import config, connect_db, insertAll
from DataBaseUpdate import backUpData, updateData
import re
def post(self):
    page = self
    url = 'http://10.30.45.30:8808'
    post = {'token': '7F24286285FE0D25399B6152D117D7F1',
            'page': page
           }
    result = requests.post(url, data=post)
    return json.loads(result.content)
def dbselect(self):
    conf = config(self)
    conn = connect_db(conf)
    sql = "select table_json from datasource_all where id ='1'"
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()[0]

    print json.loads(result)[0]['comment'].decode()
    cursor.close()
    conn.close()
if __name__ == '__main__':
    conf = config('test.json')
    conn = connect_db(conf)
    cursor = conn.cursor()
    norsql = "SELECT max(data_dictionary_key) FROM dim_table_catalog WHERE product_line = '{}' AND LENGTH(data_dictionary_key)>12".format("C2B")
    cursor.execute(norsql)
    # 最大值 table_id
    onemax = cursor.fetchone()
    if onemax[0] is None:
        print onemax[0]
    else:
        print "ssss-----{}".format(onemax[0])