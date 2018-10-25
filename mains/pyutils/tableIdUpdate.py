# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from DataBaseSync import config, connect_db
import re


def tableidexists(conn):
    cursor = conn.cursor()
    sql = "SELECT data_dictionary_key, product_line,duty_user_id,table_name  FROM dim_table_catalog WHERE product_line !=''"
    cursor.execute(sql)

    count =cursor.fetchall()
    print count
    if len(count) == 0:
        print "全量更新tableid"
    else:
        for i in range(len(count)):
            table_id = count[i][0]
            product_line = count[i][1]
            table_name = count[i][3]
            if product_line in table_id:
                continue
            else:
               norsql = "SELECT max(data_dictionary_key) FROM dim_table_catalog WHERE product_line = '{}' AND LENGTH(data_dictionary_key)<12".format(product_line)
               cursor.execute(norsql)
               # 最大值 table_id
               onemax = cursor.fetchone()[0]
               print onemax
               if onemax is None:
                   linesql = "SELECT product_line_id FROM metadata_product_line WHERE product_line_code='{}'" \
                       .format(product_line)
                   cursor.execute(linesql)
                   result = cursor.fetchone()
                   nextone = product_line + result[0] + "00001"
                   print nextone
               else:
                   # table_id后缀数字最大值提取
                   digit = re.findall(r"\d+[0-9]", onemax)[0]
                   # 获得最终+1之后的table_id
                   ss = str(int(digit) + 1)

                   if len(ss) == 6:
                       nextone = product_line + "00" + ss
                   else:
                       nextone = product_line + "0" + ss
                   print nextone
               updatesql = "update dim_table_catalog set data_dictionary_key='{}'" \
                           " where data_dictionary_key='{}'".format(nextone, table_id)
               cursor.execute(updatesql)
               print "更新dim_table_catalog表中的table_id成功"

               dictionarytableid(conn, cursor, table_id, nextone, table_name)
        print "更新完毕！"
        conn.commit()
        cursor.close()
def dictionarytableid(conn, cursor, b_id, a_id, table_name):
    sql = "update dim_data_dictionary set data_dictionary_key='{}' " \
          "where data_dictionary_key='{}'".format(a_id, b_id)
    cursor.execute(sql)
    conn.commit()
    print "更新dim_data_dictionary表中的table_id成功"

def run(self):
    conf = config(self)
    conn = connect_db(conf)
    tableidexists(conn)
    conn.close()
if __name__ == '__main__':
    run('D:/project/dbsync/test.json')