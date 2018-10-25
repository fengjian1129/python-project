# -*- coding: utf-8 -*-
import sys
from mains.pyutils.tableIdUpdate import tableidexists
from mains.pyutils.DataBaseSync import config, connect_db



if __name__ == '__main__':
    conf = config('D:/project/dbsync/test.json')
    conn = connect_db(conf)
    tableidexists(conn)
    conn.close()