# -*- coding: utf-8 -*-
import sys
import requests
import json
import datetime
import time
def config(self):
    file = open(self, 'r')
    return json.load(file)
# 增加配置信息 /api/addapi  对应数据表 odps_dataworks_api_service
def addapi(conf):
    addjson = conf['addapi']
    list = []
    for i in range(len(addjson)):
        url = conf['url']
        uri = conf['uri']
        result = requests.post(url + uri + "addapi", json=addjson[i])
        list.append(result.content)
    return list
# 增加配置信息 /api/addcontrol 对应数据表 odps_dataworks_api_warn
def addcontrol(conf):
    addcontrol = conf['addcontrol']
    list = []
    for i in range(len(addcontrol)):
        url = conf['url']
        uri = conf['uri']
        result = requests.post(url + uri + "addcontrol", json=addcontrol[i])
        list.append(result.content)
    return list
# 增加告警对应通知人 /api/userwarn 对应数据表 odps_dataworks_api_warn_user
def addwarnuser(conf):
    adduser = conf['userwarn']
    list = []
    for i in range(len(adduser)):
        url = conf['url']
        uri = conf['uri']
        result = requests.post(url + uri + "userwarn", json=adduser[i])
        list.append(result.content)
    return list
# 触发对应配置的规则告警（邮件通知） /api/warnonce 对应数据表 odps_dataworks_api_warn（配置sql 发送出邮件）
def warnonce(conf):
    warn = conf['warnonce']
    list = []
    for i in range(len(warn)):
        url = conf['url']
        uri = conf['uri']
        result = requests.post(url + uri + "warnonce", data=warn[i])
        list.append(result.content)
    return list

# 测试请求/api/post  对应数据表 无（仅仅返回数据共请求方使用）
def apipost(conf):
    postdata = conf['post']
    url = conf['url']
    uri = conf['uri']
    result = requests.post(url + uri + "post", data=postdata)
    return result
def showlist(conf):
    get = conf['showlist']
    url = conf['url']
    uri = conf['uri']
    result = "wucan"
    if get == "GET":
        result = requests.get(url+uri+"showlist")

    return result
if __name__ == '__main__':
    conf = config('config.json')
    start = time.time()

    # 测试post请求数据
    result =apipost(conf)
    print result.content
    end = time.time()
    print (end-start)
    # result = showlist(conf)
    # print json.loads(result.content)
    # 测试增加配置信息
    # result = addapi(conf)
    # for i in range(len(result)):
    #     print result[i]
    # 测试增加告警信息
    # result = addcontrol(conf)
    # for i in range(len(result)):
    #     print result[i]
    # 测试发送告警信息
    # result = warnonce(conf)
    # for i in range(len(result)):
    #     print result[i]


