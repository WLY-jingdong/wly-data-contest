import pandas as pd
import numpy as np
import pymysql
import matplotlib.pyplot as plt
import collections
from datetime import datetime

def tongji_fenbu(cate):
    # 统计15号之前品类cate被购买的日均分布
    sql_query = 'SELECT * FROM JData_Action_201604 WHERE (type = 4) AND (cate = {})  AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(cate)
    bought4= pd.read_sql(sql_query,conn_jingdong)
    sql_query = 'SELECT * FROM JData_Action_201603 WHERE (type = 4) AND (cate = {}) AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(cate)
    bought3 = pd.read_sql(sql_query,conn_jingdong)
    sql_query = 'SELECT * FROM JData_Action_201602 WHERE (type = 4) AND (cate = {}) AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(cate)
    bought2 = pd.read_sql(sql_query,conn_jingdong)
    # 由于历史原因，变成cate8 be buy，就不改了
    cate8_be_buy = pd.concat([bought2,bought3,bought4])
    # 转换为时间格式
    cate8_be_buy.time = pd.to_datetime(cate8_be_buy.time)
    cate8_be_buy.time = cate8_be_buy.time - datetime(2016,1,31)
    cate8_be_buy = pd.DataFrame(cate8_be_buy.values)
    cate8_be_buy = cate8_be_buy.rename(columns={0:'user_id',1:'sku_id',2:'time',3:'model_id',4:'type',5:'cate',6:'brand'})
    cate8_be_buy['day'] = cate8_be_buy.time.apply(lambda x:x.days)
    # 统计每一天的销售量
    c8 = collections.Counter(cate8_be_buy.day.values)
    print(c8)
    day_ = []
    day_sell = []
    for item in c8.items():
        day_.append(item[0])
        day_sell.append(item[1])
    fig = plt.gcf()
    fig.set_size_inches(18.5, 10.5)
    plt.bar(day_,day_sell,width=0.5,alpha =1 )
    plt.show()
   
# 查看8品类商品的日销售分布并绘图，如果要看4号商品，则将'8'换为'4'即可
tongji_fenbu('8')
