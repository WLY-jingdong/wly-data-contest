import pandas as pd
import collections
import pickle
import pymysql

def Get_Ensemble_information(date_, conn_jingdong):
    sql_query = 'SELECT cate FROM JData_Action_201604 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    cate_be_bought_4 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT cate FROM JData_Action_201603 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    cate_be_bought_3 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT cate FROM JData_Action_201602 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    cate_be_bought_2 = pd.read_sql(sql_query, conn_jingdong)
    cate_be_bought = pd.concat([cate_be_bought_2, cate_be_bought_3, cate_be_bought_4])
    cate_be_bought = collections.Counter(cate_be_bought.cate.values)
    f = open('cate_popularity.pkl', 'wb')
    pickle.dump(cate_be_bought, f)
    f.close()

    sql_query = 'SELECT brand FROM JData_Action_201604 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    brand_be_bought_4 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT brand FROM JData_Action_201603 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    brand_be_bought_3 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT brand FROM JData_Action_201602 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(date_)
    brand_be_bought_2 = pd.read_sql(sql_query, conn_jingdong)
    brand_be_bought = pd.concat([brand_be_bought_2, brand_be_bought_3, brand_be_bought_4])
    brand_be_bought = collections.Counter(brand_be_bought.brand.values)
    f = open('brand_popularity.pkl', 'wb')
    pickle.dump(brand_be_bought, f)
    f.close()

    sql_query = 'SELECT sku_id FROM JData_Action_201604 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(
        date_)
    sku_be_bought_4 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT sku_id FROM JData_Action_201603 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(
        date_)
    sku_be_bought_3 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT sku_id FROM JData_Action_201602 WHERE (type = 4) AND (time < DATE_FORMAT("{}","%y-%m-%d"))'.format(
        date_)
    sku_be_bought_2 = pd.read_sql(sql_query, conn_jingdong)
    sku_be_bought = pd.concat([sku_be_bought_2, sku_be_bought_3, sku_be_bought_4])
    sku_be_bought = collections.Counter(sku_be_bought.sku_id.values)
    f = open('sku_popularity.pkl', 'wb')
    pickle.dump(sku_be_bought, f)
    f.close()

    print('target ensemble information finished')

date_ = '2016-04-16'
conn_jingdong = pymysql.connect(host='localhost', port=3306, user='root', passwd='yao2376098', charset='latin1',
                                    db='jingdongdata')
Get_Ensemble_information(date_, conn_jingdong)
