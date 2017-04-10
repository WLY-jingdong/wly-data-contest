import pandas as pd
import pickle
import collections
import pymysql
import warnings

warnings.filterwarnings("ignore")

def Analyse_Single_User_data(User_id):
    sku_popularity = pickle.load(open('sku_popularity.pkl', 'rb'))
    cate_popularity = pickle.load(open('cate_popularity.pkl', 'rb'))
    brand_popularity = pickle.load(open('brand_popularity.pkl', 'rb'))

    User_data = pd.read_csv('JData_User.csv', encoding='gbk')
    Comment_data = pd.read_csv('JData_Comment.csv', encoding='gbk')
    Comment_data.dt = pd.to_datetime(Comment_data.dt)
    print('用户个人信息： User_id, 年龄, 性别, 用户等级, 注册时间')
    print(User_data[User_data.user_id == User_id].values[0])
    conn_jingdong = pymysql.connect(host='localhost', port=3306, user='root', passwd='yao2376098', charset='latin1',
                                    db='jingdongdata')
    sql_query = 'SELECT * FROM JData_Action_201604 WHERE (user_id = {}) AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(
        str(User_id))
    User_action4 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT * FROM JData_Action_201603 WHERE (user_id = {}) AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(
        str(User_id))
    User_action3 = pd.read_sql(sql_query, conn_jingdong)
    sql_query = 'SELECT * FROM JData_Action_201602 WHERE (user_id = {}) AND (time < DATE_FORMAT("2016-4-16","%y-%m-%d"))'.format(
        str(User_id))
    User_action2 = pd.read_sql(sql_query, conn_jingdong)
    User_action = pd.concat([User_action2, User_action3, User_action4])
    print('总交互商品数：', len(User_action.sku_id.unique()))
    # User_action = User_action.rename(columns={0: 'user_id', 1: 'sku_id', 2: 'time', 3: 'model_id', 4: 'type', 5: 'cate', 6: 'brand'})
    for Sku_id in User_action.sku_id.unique():
        Sku = User_action[User_action.sku_id == Sku_id]
        Sku_comment = Comment_data[Comment_data.sku_id == Sku_id]
        Sku.time = pd.to_datetime(Sku.time)
        print(' ')
        print('商品ID: ', Sku_id)
        C = collections.Counter(Sku.type.values)
        print('    品类: ', Sku.cate.unique()[0], ' 品牌：', Sku.brand.unique()[0])
        print('    浏览：', C[1])
        print('    ', Sku[Sku.type == 1].time.values)
        print('    点击：', C[6])
        print('    ', Sku[Sku.type == 6].time.values)
        print('    加购：', C[2])
        print('    ', Sku[Sku.type == 2].time.values)
        print('    删购：', C[3])
        print('    ', Sku[Sku.type == 3].time.values)
        print('    关注：', C[5])
        print('    ', Sku[Sku.type == 5].time.values)
        print('    购买：', C[4])
        print('    ', Sku[Sku.type == 4].time.values)
        print('    最早交互时间：', Sku.time.max(), ' 最晚交互时间：', Sku.time.min(), ' 交互时长', (Sku.time.max()-Sku.time.min()).days + 1, '天')
        if(Sku_comment.empty): print('    无评论信息')
        else:
            print('    评论等级：', Sku_comment[Sku_comment.dt == Sku_comment.dt.max()]['comment_num'].values[0])
            print('    是否有差评：', Sku_comment[Sku_comment.dt == Sku_comment.dt.max()]['has_bad_comment'].values[0])
            print('    差评率：', Sku_comment[Sku_comment.dt == Sku_comment.dt.max()]['bad_comment_rate'].values[0])
            print('    评论数变化率:', (Sku_comment[Sku_comment.dt == Sku_comment.dt.max()]['comment_num'].values[0] - Sku_comment[Sku_comment.dt == Sku_comment.dt.min()]['comment_num'].values[0])/((Sku_comment.dt.max()-Sku_comment.dt.min()).days + 1))
            print('    差评变化率:', (Sku_comment[Sku_comment.dt == Sku_comment.dt.max()]['bad_comment_rate'].values[0] - Sku_comment[Sku_comment.dt == Sku_comment.dt.min()]['bad_comment_rate'].values[0])/((Sku_comment.dt.max()-Sku_comment.dt.min()).days + 1))
        print('    品类热度: ', cate_popularity[Sku.cate.unique()[0]])
        print('    品牌热度: ', brand_popularity[Sku.brand.unique()[0]])
        print('    商品热度: ', sku_popularity[Sku_id])

User_id = input('你好，请输入要检查的User_id:')
print('我是用户信息分析小助手，现在为你分析...')
print(' ')
print(' ')
Analyse_Single_User_data(int(User_id))
