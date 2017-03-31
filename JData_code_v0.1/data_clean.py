import pandas as pd
import numpy as np
import os

# 将JData_201604_User中的数据用pandas读入
train_action_04 = pd.read('JData_Action_201604/JData_Action_201604.csv')
# unique方法，将所有User_id逐一列出，且不会重复，下面的for循环将4月份的有发生交互的用户逐一保存为一个csv文件，文件名为用户ID号
for user_id in train_action_04['user_id'].unique():
    train_action_04[train_action_04['user_id']==user_id].to_csv('JData_201604_User/'+str(user_id)+'.csv')

# 清洗规则１
# 先将JData_201604_User文件夹下的文件名列出
pathDir = os.listdir('JData_201604_User')
# 开始清洗，清洗没有对8品类商品进行购买，加购物车和关注的用户
for dir in pathDir:
    user = pd.read_csv('JData_201604_User/'+dir)
	# 2,5,4分别为加购物车，关注和购买
    user_type_2 = user[user['type']==2]
    user_type_5 = user[user['type']==5]
    user_type_4 = user[user['type']==4]
    if (8 not in user_type_2['cate'].unique()) and (8 not in user_type_4['cate'].unique()) and (8 not in user_type_5['cate'].unique()):
        os.remove('JData_201604_User/'+dir) # 原先的处理方式是直接删除文件，下面的修改方式是对文件名进行更改，加入.1标签表示该用户被清洗		
	# os.rename('JData_201604_User/'+dir,'JData_201604_User/'+dir.split('.')[0]+'.1.csv')
    else: continue

# 清洗规则2
# 清除在10号到15号没有与任何商品发生交互行为的用户
pathDir = os.listdir('JData_201604_User')
for dir in pathDir:
    user = pd.read_csv('JData_201604_User/'+dir)
    user['time'] = pd.to_datetime(user['time'])
    if(user['time'][len(user['time'])-1] < datetime(2016,4,10)):
        os.remove('JData_201604_User/'+dir)
        # os.rename('JData_201604_User/'+dir,'JData_201604_User/'+dir.split('.')[0]+'.1.csv')
	
# 清洗规则3
# 清洗在2016年4月13号后没有和8号品类商品有过交互的用户
pathDir = os.listdir('JData_201604_User')
for dir in pathDir:
    user = pd.read_csv('JData_201604_User/'+dir)
    user['time'] = pd.to_datetime(user['time'])
    if 8 not in user[user['time']>=datetime(2016,4,13)]['cate'].unique():
        os.remove('JData_201604_User/'+dir)
	# os.rename('JData_201604_User/'+dir,'JData_201604_User/'+dir.split('.')[0]+'.1.csv')

# 经过上述3个规则的清洗，可以将用户数量从85000降到18400
