import numpy as np
import pandas as pd
import os
from datetime import datetime

# train_data = np.zeros(shape=(1, 16))
test_data = np.zeros(shape=(1, 15))

def GetTestData(User_Action_data , User_data, User_id):
    train_data = np.zeros(shape=(1, 15))
    for sku_id in User_Action_data['sku_id'].unique():
        data = []
        User_skuid = User_Action_data[(User_Action_data['sku_id'] == sku_id) ]#& (User_Action_data['time'] <= datetime(2016, 4, 16, 0, 0, 0))]
        data.append(User_id)
        data.append(sku_id)
        # 如果在13号之前没有经过交互，则无交互信息
        # if User_skuid.empty:
        #     data.append(User_data.sex.values[0])  # 用户性别
        #     data.append(User_data.age.values[0])  # 用户年龄
        #     data.append(User_data.user_lv_cd.values[0])  # 用户等级
        #     data.append((User_data['user_log_time'][User_data[User_data['user_id'] == User_id].index.values[0]].days) / 30)  # 用户注册时长
        #     count = 0
        #     while (count < 9):  # 补全信息
        #         data.append(0)
        #         count += 1
        #     # if sku_id in User_Action_data[User_Action_data['time'] > datetime(2016, 4, 13, 0, 0, 0)]['sku_id'].unique():
        #     #     a = User_Action_data[User_Action_data['sku_id'] == sku_id]
        #     #     if 4 in a[a['time'] >= datetime(2016, 4, 13)]['type'].unique():
        #     #         data.append(1)
        #     #     else:
        #     #         data.append(0)
        #     # else:
        #     #     data.append(0)
        #     train_data = np.vstack((train_data, np.array(data)))
        #     continue
        # 用户行为统计
        action = np.bincount(User_skuid['type'])
        data.append(User_data.sex.values[0])  # 用户性别
        data.append(User_data.age.values[0])  # 用户年龄
        data.append(User_data.user_lv_cd.values[0])  # 用户等级
        data.append((User_data['user_log_time'][User_data[User_data['user_id']==User_id].index.values[0]].days)/30) # 用户注册时长
        for i in range(1, len(action)):  # 将各个行为数据进行加入，初步
            # 针对浏览和点击进行分阈值处理
            if i == 1 or i == 6:
                if action[i] > 600:
                    data.append(4)
                elif action[i] > 400:
                    data.append(3)
                elif action[i] > 200:
                    data.append(2)
                else:
                    data.append(1)
            else:
                data.append(action[i])
        count = len(action)
        while (count <= 6):  # 补全信息
            data.append(0)
            count += 1

        # print(User_skuid['time'].max())
        a = int((str(User_skuid['time'].max()).split()[0]).split('-')[2])
        data.append(a)
        a = int((str(User_skuid['time'].min()).split()[0]).split('-')[2])
        data.append(a)

            # print(User_skuid)
            # print(sku_id)
            # print(User_skuid['time'].max(),"   ",User_skuid['time'].min())
            # exit(0)
        data.append((User_skuid['time'].max() - User_skuid['time'].min()).days + 1)
        # if sku_id in User_Action_data[ User_Action_data['time'] > datetime(2016, 4, 13, 0, 0, 0)]['sku_id'].unique():
        #     a = User_Action_data[User_Action_data['sku_id'] == sku_id]
        #     if 4 in a[a['time'] >= datetime(2016, 4, 13)]['type'].unique():
        #         data.append(1)
        #     else:
        #         data.append(0)
        # else:
        #     data.append(0)
        train_data = np.vstack((train_data, np.array(data)))
    print('user'+str(User_id)+' finish')
    return train_data

# user_172 = pd.read_csv('../JData_201604_User/172.csv')
# user_172['time'] = pd.to_datetime(user_172['time'])
# user_data = pd.read_csv('../JData_User/JData_User.csv',encoding = 'GBK')
# age_to_num = {'-1':0,'36-45岁':4,'16-25岁':2,'15岁以下':1,'26-35岁':3,'46-55岁':5,'56岁以上':6}
# user_data['age'] = user_data['age'].map(age_to_num)
# user_data['user_reg_dt'] = pd.to_datetime(user_data['user_reg_dt'])
# user_data['user_log_time'] = np.nan
# # print(user_data)
# # print(datetime(2016, 4, 15))
# user_data['user_log_time'] = datetime(2016, 4, 15) - user_data['user_reg_dt']
# print((user_data['user_log_time'].max()))
# print(type(user_172['time'][0]))
# a = str(user_172['time'].max()).split()[0]
# print(int(a.split('-')[2]))
# train_data = np.vstack((train_data, GetTrainData(user_172, user_data, 172)))
#
# print(train_data)
user_data = pd.read_csv('../JData_User/JData_User.csv', encoding='GBK')
age_to_num = {'-1': 0, '36-45岁': 4, '16-25岁': 2, '15岁以下': 1, '26-35岁': 3, '46-55岁': 5, '56岁以上': 6}
user_data['age'] = user_data['age'].map(age_to_num)
user_data['user_reg_dt'] = pd.to_datetime(user_data['user_reg_dt'])
user_data['user_log_time'] = datetime(2016, 4, 15) - user_data['user_reg_dt']

path_Dir = os.listdir('../JData_201604_User')
for dir in path_Dir:
    user = pd.read_csv('../JData_201604_User/'+dir)
    user['time'] = pd.to_datetime(user['time'])
    test_data = np.vstack((test_data, GetTestData(user, user_data, int(dir.split('.')[0]))))

print(test_data)
np.savetxt("test_data.txt",test_data)
