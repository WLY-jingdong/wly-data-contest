import numpy as np
import pandas as pd
import os
from datetime import datetime
from threading import Thread
from threading import main_thread
# 采用两线程进行数据集构造
# 但好像速度没什么变化，后面可以尝试多进程，让CPU跑满
# 参数解释 Path_User_Action是交互数据的文件路径，Path_User_Data是用户数据的文件路径，OpNum用于表明线程执行的是奇数文件名的处理还是偶数文件名的处理
def GetTrainData(Path_User_Action, Path_User_Data, OpNum):
    train_data = np.zeros(shape=(1, 18)) # 构造18维数据，其中有效维度13维，label 1维，加商品ID,用户ID,品类ID和品牌ID
    user_data = pd.read_csv(Path_User_Data, encoding='GBK')# 先对用户数据进行处理，将GBK类型的字符转换为数值，用map函数实现
    age_to_num = {'-1': 0, '36-45岁': 4, '16-25岁': 2, '15岁以下': 1, '26-35岁': 3, '46-55岁': 5, '56岁以上': 6}
    user_data['age'] = user_data['age'].map(age_to_num)
    user_data['user_reg_dt'] = pd.to_datetime(user_data['user_reg_dt'])# 将注册时间转化为datetime格式
    user_data['user_log_time'] = datetime(2016, 4, 15) - user_data['user_reg_dt'] # 获得注册时长，默认到2016年4月15号

    path_Dir = os.listdir(Path_User_Action) # 开始进行数据集构造
    for dir in path_Dir:
        if OpNum == 1:
            if int(dir.split('.')[0]) % 2 == 0: # OpNum == 1是处理偶数名文件
                user = pd.read_csv(Path_User_Action + dir)
                user['time'] = pd.to_datetime(user['time'])
                train_data = np.vstack((train_data, UserItemInfo(user, user_data, int(dir.split('.')[0])))) # 用numpy的vstack方法将抽取出来的数据堆叠成样本集
            else:
                continue
        else:
            if not int(dir.split('.')[0]) % 2 == 0:
                user = pd.read_csv(Path_User_Action + dir)
                user['time'] = pd.to_datetime(user['time'])
                train_data = np.vstack((train_data, UserItemInfo(user, user_data, int(dir.split('.')[0]))))
            else:
                continue
    # print(train_data)
    # np.savetxt("train_data.txt", train_data)
    return train_data
# 该函数是对单个用户进行特征抽取构造训练集
# 输入参数，pandas类型的用户交互信息User_Action_data,pandas类型用户信息User_data,整型User_id是要处理的用户id号
def UserItemInfo(User_Action_data , User_data, User_id):
    train_data = np.zeros(shape=(1, 18))
    for sku_id in User_Action_data['sku_id'].unique(): # 对每个用户商品交互数据中，处理每个用户商品对
        data = []
        User_skuid = User_Action_data[(User_Action_data['sku_id'] == sku_id) & (User_Action_data['time'] < datetime(2016, 4, 13, 0, 0, 0))] # 数据属性抽取的时间是在4月1号-4月13号这个时段中的数据
        data.append(User_id) # 第一维是用户ID
        data.append(sku_id) # 第二维是商品ID
        data.append(User_Action_data[User_Action_data['sku_id'] == sku_id]['cate'].unique()[0]) # 第三维是品类ID
        data.append(User_Action_data[User_Action_data['sku_id'] == sku_id]['brand'].unique()[0]) # 第四维是品牌ID
        # 如果在4月13号之前这个商品和该用户无任何交互，则无交互信息，暂时处理为先直接补零
        if User_skuid.empty: 
            data.append(User_data.sex.values[0])  # 第五维是用户性别
            data.append(User_data.age.values[0])  # 第六维用户年龄
            data.append(User_data.user_lv_cd.values[0])  # 第七维用户等级
            data.append((User_data['user_log_time'][User_data[User_data['user_id'] == User_id].index.values[0]].days) / 30)  # 第八维用户注册时长,单位为月份
            count = 0
            while (count < 9):  # 补全剩余的交互信息，全部补零
                data.append(0)
                count += 1
			# 下面这段是打标签，如果在13号之后该商品被实际购买，就是4这个行为出现，则打标签为1，否则打标签为0
            if sku_id in User_Action_data[User_Action_data['time'] > datetime(2016, 4, 13, 0, 0, 0)]['sku_id'].unique():
                a = User_Action_data[User_Action_data['sku_id'] == sku_id]
                if 4 in a[a['time'] >= datetime(2016, 4, 13)]['type'].unique():
                    data.append(1)
                else:
                    data.append(0)
            else:
                data.append(0)
            train_data = np.vstack((train_data, np.array(data)))
            continue
        # 用户行为统计，下面是当4月1号-4月13号与用户有交互时的处理方式
        action = np.bincount(User_skuid['type'])
        data.append(User_data.sex.values[0])  # 用户性别
        data.append(User_data.age.values[0])  # 用户年龄
        data.append(User_data.user_lv_cd.values[0])  # 用户等级
        data.append((User_data['user_log_time'][User_data[User_data['user_id']==User_id].index.values[0]].days)/12) # 用户注册时长
        for i in range(1, len(action)):  # 将各个行为数据进行加入，初步
            # 针对浏览和点击进行分阈值处理，因为浏览量点击量会远超其他行为，所以按阈值进行保存
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
        while (count <= 6):  # 补全信息，处理的时候会发现有些商品用户交互数据没有点击行为，这样会导致bincount函数计算不到6维数据，很奇怪，这里做一个补全操作避免错误
            data.append(0)
            count += 1
		# 下面就是进行交互时间数据的填入
        a = int((str(User_skuid['time'].max()).split()[0]).split('-')[2])
        data.append(a)
        a = int((str(User_skuid['time'].min()).split()[0]).split('-')[2])
        data.append(a)
        data.append((User_skuid['time'].max() - User_skuid['time'].min()).days + 1) # 注意这里要加1，避免出现0天的情况，交互至少为1天
		# 打标签过程
        if sku_id in User_Action_data[ User_Action_data['time'] > datetime(2016, 4, 13, 0, 0, 0)]['sku_id'].unique():
            a = User_Action_data[User_Action_data['sku_id'] == sku_id]
            if 4 in a[a['time'] >= datetime(2016, 4, 13)]['type'].unique():
                data.append(1)
            else:
                data.append(0)
        else:
            data.append(0)
        train_data = np.vstack((train_data, np.array(data)))
    # 处理过程提示
	print('user'+str(User_id)+' finish')
    return train_data

# 自定义线程类
class MyThread(Thread):
    def __init__(self, Path_User_Action, Path_User_Data, OpNum):
        Thread.__init__(self)
        self.Path_User_Data = Path_User_Data
        self.Path_User_Action = Path_User_Action
        self.train_data = np.zeros(shape=(1, 18))
        self.OpNum = OpNum
    def run(self):
        self.train_data = GetTrainData(self.Path_User_Action,self.Path_User_Data,self.OpNum)
    def get_result(self):
        return self.train_data

if __name__ == '__main__':
    thd1 = MyThread('../JData_201604_User/', '../JData_User/JData_User.csv', 1)
    thd2 = MyThread('../JData_201604_User/', '../JData_User/JData_User.csv', 2)
    thd1.start()
    thd2.start()
    thd1.join()
    thd2.join()
	# 将两个线程的处理结果进行整合，得到最终的训练集
    train_data = np.vstack((thd1.get_result(), thd2.get_result()))
    np.savetxt("train_data_multithread.txt", train_data)
