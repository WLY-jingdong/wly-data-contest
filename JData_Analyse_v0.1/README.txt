单用户信息分析小工具（分析单一/特殊用户信息或者分析如何提取规则，Python3.6）：

先运行Get_Ensemable_information.py文件，这个文件生成每个商品被购买的次数，每个品牌被购买的次数和每个品类被购买的次数（体现商品、品牌和品类的热度）的文件，文件格式为pkl文件，该文件运行一次即可，往后不需要再运行。
Single_User_Analysis.py 用于分析单一用户的属性，行为，以及涉及商品的信息

使用方式很简单，仅仅需要执行时输入要检查的用户的User_id号即可，会显示出User的各个信息，然后和该User交互的商品的信息，其中有几个字段要讲解一下，各热度信息是统计对应品类，品牌和商品被购买次数，差评率，是否差评和评论数是统计最终的累计值，评论和差评的变化率等信息。

注意事项：运行时写入修改数据库的用户名，密码等信息，字符串格式。
