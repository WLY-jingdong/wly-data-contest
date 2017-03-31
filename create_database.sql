CREATE DATABASE `jingdongdata` /*!40100 DEFAULT CHARACTER SET latin1 */;

CREATE TABLE `JData_Action_201602` (
  `user_id` int(11) DEFAULT NULL,
  `sku_id` int(11) DEFAULT NULL,
  `time` datetime DEFAULT NULL,
  `model_id` varchar(5) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `cate` int(11) DEFAULT NULL,
  `brand` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `JData_Action_201603` (
  `user_id` int(11) DEFAULT NULL,
  `sku_id` int(11) DEFAULT NULL,
  `time` datetime DEFAULT NULL,
  `model_id` varchar(45) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `cate` int(11) DEFAULT NULL,
  `brand` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `JData_Action_201604` (
  `user_id` int(11) DEFAULT NULL,
  `sku_id` int(11) DEFAULT NULL,
  `time` datetime DEFAULT NULL,
  `model_id` varchar(45) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `cate` int(11) DEFAULT NULL,
  `brand` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `JData_Comment` (
  `dt` datetime DEFAULT NULL,
  `sku_id` int(11) DEFAULT NULL,
  `comment_num` int(11) DEFAULT NULL,
  `has_bad_comment` int(11) DEFAULT NULL,
  `bad_comment_rate` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `JData_Product` (
  `sku_id` int(11) DEFAULT NULL,
  `attr1` int(11) DEFAULT NULL,
  `attr2` int(11) DEFAULT NULL,
  `attr3` int(11) DEFAULT NULL,
  `cate` int(11) DEFAULT NULL,
  `brand` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Action_201602.csv'   
into table JData_Action_201602    
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  

show variables like '%secure%';
SELECT @@global.secure_file_priv;

use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Product.csv'   
into table JData_Product    
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  

use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Comment.csv'   
into table JData_Comment   
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  


use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Action_201604/JData_Action_201604.csv'   
into table JData_Action_201604    
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  

use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Action_201603/JData_Action_201603.csv'   
into table JData_Action_201603    
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  

use jingdongdata;
load data infile '/Users/yuxiao/项目/bigdata/jdong/data/JData_Action_201603/JData_Action_201603_extra.csv'   
into table JData_Action_201603    
fields terminated by ','  optionally enclosed by '"' escaped by '"'   
lines terminated by '\n';  

SELECT count(*) FROM jingdongdata.JData_Action_201602;

use jingdongdata;
Select count(*) from JData_Action_201602 where cate =8;