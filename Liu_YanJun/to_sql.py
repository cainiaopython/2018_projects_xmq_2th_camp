#-*- coding:utf-8 -*-

#功能：1.连接名为job的数据库
#     2.建立三个表，分别存储薪资、公司、城市相关信息
#     3.接受全信息的df、只包含城市信息的df和只包含公司信息的df，然后入库
#     4.更新数据库、关闭游标、关闭数据库

import pymysql
import pandas as pd
#from threading import Thread

def get_conn(db):#链接数据库
	conn = pymysql.connect(host = 'localhost', port = 3306, user = 'root', passwd = '', db = db, charset = 'utf8')
	return conn

def clear_table(cur, t_name):#清空表中数据
	#sql = 'delete from ' + t_name #删除表中所有内容，若表为空则会报错
	sql = 'truncate table ' + t_name
	'''
	delete语句和truncate语句的区别：
	对于auto_increment类型的值，使用delete语句清除后再添加数据不会从1开始，而是承接以前的数据值
	而使用truncate语句可以彻底清除
	'''
	cur.execute(sql)

def select_table(cur, t_name):#查询表中现有数据量
	sql = 'select * from ' + t_name
	return cur.execute(sql)#返回int类型的数据数量

#-----------!! 建立公司表和城市表的功能可以合并 !!-----------
def cre_comp_table(cur):#建立公司表,表名为company
	args = '''
	COMP_ID char(10) not null primary key,
	COMP_NAME varchar(40),
	COMP_FULL_NAME varchar(100)
	'''
	sql = 'create table if not exists company' + '(' + args + ') default charset=utf8'
	cur.execute(sql)
	if select_table(cur, 'company') > 0:#如果表中有内容，则清空数据
		clear_table(cur, 'company')

def cre_city_table(cur):#建立城市表,表名为city
	args = '''
	CITY_ID char(5) not null primary key,
	CITY_NAME varchar(20)
	'''
	sql = 'create table if not exists city' + '(' + args + ') default charset=utf8'
	cur.execute(sql)
	if select_table(cur, 'city') > 0:#如果表中有内容，则清空数据
		clear_table(cur, 'city')
#-----------!! END 建立公司表和城市表的功能可以合并 !!-----------

def cre_salary_table(cur): #建立工资表，表名为salary,并建立外键与公司和城市表关联
	args = '''
	    POSI_ID char(10) not null primary key,
        POSI_NAME varchar(40),
        SALARY varchar(30),
        MIN_SALA char(10),
        MAX_SALA char(10),
        COMP_ID char(10),
        CITY_ID char(10),
        CRE_DATE char(20),
        constraint FK_COMP_ID foreign key(COMP_ID) references company(COMP_ID),
        constraint FK_CITY_ID foreign key(CITY_ID) references city(CITY_ID)
	'''
	sql = 'create table if not exists salary' + '(' + args + ') default charset=utf8'
	#if not exists用于判断该表是否存在； charset很重要，不然会出现很多麻烦
	cur.execute(sql)
	if select_table(cur, 'salary') > 0:#如果表中有内容，则清空数据
		clear_table(cur, 'salary')

def data_to_table(df, df_type, cur):#根据不同的df和相应的df_type，插入到不同的表中
	if df_type is 'city':
		sql = "insert into city (CITY_ID, CITY_NAME) values(%s,%s)"
	elif df_type is 'company':
		sql = "insert into company (COMP_ID, COMP_NAME, COMP_FULL_NAME) values(%s,%s,%s)"
	else:
		sql = "insert into salary (POSI_ID, POSI_NAME, SALARY, MIN_SALA, \
				MAX_SALA, COMP_ID, CITY_ID, CRE_DATE) values(%s,%s,%s,%s,%s,%s,%s,%s)"

	for i in range(len(df)):
	#这样做有一个前提，就是三个df的列顺序跟表中的列顺序必须一一对应
		args = tuple(df.iloc[i])
		#print(args[0], type(args[0]))
		cur.execute(sql, args)

def data_to_db(df_list, type_list):
	conn = get_conn('job')#链接job数据库
	cur = conn.cursor()#获取游标
	cre_city_table(cur)#建立city表
	cre_comp_table(cur)#建立company表
	cre_salary_table(cur)#建立salary表
	for i in range(len(df_list)):#依次将各df的数据导入到相应表中
		data_to_table(df_list[i], type_list[i], cur)
	conn.commit()#更新
	cur.close()#关闭游标
	conn.close()#关闭数据库


