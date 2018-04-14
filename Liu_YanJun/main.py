# -*- coding:utf-8 -*-
import os

import pandas as pd

import data_clean_2 as dc
import to_sql as ts

def get_new_df(file_list):#调用data_clean_2中的data_clean函数，生成数据清洗和优化后的df
	df = dc.data_clean(file_list)
	return df

def data_to_db(df_list, type_list):
	ts.data_to_db(df_list, type_list)

def cre_city_df(df):#生成只包含城市信息的df并去重，用于将来入库
	df_c = df.copy()#使用copy函数而不是直接引用，防止修改对原df产生影响
	df_city = df_c.loc[:, ['city_id', 'city']]
	df_city = df_city.drop_duplicates(subset = 'city_id')#去重
	df_city['city_id'] = df_city['city_id'].astype(str)
	#为防止出现“AttributeError: 'numpy.int64' object has no attribute 'translate'”的问题，把所有numpy.int数值类型转换为str
	return df_city

def cre_comp_df(df):#生成只包含公司信息的df并去重，用于将来入库
	df_c = df.copy()#使用copy函数而不是直接引用，防止修改对原df产生影响
	df_comp = df_c.loc[:, ['comp_id', 'company_full_names', 'company_name']]
	df_comp = df_comp.drop_duplicates(subset = 'comp_id')#去重
	df_comp['comp_id'] = df_comp['comp_id'].astype(str)
	#为防止出现“AttributeError: 'numpy.int64' object has no attribute 'translate'”的问题，把所有numpy.int数值类型转换为str
	return df_comp

def cre_sala_df(df):#生成只包含工资信息的主df并去重，用于将来入库
	df_c = df.copy()
	df_sala = df_c.loc[:, ['positionId', 'positionName', 'salary', 'salary_min', 'salary_max', 
							'comp_id', 'city_id', 'create_time']]
	df_sala = df_sala.drop_duplicates(subset = 'positionId')#去重，positionid居然也有重复的
	df_sala[['positionId','comp_id','city_id']] = df[['positionId','comp_id','city_id']].astype(str)
	#为防止出现“AttributeError: 'numpy.int64' object has no attribute 'translate'”的问题，把所有numpy.int数值类型转换为str
	tmp = df_sala.iloc[0]
	for data in tmp:
		print(data, type(data))
	return df_sala

def main_func(file_list):
	df = get_new_df(file_list)#数据清洗和预处理
	df_city = cre_city_df(df)#生成用于city表的df
	df_comp = cre_comp_df(df)#生成用于company表的df
	df_sala = cre_sala_df(df)#生成用于主表的df
	df_list = [df_city, df_comp, df_sala]
	type_list = ['city', 'company', 'salary']
	data_to_db(df_list, type_list)#依次入库，type_list为判断标识，用于识别导入到哪个表
	#print(df_comp.head())	

if __name__ == '__main__':
	#java_file = r'D:\workspace\python训练营\2018第二期数据库初步\job_全国_Java_2018_03_29.json'
	#python_file = r'D:\workspace\python训练营\2018第二期数据库初步\job_全国_Python_2018_03_29.json'
	wk_path = os.getcwd()
	java_file = wk_path + '\\' + 'job_全国_Java_2018_03_29.json'
	python_file = wk_path + '\\' + 'job_全国_Python_2018_03_29.json'
	file_list = [java_file, python_file]
	main_func(file_list)
