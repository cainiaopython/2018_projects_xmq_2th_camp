#!/usr/bin/env python
# encoding: utf-8
'''
   建三个表（公司表，城市表，职位表）
   Created by puhui-hebin on 2018/4/2 10:40.
'''
import datetime

import pandas as pd;
import re
import sqlite3
import sys

# 如果不加这个，从json中读取出来的文本可能无法用==去比较
reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'filetest/job_recruitment.db';


# 计算运行方法耗时
def decorator(func):
    def wrap(*args):
        start_time = datetime.datetime.now();
        # 因为有的函数有参数所以传入可变参数
        result = func(*args);
        print (args[-1] + '耗时：{}').format(datetime.datetime.now() - start_time);
        # 有的方法有返回值，所以func的执行结果要返回
        return result;

    return wrap;


# 将文件中的json利用pandas按照dataframe二维数据读取出来
def readJson():
    pds_java = pd.read_json('filetest/job_全国_Java_2018_03_29.json')
    pds_python = pd.read_json('filetest/job_全国_Python_2018_03_29.json')
    # 合并两个pandas的数据,如果不设置ignore_index=true，那么虽然会合并，但是会发现一行是有两行数据，就是ix[0]会出现两行数据
    return pd.concat([pds_java, pds_python], ignore_index=True);


# 将薪水按照最高和最低分割出来（例如15K-30K，分割成15,30）
@decorator
def getSalaryByMaxAndMin(pds, description):
    # 最高薪水数组
    max_salary_array = [];
    # 最低薪水数组
    min_salary_array = [];
    # 取出薪水这一列的值，就是一个series，然后取出所有的值
    values = pds['salary'].values;
    # 匹配字符串中的连续数字
    patt = re.compile(r'\d+')
    for value in values:
        salary_array = re.findall(patt, value)
        # 存储最高薪资
        max_salary_array.append(salary_array[-1])
        # 存储最低薪资
        min_salary_array.append(salary_array[0])
    return max_salary_array, min_salary_array;


# 添加最高薪水以及最低薪水两列，且移除salary列
def modifyColumn(pds, salary):
    # 给pandas增加两列，salary_max(每家公司薪水最大值)和salary_min（每家公司薪水最小值）
    pds['salary_min'] = salary[-1]
    pds['salary_max'] = salary[0]
    # 丢弃 salary这一列
    return pds.drop(['salary'], axis=1)


# 对于时间的清洗
@decorator
def cleanTime(pds, description):
    pds['create_time'] = map(lambda x: x.split()[0], pds['create_time'].values);


@decorator
def create_db(description):
    # 连接数据库，如果不存在则会进行创建
    conn = sqlite3.connect(DB_NAME)
    conn.text_factory = str
    # 为了多次运行测试，故而创建之前先删除掉
    conn.execute('DROP TABLE if exists Company')
    conn.execute('DROP TABLE if exists City')
    conn.execute('DROP TABLE if exists Position')
    # 创建公司表和薪水表以及城市表，如果不存在的话
    # 下面如果要添加autoincrement，则前面必须是INTEGER，且不能加位数限制，例如INTEGER(11)这样的
    conn.execute(
        '''CREATE TABLE Company (company_id INTEGER NOT NULL primary key autoincrement unique ,
                                                company_full_names varchar(200) NOT NULL ,company_name varchar(100) NOT NULL);''');
    conn.execute(
        '''CREATE TABLE City (city_id INTEGER NOT NULL primary key autoincrement unique,city_name varchar(100) NOT NULL);''');
    conn.execute('''CREATE TABLE Position (positionId INTEGER(11) primary key unique NOT NULL  ,
                                                         positionName varchar(200) NOT NULL,salary_min INTEGER(11) NOT NULL,
                                                         salary_max INTEGER(11) NOT NULL,city_id INTEGER(11) not null, 
                                                         company_id INTEGER NOT NULL,create_time datetime default CURRENT_TIMESTAMP,
                                                         foreign key (company_id) references Company(company_id) on update no action ,
                                                         foreign key (city_id) references City(city_id) on update no action );''')
    conn.commit()
    conn.close()


# 保存公司信息
@decorator
def save_company_info(pds, description):
    conn = sqlite3.connect(DB_NAME)
    for i in pds.index:
        conn.execute(
            'INSERT INTO Company (company_full_names,company_name) VALUES (?,?)',
            (pds.ix[i]['company_full_names'], pds.ix[i]['company_name']));
    conn.commit()
    conn.close()


# 保存城市信息
@decorator
def save_city_info(pds, description):
    conn = sqlite3.connect(DB_NAME)
    for i in pds.index:
        # 切记execute的第二个参数是一个元组，即使只有一个参数也需要加上逗号
        # 否则会报错：Incorrect number of bindings supplied. The current statement uses 1, and there are 2 supplied
        conn.execute(
            'INSERT INTO City (city_name) VALUES (?)', (pds[i],));
    conn.commit()
    conn.close()


# 保存职位薪水信息
@decorator
def save_salary_info(pds, description):
    conn = sqlite3.connect(DB_NAME)
    for i in pds.index:
        conn.execute(
            'INSERT INTO Position (positionId,positionName,salary_min,salary_max,create_time,company_id,city_id) VALUES (?,?,?,?,?,?,?)',
            (pds.ix[i]['positionId'], pds.ix[i]['positionName'], pds.ix[i]['salary_min'],
             pds.ix[i]['salary_max'], pds.ix[i]['create_time'], pds.ix[i]['company_id'],
             pds.ix[i]['city_id']));
    conn.commit()
    conn.close()


# 获取根据公司全称+公司简称获取公司id的dict
def getCompanyDict():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('select * from Company')
    results = cursor.fetchall()
    dict_temp = {x[1] + x[2]: x[0] for x in results}
    cursor.close()
    conn.close()
    return dict_temp;


# 获取根据城市获取城市id的dict
def getCityDict():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('select * from City')
    results = cursor.fetchall()
    dict_temp = {x[1]: x[0] for x in results}
    cursor.close()
    conn.close()
    return dict_temp;


# 给pandas添加公司id自己城市id列
@decorator
def addComIdAndCityIdToPandas(pds, description):
    dict_com = getCompanyDict()
    dict_city = getCityDict()
    list_comid = []
    list_cityid = []
    for i in pds.index:
        list_comid.append(
            dict_com[pds.ix[i]['company_full_names'] + pds.ix[i]['company_name']])
        list_cityid.append(dict_city[pds.ix[i]['city']])
    pds['company_id'] = list_comid
    pds['city_id'] = list_cityid


# 简单的查询实例
def query():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''select * from Position join Company C on Position.company_id = C.company_id
    join City C2 on Position.city_id = C2.city_id where C.company_full_names = "北京旷视科技有限公司"''')
    results = cursor.fetchall()
    for row in results:
        print row[8];
    cursor.close()
    conn.close()


# 总调度方法
@decorator
def doAction(description):
    try:
        pds = readJson()
        salary = getSalaryByMaxAndMin(pds, '清洗薪资');
        pds_new = modifyColumn(pds, salary);
        # 去除重复职位
        pds_new = pds_new.drop_duplicates(['positionId']);
        cleanTime(pds_new, '清洗时间')
        # 获取去重后的城市的series
        pds_city = pds_new['city'].drop_duplicates()
        # 获取根据公司全名和公司简称去重后的pandas，因为那些重复行被去掉了，所以index也不是连续的了
        # 所以循环pds时候，不能range(len(pds))了，得根据其index来进行迭代了
        pds_company = pds_new.drop_duplicates(['company_full_names', 'company_name'], keep='first');
        create_db('创建数据库')
        save_company_info(pds_company, '保存公司信息')
        save_city_info(pds_city, '保存城市信息')
        addComIdAndCityIdToPandas(pds_new, '给pandas添加公司id和城市id列')
        save_salary_info(pds_new, '保存薪水信息')
    except Exception, e:
        print '运行出错了：{}'.format(repr(e))


if __name__ == '__main__':
    doAction('总体运行')
