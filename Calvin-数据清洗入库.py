'''
Author: Calvin
Date:2018-04-03
Finish_Date:
'''
'''
这段程序代码，主要目的是将别人提供的全国python和java招聘信息提取并存入数据库中.
主要通过以下几步：
1.提取json数据
2.数据清洗与整合
3.分表：一张职位薪水表：包含职位ID，职位名称，最低薪水，最高薪水，工作城市，所属公司ID，发布时间这七列；一张公司表：包含公司ID，公司全称，公司简称，公司所属城市这四列
4.python连接MySQL数据库并导入数据
5.实现数据查找
'''
import json
import pandas as pd
import warnings
import re
import pymysql

warnings.filterwarnings('ignore')

def json_transform_df(str,colunms):
    #将json的原始数据，转化为dataframe格式，输入为jason源文件和dataframe的列名
    temp = open(str+'.json', encoding='utf-8')  #设置以utf-8解码模式读取文件，encoding参数必须设置，否则默认以gbk模式读取文件，当文件中包含中文时，会报错
    data_list= json.loads(temp.read())
    df=pd.DataFrame()
    for i in range(len(columns)):
        df[columns[i]]=[data[columns[i]] for data in data_list]
    return df

def transform_date(str):
    #清洗日期数据，输入为一条 raw date data
    groups=re.match(r'(\d*)[_,-]*(\d*)[_,-]*(\d*)\s*[0-9,:]*',str).group(1)+'-'+\
           re.match(r'(\d*)[_,-]*(\d*)[_,-]*(\d*)\s*[0-9,:]*',str).group(2)+'-'+\
           re.match(r'(\d*)[_,-]*(\d*)[_,-]*(\d*)\s*[0-9,:]*',str).group(3)
    return groups

def extract_max_salary(str):
    #提取最大薪资，输入为一条raw salary data
    str=str.lower()
    max_salary=str[str.find('k')+1:]
    index=max_salary.find('k')
    if index==-1:
        max_salary=1000
        return int(max_salary)
    else:
        return int(max_salary[1:index])

def insert_Job_Salary_MySql(line,cursor):
    #插入薪资职称数据到数据库，输入为一条完整的薪资职称数据和数据库游标对象
    insert = "INSERT INTO Job_Salary \
       VALUES ('{}','{}','{}','{}',{},{},'{}')" .format(line[0],line[1],line[2],line[3],line[4],line[5],line[6])
    cursor.execute(insert)
    return

def insert_Company_Info_MySql(line,cursor):
    #插入公司数据到数据库，输入为输入为一条完整的公司数据和数据库游标对象
    insert = "INSERT INTO Company_Info(company_id,company_name,company_full_name) \
       VALUES ('{}','{}','{}')" .format(line[0],str(line[1]),str(line[2]))
    cursor.execute(insert)
    return

if __name__=="__main__":
    ###第一步：读取两个json表转为Dataframe格式
    columns=['company_name','company_full_names','create_time','salary','positionName','positionId','city']
    Python_job=json_transform_df('job_全国_Python_2018_03_29',columns)
    Java_job=json_transform_df('job_全国_Java_2018_03_29',columns)
    ####第二部：数据整合
    Job_Info=Python_job.append(Java_job)#整合量表
    Job_Info.reset_index(drop=True)
    Job_Info=Job_Info.drop_duplicates() #去重
    ###第三步数据清洗&分表：分成职位薪水表和公司表
        #生成公司ID，以公司全名为标识
    Company_Id=pd.DataFrame()
    Company_Id['company']=list(set(Job_Info['company_full_names']))
    Company_Id['company_id']=[index+1 for index in range(len(Company_Id['company']))]
        #生成职位薪水表
    Job_Salary=pd.merge(left=Job_Info[['positionId','positionName','salary','company_full_names','create_time','city']],right=Company_Id,left_on='company_full_names',right_on='company',how='left')
    Job_Salary.drop(['company_full_names','company'],axis=1,inplace=True)
    Job_Salary['Min_Salary']=Job_Salary['salary'].apply(lambda str:int(str.lower().split('k')[0]))
    Job_Salary['Max_Salary']=Job_Salary['salary'].apply(extract_max_salary)
    Job_Salary['Create_date']=Job_Salary.apply(lambda row:transform_date(row['create_time']),axis=1)
    Job_Salary.drop(['create_time','salary'],axis=1,inplace=True)
    print(Job_Salary)
        #生成公司表
    Company_Info=pd.merge(left=Company_Id[['company_id','company']],right=Job_Info[['company_name','company_full_names']],left_on='company',right_on='company_full_names',how='right')
    Company_Info.drop(['company'],axis=1,inplace=True)
    Company_Info=Company_Info.drop_duplicates().reset_index(drop=True)
    print(Company_Info)
    ###第四步：连接mysql，创建表，导入数据
        #连接mysql
    db=pymysql.connect("localhost","root","690723","job_salary_project1",use_unicode=True, charset="utf8")   #--位置（本地主机），用户名，密码，数据库名
        #创建表
    cursor = db.cursor()    #--使用 cursor() 方法创建一个游标对象 cursor
    cursor.execute("drop table if exists Job_Salary")  # --使用 execute() 方法执行 SQL，如果表存在则删除
    sql = """create table Job_Salary(
         position_id  varchar(25) NOT NULL,
         position_name  text,
         city text,
         company_id varchar(25) NOT NULL,
         min_salary decimal,
         max_salary decimal,
         create_date date)charset=utf8"""  #-- 使用预处理语句创建表,charsetd:定义字符集，尤其是数据里有中文的时候，需要将其转换为utf8
    cursor.execute(sql)
    cursor.execute("DROP TABLE IF EXISTS Company_Info")
    sql1 = """create table Company_Info(
         company_id  varchar(25) NOT NULL,
         company_name  varchar(255),
         company_full_name varchar(255))charset=utf8"""  #-- 使用预处理语句创建表
    cursor.execute(sql1)
        #导入数据
    Job_Salary.apply(lambda row:insert_Job_Salary_MySql(row,cursor),axis=1)
    Company_Info.apply(lambda row:insert_Company_Info_MySql(row,cursor),axis=1)
    db.commit()     #提交到数据库执行,不执行这句，数据库不会更新
        #查询
    inquery="select position_id,position_name,company_name,company_full_name,min_salary,max_salary,create_date " \
            "from Job_Salary left join Company_Info " \
            "on Job_Salary.company_id=Company_Info.company_id " \
            "where position_name like '%python%' and company_full_name like'%北京%'"   #联合两张表查询注册地在北京的公司发布的python工程师的职位和薪资
    try:
        cursor.execute(inquery)
        results = cursor.fetchall()
        print(results)
    except:
        print ("Error: unable to fetch data")
    db.close()