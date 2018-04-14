#coding=utf-8
import pandas,sqlite3
import json,os,time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#读取json，转换为dataframe格式函数
def json_to_dataframe(path):
    Df= pandas.read_json(path)
    return pandas.DataFrame(Df)



#sql建职位表函数
def position_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS PositionTable (
    	'POSTN_ID' INTEGER NOT NULL PRIMARY KEY,
    	'POSTN_NAME'	TEXT NOT NULL,
    	'SALARY_MAX'	INTEGER NOT NULL,
    	'SALARY_MIN' INTEGER NOT NULL,
    	'COMP_ID' integer REFERENCES CompanyTable(COMP_ID),
    	'CITY_ID' integer REFERENCES CityTable(CITY_ID),
    	'CREATE_TIME'	TEXT DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()


#建城市表函数
def city_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS CityTable (
    	'CITY_ID'	INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	'CITY_NAME'	TEXT NOT NULL)''')

    conn.commit()

#建公司表函数
def company_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS CompanyTable (
    	'COMP_ID'	INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	'COMP_NAME'	TEXT NOT NULL,
    	'COMP_FULL_NAME'	TEXT DEFAULT NULL)''');

    conn.commit()

if __name__=='__main__':
    start=time.time()
    path_python = 'job_Python_2018_03_29.json'
    path_java = 'job_Java_2018_03_29.json'
    db_file = 'JobDB_from_LaGou.db'

    # dataframe读取python总表json
    Df_Posi_python = json_to_dataframe(path_python)

    # dataframe读取java总表json
    Df_Posi_java = json_to_dataframe(path_java)

    #python和java两表合并生成Df_Posi
    Df_Posi_list=[Df_Posi_python,Df_Posi_java]
    Df_Posi=pandas.concat(Df_Posi_list).reset_index(drop=True)


    # 提取城市列表，然后去重生成城市city_total
    city_total = Df_Posi['city'].drop_duplicates().reset_index(drop=True)

    # 提取公司列表，然后去重生成城市company_total
    company_total = Df_Posi[['company_full_names', 'company_name']].drop_duplicates().reset_index(drop=True)


    #生成城市为key，索引为value的字典，后续职位表中cityId需要从该字典提取替换
    vdict_city = {}
    for index in city_total.index:
        vdict_city[city_total[index]]=index

    # 生成公司全名+公司名为key，索引为value的字典，后续职位表中companyId需要用
    vdict_company = {}
    for index in company_total.index:
        vdict_company[company_total.ix[index]["company_full_names"] + company_total.ix[index]["company_name"]] = index

    # 处理职位表中的薪水列，替换k和K为‘000’，然后根据‘-’进行分组取出最大值和最小值。当然最大值注意if语句判断下，个别薪水没有最大值，直接赋值None
    Df_Posi_Salary_Min = Df_Posi['salary'].apply(
        lambda x: '0' if '-' not in x else x.replace('k', '000').replace('K', '000').split('-')[0])
    Df_Posi_Salary_Max = Df_Posi['salary'].apply(lambda x: x.replace('k', '000').replace('K', '000') if '-' not in x
                                                  else x.replace('k', '000').replace('K', '000').split('-')[1])

    # 处理职位表中的时间列，根据空格分隔取[0]从而只保留年月日，然后替换‘-’和‘_’为‘.’，即最后格式为‘2018.03.30’
    Df_Posi_Create_time = Df_Posi['create_time'].apply(lambda x: x.split(' ')[0].replace('-', '.').replace('_', '.'))

    #如果数据库存在，先删除
    if os.path.exists(db_file):
        os.remove(db_file)

    #连接数据库，如果不存在会新建该数据库
    conn=sqlite3.connect(db_file)

    #调用建表函数，新建三个表
    position_table(conn)
    city_table(conn)
    company_table(conn)

    #添加城市表数据
    for index in city_total.index:
        conn.execute("INSERT INTO CityTable(CITY_ID,CITY_NAME) VALUES(?,?)",(index,city_total[index]))
    print "insert city table successfully!"

    #添加公司表数据
    for index in company_total.index:
        conn.execute("INSERT INTO CompanyTable(COMP_ID,COMP_NAME,COMP_FULL_NAME) VALUES(?,?,?)",(index,company_total.ix[index]["company_name"],company_total.ix[index]["company_full_names"]))
    print "insert company table successfully!"

    #添加职位薪水表数据
    for index in Df_Posi.index:
        #职位薪水表中的companyID要从公司表匹配添加
        companyID=vdict_company[Df_Posi.ix[index]["company_full_names"] + Df_Posi.ix[index]["company_name"]]
        cityID=vdict_city[Df_Posi.ix[index]["city"]]


        #所有数据具备有，统一添加职位薪水表
        conn.execute("INSERT INTO PositionTable(POSTN_NAME,SALARY_MAX,SALARY_MIN,COMP_ID,CITY_ID,CREATE_TIME) VALUES(?,?,?,?,?,?)",
                     (Df_Posi.ix[index]["positionName"],int(Df_Posi_Salary_Max[index].strip('以上')),int(Df_Posi_Salary_Min[index]),companyID,cityID,Df_Posi_Create_time[index]))
    print "insert position table successfully!"
    #最后提交插入命令，然后关闭数据库
    conn.commit()
    conn.close()
    end=time.time()

    #统计下总耗时
    print "spend {} seconds!".format(end-start)






    ##根据城市名联合查询三张表，例如以南京为城市名查出25条，和dbbrowser查出的一致
    # i=0
    # conn = sqlite3.connect(db_file)
    # cursor = conn.execute(
    #     "SELECT PositionTable.POSTN_NAME,PositionTable.SALARY_MAX,PositionTable.SALARY_MIN,PositionTable.CREATE_TIME,CityTable.CITY_NAME,"
    #     "CompanyTable.COMP_NAME FROM PositionTable JOIN CityTable JOIN CompanyTable ON PositionTable.CITY_ID=CityTable.CITY_ID AND PositionTable.COMP_ID=CompanyTable.COMP_ID WHERE CityTable.CITY_NAME='南京'")
    # for row in cursor:
    #     for j in row:
    #         print j
    #     i+=1
    # print i
    # cursor.close()
    # conn.close()




