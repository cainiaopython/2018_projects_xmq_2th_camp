import pandas as pd
import datetime, time, collections, re
import dbUtil
data1 = pd.read_json(r"E:\upstudy1\datasource\job_Python_2018_03_29.json")
data2 = pd.read_json(r"E:\upstudy1\datasource\job_Java_2018_03_29.json")
df = pd.concat([data1, data2])
df = df.drop("positionId",1).sort_values(by=['city','company_full_names','company_name',"create_time","positionName","salary"]).drop_duplicates()

new_df = []
# 进行数据清理
for j in df.values:
    i = j.tolist()
    # 正则 ()取出来，[]是选择范围
    reStr = r"(\d{1,2})(以上)?([k|K]-(\d{1,2})[k|K])?"
    reEnd = re.findall(reStr, i[5])
    i[5] = reEnd[0][0]
    s = reEnd[0][3] if reEnd[0][3] != '' else 0
    i.append(s)
    try:
        # 转换时间
        t = time.strftime("%Y-%m-%d", time.strptime(i[3], "%Y_%m_%d %H:%M"))
        i[3] = t
    except:
        pass

    new_df.append(i)

dd = collections.defaultdict(dict)
for i in new_df:
    print(i)
    dd.setdefault(i[0], collections.defaultdict(list))
    dc = dd[i[0]]
    dc[(i[1],i[2])].append([i[3],i[4],i[5],i[6]])
    dd[i[0]] = dc
#
for i in dd.items():
    print(i[0],i[1])
for i in dd.items():
    print(i[0])
    sql1 = "insert into city(city_name) values ('{}')".format(i[0])
    cid = dbUtil.insertExe(sql1)
    for j in i[1].items():
        print(j[0][0],j[0][1],j[1])
        sql2 = "insert into company(full_name,simple_name) values ('{}','{}')".format(j[0][0],j[0][1])
        mid = dbUtil.insertExe(sql2)
        for k in j[1]:
            print(k)
            sql3 = "insert into positionInfo(city_id,company_id,position_name,salary_min,salary_max,create_time) values ({},{},'{}','{}','{}','{}')"\
                .format(cid,mid,k[1],k[2],k[3],k[0])
            mid = dbUtil.insertExe(sql3)




