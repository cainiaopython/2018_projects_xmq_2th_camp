import pandas as pd
import json
import re

from pymongo import MongoClient


with open('c:\\db\\job_Python.json','r') as f:
    data = json.load(f)

df = pd.DataFrame(data)

df2=df.drop_duplicates()

def create_time(a):
    if '_' in a:
        a=a.split()[0]
        a=a.replace('_','-')
#       print a
#    else:
#       print a
    return a

def salary2(b):
    if '-' in b:
        b=b.split('-') #['15k','30k']
#        print b
        b=[int(x.lower().split('k')[0]) for x in b]
#        print b
    else:
#        print b
        b=re.findall(r"\d+",b)
        b=[int(b[0]),int(b[0])]
#        print b
    return b


q=[]
for i in range(0,len(df2)):
    y=salary2(df2['salary'].values[i])
    z=create_time(df2['create_time'].values[i])
    y.append(z)

    q.append(y)
#    print y

salary_min=[i[0] for i in q]
salary_max=[i[1] for i in q]
createtime2=[i[2] for i in q]
print salary_min
print salary_max
print createtime2
df3=df2.copy()
df3['salary_min']=salary_min
df3['salary_max']=salary_max
df3['create_time2']=createtime2

#len(df3)

client = MongoClient('localhost', 27017)

db = client.python_job

job=db.python_job_table

#print json.loads(df3.T.to_json()).values()

job.insert(json.loads(df3.T.to_json()).values())

print job.find().count()
job.close
