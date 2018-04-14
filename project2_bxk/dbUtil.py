import pymysql

def insertExe(sql):
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='project2', charset='utf8')
    cursor = conn.cursor()
    effect_row = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    # 获取自增id
    new_id = cursor.lastrowid
    return  new_id