import pymysql

global db
global cur
db = pymysql.connect(host='localhost', user='root',
                     password='demo123', port=3306)
cur = db.cursor()


def cnc():
    return db


def DBexist(name):
    cur.execute('show databases;')
    dbs = cur.fetchall()
    return (name,) in dbs


def tableExist(table_name,database_name='LandInfo'):
    cur.execute('show tables from %s;'%(database_name))
    dbs = cur.fetchall()
    return (table_name,) in dbs


def createDB(table_name, database_name='LandInfo'):
    cur.execute(
        'CREATE DATABASE IF NOT EXISTS ' + database_name + ' DEFAULT CHARACTER SET utf8')
    sql2 = 'CREATE TABLE IF NOT EXISTS '+database_name+'.'+table_name+'(\
    FloorOrder  VARCHAR(45)  NOT NULL,\
    name  VARCHAR(45) NULL,\
    address  VARCHAR(255) NULL,\
    size  VARCHAR(45) NULL,\
    FARatio  VARCHAR(45) NULL,\
    soldDate  VARCHAR(45) NULL,\
    price  VARCHAR(45) NULL,\
    owner  VARCHAR(45) NULL,\
    PRIMARY KEY ( FloorOrder));'
    cur.execute(sql2)


def insertData(kw, table_name, database_name='LandInfo'):

    keys = ', '.join(kw.keys())
    values = ', '.join(['%s']*len(kw))
    sql = 'INSERT INTO {database}.{table} ({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE '.format(
        database=database_name, table=table_name, keys=keys, values=values)
    update = ', '.join(['{key} = %s'.format(key=key) for key in kw])
    sql += update

    try:
        cur.execute(sql, tuple(kw.values())*2)
        db.commit()
    except:
        print('Faill to commit data to dbs')
        db.rollback()
        pass


def getLatestData(table_name='NanJing', databes_name='LandInfo'):
    sql = ' SELECT * FROM %s.%s order by id desc limit 1' % (
        databes_name, table_name)
    cur.execute(sql)
    one = cur.fetchone()
    return one

def getItemNum(table_name='NanJing', databes_name='LandInfo'):
    sql = ' SELECT * FROM %s.%s' % (
        databes_name, table_name)
    num=cur.execute(sql)
    return num


if __name__ == "__main__":
    # print(getLatestData())
    # createDB('NanJing')
    # kw = {'FloorOrder': 'NO.2019G02','FARatio': '2.5' , 'address': '东至规划檀山路，南至规划吴山路，西至规...，北至规划沿山路。',
    #       'name': '测试2 雨花台区西善桥街道宁芜铁路以南C2、C....2019G02)', 'owner': '上海漫珑企业管理有限公司', 'price': '280000万元', 'size': '67389.32平方米', 'soldDate': '2019/3/30'}
    # insertData(kw,'NanJing')

    print(tableExist('NanJing'))


    # s= 'xczxc%s%s'%('我',12)
    # print(s)
