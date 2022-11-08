# encoding: utf-8
import sys,os
path = os.path.abspath('..')
sys.path.append(path)

import logging
import datetime
import time
from utils.myencrypt import MyEncrypt

impala_config = [
    {'database': '109072951978887485234397269251054410', 'auth_mechanism': '114101097108116036179054561630739', 'host': '105109112097108097045107115045101116108046100110168280156193821916561483450692114', 'user': '114101097108116105170334972238081','timeout': 114101097108116105109054336134631, 'port': 114101097108116105077748056464367}, # cloud-local
    {'database': '109072951978887485234397269251054410', 'auth_mechanism': '114101097108116036179054561630739', 'host': '103050045105109112097108097045119097114101104111117115101046100110046605201530531388915285996483090','user': '114101097108116105170334972238081', 'timeout': 114101097108116105109101095100130, 'port': 114101097108116105077748056464367}, # g2-online
    {'database': '109072951978887485234397269251054410', 'auth_mechanism': '114101097108116036179054561630739', 'host': '158078230609096919452002127237580', 'user': '114101097108116105170334972238081','timeout': 114101097108116105109054336134631, 'port': 114101097108116105077748056464367}, # local-test
    {'database': '109072951978887485234397269251054410', 'auth_mechanism': '114101097108116036179054561630739', 'host': '105109112097108097045107115045101116108046100110168280156193821916561483450692114', 'user': '114101097108116105170334972238081','timeout': 114101097108116105109101095133519, 'port': 114101097108116105077748056464367} # cloud-origin
]

# 设置日志格式
LOG_FORMAT = "%(asctime)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', filemode='w', level=logging.WARNING, format=LOG_FORMAT, datefmt=DATE_FORMAT)

def check_table(table_name):
    f=open('check_query.sql')
    sql_str=f.read().format(table_name=table_name).strip()
    check_dict={}
    for i in [0,1]:
        # 两边环境执行结果记录
        check_dict[i]=execute_etl_sql(i,sql_str)

    if len(check_dict)==2:
        # 多个指标多个key
        for key in check_dict[0].keys():
            if check_dict[0][key]!=check_dict[1][key]:
                print '%s 两边不一致，cloud-env is %s，g2-env is %s' % (table_name,check_dict[0][key],check_dict[1][key])
    else:
        print '%s 对比数据缺失' % table_name


def execute_etl_sql(engine_num,sql_str):
    from impala.dbapi import connect as impala_connect
    # 创建连接对象
    code = MyEncrypt()
    impala_decode_config = code.getDecodeConfig(impala_config[engine_num])
    conn = impala_connect(**impala_decode_config)
    cur = conn.cursor()

    # 切分sql
    sql_list=sql_str.split(";")

    result_dict = {}
    engine_list = {0: "cloud-implala", 1: "g2-impala", 2: "local-impala", 3: "cloud-origin-impala"}
    try:
        with cur:
            engine_name=engine_list[engine_num]
            logging.critical("**************************************impala engine is %s **************************************"% engine_name)
            for i in range(len(sql_list)):
                sql=sql_list[i]
                if sql=='':
                    break
                logging.critical("sql scripts: \n %s \n" % sql)
                cur.execute(sql)
                data_list=[]
                try:
                   data_list = cur.fetchall()
                except Exception as e:
                   logging.critical(e)
                # 每条sql的第一条记录第一个字段
                # if isinstance(data_list[0][0], datetime.datetime):
                #    result_dict[i]=data_list[0][0].strftime('%Y-%m-%d %H:%M')
                # else:
                #    result_dict[i]=data_list[0][0]

                res_log = "query result: \n"
                # for data in data_list:
                #    for i in range(len(data)):
                #        res_log=res_log+str(data[i])+"\t"
                #    res_log=res_log+"\n"
                print data_list
                logging.critical(res_log)
    except Exception as e:
        logging.critical(e)
        print e
    finally:
        logging.critical('close conn \n \n \n')
        if conn:
            conn.close()
        return result_dict


def check_multi_table():
    table_list = []
    with open('table_list.txt', 'r') as f:
        while True:
            line = f.readline()
            if not line or line.strip('\n')=='':
                break
            table_list.append(line.strip('\n'))

    for table_name in table_list:
        check_table(table_name)
        time.sleep(3)


if __name__ == '__main__':
    # 单表核查 执行check_query.sql 控制台输出不一致信息
    # check_table('mysql_source.cars___car_source')

    # 多表核对 读取table_list.txt 执行check_query.sql 控制台输出不一致表清单
    # check_multi_table()

    # 单引擎CRUD my.log查看执行日志
    execute_etl_sql(0,"""
    INVALIDATE METADATA realtime_dwd.dwd_auction_bid 
    """)
