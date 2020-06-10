# -*- coding: utf-8 -*-
from ftplib import FTP
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
from io import StringIO
import pandas as pd
import schedule
import time
import datetime
import logging
import re
import os
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# def test():
#     now = datetime.datetime.now()
#     ts = now.strftime('%Y-%m-%d %H:%M:%S')
#     print(ts, "I'm working...")

# def run():


FTPIP= "50.30.37.179"
FTPPORT= 21
USERNAME= "ljwthurstongc"
USERPWD= "dgTn-v4pR_RfFf_"
ftp = FTP()
AllFile = []
def ftpInit():
    logging.info('in ftp function')

    # ftp调试输出等级，越高越详细
    ftp.set_debuglevel(0) 
    ftp.connect(FTPIP, FTPPORT, 20)  
    #通过账号和密码登录FTP服务器 
    ftp.login(USERNAME,USERPWD)  

    file = '/2020'
    getAllFTPFile(file)
    #关闭FTP客户端连接
    ftp.close()

    for x in AllFile:
        if re.match('^AUDCAD', x.split('/')[2]):
            count = 0
            while True:
                try:
                    logging.info('All File: {}'.format(x)) # 显示目录下所有目录的信息
                    if ftpDown(x):
                        break
                except Exception as e:
                    logging.info('error {}'.format(e.args))
                finally:
                    count += 1
                    logging.info('count: {}'.format(count))
            # ftpDown(x)
            # pass

    logging.info('done')
    

#遍历ftp目录，获取所有文件路径
def getAllFTPFile(file_dir = '/'):
    if file_dir != ftp.pwd():
        ftp.cwd(file_dir)

    dirlist = []
    ftp.dir('.', dirlist.append)   #对当前目录进行dir()，将结果放入列表

    for x in dirlist:
        if x.startswith('d') and re.match('^A', x.split(' ')[-1]):
            newDir = file_dir + '/' + x.split(' ')[-1]
            getAllFTPFile(newDir)
        else:
            AllFile.append(file_dir + '/' + x.split(' ')[-1])

#将ftp文件保存到相应的目录
def ftpDown(file):
    #使用新连接，可及时关闭连接，避免client数过多
    ftpDo = FTP()
    ftpDo.set_debuglevel(0)
    # 设置超时时间，避免下载过长没有反应脚本停掉
    ftpDo.connect(FTPIP, FTPPORT, 20)  
    ftpDo.login(USERNAME,USERPWD)

    try:
        Local_dir = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\one'
        ftpDri = file.split('/')
        dirs = os.path.join(Local_dir, *ftpDri[:-1])

        #检查目录
        if not os.path.exists(dirs):
            os.makedirs(dirs)

        DownLocalFilename = os.path.join(dirs, ftpDri[-1])
        #为准备下载到本地的文件，创建文件对象  
        f = open(DownLocalFilename, 'wb')  
        #从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小  
        ftpDo.retrbinary('RETR ' + file , _retr_callback(f,  __name__) , 1024)

        # f.close()
        # ftpDo.close()
        logging.info('File {} is down done'.format(file))
        return True
    except Exception as e:
        logging.info('error {}'.format(e.args))
        return False
    finally:
        f.close()
        ftpDo.close()
    # pass

def _retr_callback(fo,  mod_name):
    return lambda data: _show_progress(data, fo,  mod_name)

def _show_progress(data, fo,  mod_name):
    fo.write(data)
    # logging.info('file: {}'.format(fo.name))

def run(file_year):

    year = file_year
    table_name_pre = 'forextick_'
    folder = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\{}\\'.format(year)
    # file_profix = 'DAT_ASCII_{}_T_{}'
    pattern = 'DAT_ASCII_[A-Z]+_T_[0-9]+.zip'
    # logging.info('in ftp function')

    engine = create_engine('postgresql://postgres:Supwin999@#$@sg.gaawei.com:45432/test_forex')
    connection = engine.raw_connection()
    cursor = connection.cursor()

    all_count = 0

    all_start = time.time()
    for subdir, dirs, files in os.walk(folder):
        logging.info( 'root: {}'.format(subdir))
        # print 'subdir: {}'.format(dirs)
        # print 'files: {}'.format(files)
        for file in files:
            
            matchObj = re.match(pattern, file)
            if matchObj:
                logging.info('*************************************************')
                logging.info( 'start import file: {}'.format(file))
                symbol = file.split('_')[2].lower()
                import_files = get_file()
                create_tables = get_hypertable()
                if file  not in import_files:
                    start =time.time()
                    myzip=ZipFile(os.path.join(subdir, file))
                    csv_file = '{}.csv'.format(os.path.splitext(file)[0])
                    f=myzip.open(csv_file)
                    logging.info( 'open csv file: {}'.format(csv_file))
                    df=pd.read_csv(f,header=None)
                    df.columns = ['time', 'bid','ask','volume']
                    df = df.drop(columns=['volume'])
                    est_tz = pytz.timezone('EST') # 标注时间的时区
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    df['symbol'] = symbol
                    logging.info( 'symbol: {}'.format(symbol))
                    # df.to_sql('forex_tick', engine, if_exists='append',index=False,method='multi')

                    output = StringIO()
                    # ignore the index
                    df.to_csv(output,sep='\t', index = False, header = False)
                    output.getvalue()
                    # jump to start of stream
                    output.seek(0)
                    
                    # connection = engine.raw_connection()
                    # cursor = connection.cursor()
                    end = time.time()
                    logging.info( 'convert data time(second) : {}'.format(end-start))
                    start =time.time()

                    # checking table exist
                    sql_table = 'CREATE TABLE IF NOT EXISTS \"public\".\"{}\" ( \"time\" TIMESTAMP with time zone NOT NULL,  \"bid\" numeric(18,9) NOT NULL,  \"ask\" numeric(18,9) NOT NULL, \"symbol\" varchar(50) NOT NULL);'.format(table_name_pre + symbol)
                    engine.execute(sql_table)
                    logging.info('checking table exist')

                    # #checking index exist
                    # sql_index = 'CREATE INDEX IF NOT EXISTS \"symbol_time\" ON \"public\".\"{}\" (  \"time\" DESC,  \"symbol\" ASC);'.format(table_name_pre + symbol)
                    # engine.execute(sql_index)
                    # logging.info('checking index exist')

                    if (table_name_pre + symbol) not in create_tables:
                        # create hypertable
                        sql_hypertable = "SELECT create_hypertable('\"{}\"', 'time', if_not_exists => true);".format(table_name_pre + symbol)
                        logging.info('create hypertable: {}'.format(sql_hypertable))
                        engine.execute(sql_hypertable)
                        logging.info('create hypertable')
                        add_hypertable(table_name_pre + symbol)
                    else:
                        logging.warn('already create hypertable')
                    

                    # null value become ''
                    cursor.copy_from(output,table_name_pre + symbol,null='',columns=['time', 'bid','ask','symbol'])
                    connection.commit()

                    end = time.time()
                    count = len(df)
                    logging.info( 'Running time: {} Seconds, insert count: {}, insert qos per second: {}'.format(end-start, count, count/(end-start)))
                    all_count += count
                    logging.info( 'all insert count now: {}'.format(all_count))
                    all_end = time.time()
                    logging.info( 'all spend time: {}'.format(str(datetime.timedelta(seconds=(all_end - all_start)))))
                    add_file(file)
                    # cursor.close()
                else:
                    logging.warn('already import file: {}'.format(file))
    cursor.close()
    connection.close()

# already exist hypertable
def get_hypertable():
    if os.path.exists('hypertable.txt'):
        fp = open("hypertable.txt", 'r')
    else:
        fp = open("hypertable.txt", 'a+')
    # fp = open("hypertable.txt", 'r')
    text = fp.read()
    fp.close()
    return text

# already exist import file
def get_file():
    if os.path.exists('file.txt'):
        fp = open("file.txt", 'r')
    else:
        fp = open("file.txt", 'a+')
    # fp = open("file.txt", 'r')
    text = fp.read()
    fp.close()
    return text

def add_hypertable(table):
    fp = open("hypertable.txt",'a+')
    fp.write(table)
    fp.write('\n')
    fp.close()

def add_file(file):
    fp = open("file.txt",'a+')
    fp.write(file)
    fp.write('\n')
    fp.close()

if __name__ == "__main__":
    # schedule.every(10).seconds.do(test)
    # schedule.every(1).minutes.do(run)
    # # schedule.every().hour.do(job)
    # # schedule.every().day.at("10:30").do(job)
    # # schedule.every().monday.do(job)
    # # schedule.every().wednesday.at("13:15").do(job)
    
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    # ftpInit() # ftp下载文件
    run('2020')

