# -*- coding: utf-8 -*-
from ftplib import FTP
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
from io import StringIO
import pandas as pd
import schedule
import time
# import datetime
from datetime import datetime, timezone, timedelta
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
    # csv_file = 'DAT_ASCII_[A-Z]+_T_[0-9]+.zip'
    # logging.info('in ftp function')

    engine = create_engine('postgresql://postgres:Supwin999@#$@192.168.135.24:5432/testdata')
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
            # logging.info('file is: {}, match:{}'.format(file, matchObj))
            if matchObj:
                logging.info('*************************************************')
                logging.info( 'start import file: {}'.format(file))
                symbol = file.split('_')[2].lower()
                import_files = get_file()
                # logging.info('File info: {}'.format(import_files))
                create_tables = get_hypertable()
                # df_len = 0
                if file  not in import_files:
                    start =time.time()
                    myzip=ZipFile(os.path.join(subdir, file))
                    csv_file = '{}.csv'.format(os.path.splitext(file)[0])
                    f=myzip.open(csv_file)
                    # logging.info( 'open csv file: {}'.format(file))
                    df=pd.read_csv(f, header=None)
                    logging.info( 'open csv file: {}'.format(file))
                    df.columns = ['time', 'bid','ask','volume']
                    df = df.drop(columns=['volume'])
                    # df_len = len(df)
                    est_tz = pytz.timezone('EST') # 标注时间的时区
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    df['symbol'] = symbol
                    logging.info( 'symbol: {}'.format(symbol))
                    # est_tz = pytz.timezone('EST') # 标注时间的时区
                    # # utc_tz = pytz.timezone('UTC') # utc时间
                    # # logging.info( 'df["time"]: {}'.format(df['time']))
                    # # csv_time = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize('EST')
                    # # csv_time = datetime.datetime.strptime(csv_time.decode('ascii'), '%Y%m%d %H%M%S.%f')
                    # df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    # df['symbol'] = symbol
                    # logging.info( 'symbol: {}'.format(symbol))
                    # # df.to_sql('forex_tick', engine, if_exists='append',index=False,method='multi')

                    # output = StringIO()
                    # # ignore the index
                    # df.to_csv(output,sep='\t', index = False, header = False)
                    # output.getvalue()
                    # # jump to start of stream
                    # output.seek(0)
                    
                    # # connection = engine.raw_connection()
                    # # cursor = connection.cursor()
                    # end = time.time()
                    # logging.info( 'convert data time(second) : {}'.format(end-start))
                    # start =time.time()

                    # # checking table exist
                    # sql_table = 'CREATE TABLE IF NOT EXISTS \"public\".\"{}\" ( \"time\" TIMESTAMP with time zone NOT NULL,  \"bid\" numeric(18,9) NOT NULL,  \"ask\" numeric(18,9) NOT NULL, \"symbol\" varchar(50) NOT NULL);'.format(table_name_pre + symbol)
                    # engine.execute(sql_table)
                    # logging.info('checking table exist')

                    # # #checking index exist
                    # # sql_index = 'CREATE INDEX IF NOT EXISTS \"symbol_time\" ON \"public\".\"{}\" (  \"time\" DESC,  \"symbol\" ASC);'.format(table_name_pre + symbol)
                    # # engine.execute(sql_index)
                    # # logging.info('checking index exist')

                    # if (table_name_pre + symbol) not in create_tables:
                    #     # create hypertable
                    #     sql_hypertable = "SELECT create_hypertable('\"{}\"', 'time', if_not_exists => true);".format(table_name_pre + symbol)
                    #     logging.info('create hypertable: {}'.format(sql_hypertable))
                    #     engine.execute(sql_hypertable)
                    #     logging.info('create hypertable')
                    #     add_hypertable(table_name_pre + symbol)
                    # else:
                    #     logging.warn('already create hypertable')
                    

                    # # null value become ''
                    # cursor.copy_from(output,table_name_pre + symbol,null='',columns=['time', 'bid','ask','symbol'])
                    # connection.commit()

                    # end = time.time()
                    # count = len(df)
                    # logging.info( 'Running time: {} Seconds, insert count: {}, insert qos per second: {}'.format(end-start, count, count/(end-start)))
                    # all_count += count
                    # logging.info( 'all insert count now: {}'.format(all_count))
                    # all_end = time.time()
                    # logging.info( 'all spend time: {}'.format(str(datetime.timedelta(seconds=(all_end - all_start)))))
                    # add_file(file+ ' ' +str(len(df)))
                    # # cursor.close()
                else:
                    # rowList=re.compile("(?<={}\s)\d+".format(file))
                    rowList=re.compile("(?<={}\s)\d+-\d+-\d+\s\d+:\d+:\d+.\d+-\d+:\d+".format(file))
                    row_arr = rowList.findall(import_files)
                    # logging.info("len : {}".format(row_arr[0]))
                    row = row_arr[-1]
                    # logging.info( 'len: {}'.format(row))
                    
                    start =time.time()
                    myzip=ZipFile(os.path.join(subdir, file))
                    csv_file = '{}.csv'.format(os.path.splitext(file)[0])
                    f=myzip.open(csv_file)
                    df=pd.read_csv(f,header=None)
                    # df=pd.read_csv(os.path.join(subdir, file), header=None)
                    logging.info( 'open csv file: {}'.format(file))
                    df.columns = ['time', 'bid','ask','volume']
                    df = df.drop(columns=['volume'])
                    # df_len = len(df)
                    # if df_len==row:
                    #     logging.info('Databasce is not need updata')
                    #     continue
                    # df =  df[ df['time']>row]
                    est_tz = pytz.timezone('EST') # 标注时间的时区
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    df['symbol'] = symbol
                    logging.info('len: {}'.format(len(df)))

                    df = df[ df['time']>row]
                    # logging.info('time: {}, df: {}'.format(row, df))
                    logging.info( 'symbol: {}'.format(symbol))

                df_list = df[df.duplicated(subset=['time'], keep=False) == True]
                repeat_df = df_list.drop_duplicates(subset='time')
                # logging.info('all len: {}'.format(len(df)))

                if len(repeat_df) > 0:
                    df = duplicate_df(df, repeat_df)

                # logging.info('all len: {}'.format(len(df)))
                output = StringIO()
                # ignore the index
                df.to_csv(output,sep='\t', index = False, header = False)
                output.getvalue()
                # jump to start of stream
                output.seek(0)
                
                end = time.time()
                logging.info( 'convert data time(second) : {}'.format(end-start))
                start =time.time()

                sql_table = 'CREATE TABLE IF NOT EXISTS \"public\".\"{}\" ( \"time\" TIMESTAMP with time zone NOT NULL,  \"bid\" numeric(18,9) NOT NULL,  \"ask\" numeric(18,9) NOT NULL, \"symbol\" varchar(50) NOT NULL);'.format(table_name_pre + symbol)
                engine.execute(sql_table)
                logging.info('checking table exist')

                if (table_name_pre + symbol) not in create_tables:
                    # create hypertable
                    sql_hypertable = "SELECT create_hypertable('\"{}\"', 'time', if_not_exists => true);".format(table_name_pre + symbol)
                    logging.info('create hypertable: {}'.format(sql_hypertable))
                    engine.execute(sql_hypertable)
                    logging.info('create hypertable')
                    add_hypertable(table_name_pre + symbol)
                else:
                    logging.warn('already create hypertable')
                

                cursor.copy_from(output,table_name_pre + symbol,null='',columns=['time', 'bid','ask','symbol'])
                connection.commit()

                end = time.time()
                count = len(df)
                logging.info( 'Running time: {} Seconds, insert count: {}, insert qos per second: {}'.format(end-start, count, count/(end-start)))
                all_count += count
                logging.info( 'all insert count now: {}'.format(all_count))
                all_end = time.time()
                logging.info( 'all spend time: {}'.format(str(timedelta(seconds=(all_end - all_start)))))
                # now = datetime.now()  # 这是时间数组格式
                # otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                otherStyleTime = df.max(0)['time']
                if len(df)>0:
                    add_file(file+ ' '  +str(otherStyleTime))
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

def duplicate_df(all_df, repeat):
    temp_df = all_df
    for index,row in repeat.iterrows():
        flag = False
        count = 0
        ok_flag = []
        false_flag = []
        repeat_list = temp_df[ temp_df['time'].isin([row['time']]) ]
        logging.info( 'csv need duplicate, len: {}, index: {}'.format(len(repeat_list), index))
        for list_index,list_row in repeat_list.iterrows():
            ok_flag.append(list_index)
            new_time = list_row['time'] + timedelta(microseconds=count)*1000

            if count==0:
                count +=1
                continue
            elif len(temp_df[ temp_df['time'].isin([new_time]) ]) <1:
                temp_df.loc[list_index, ['time']] = new_time
            else: 
                count +=1
                false_flag = repeat_list.loc[list_index:].index.tolist()
                flag = True
                break
            count +=1
        if flag:
            new_count = 0
            add_len = len(repeat_list)-count
            for list_index,list_row in repeat_list.iterrows():
                if new_count < count:
                    temp_df.loc[list_index, ['bid']] = repeat_list.iloc[new_count+add_len, 1]
                    temp_df.loc[list_index, ['ask']] = repeat_list.iloc[new_count+add_len, 2]
                    new_count += 1
                else: 
                    temp_df = temp_df.drop(index=false_flag)
                    break
    return temp_df

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
    # ftpInit()
    run('2020')

