# -*- coding: utf-8 -*-
from ftplib import FTP
from sqlalchemy import create_engine
from zipfile import ZipFile
from io import StringIO
import pandas as pd
import schedule
import time
from datetime import datetime, timezone, timedelta
import logging
import re
import os
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

FTPIP= "ftpsite.histdata.com"
FTPPORT= 21
USERNAME= "ljwthurstongc"
USERPWD= "dgTn-v4pR_RfFf_"
YEAR = datetime.now().year
MONTH = datetime.now().month
# MONTH = 1
# Local_dir = '/root/windows'
Local_dir = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\down'

ftp = FTP()
AllFile = []
def ftpInit():
    logging.info('in ftp function')

    #创建ftp对象实例 
    ftp.set_debuglevel(0)
    ftp.connect(FTPIP, FTPPORT)  
    # logging.info('connect is ok')
    #通过账号和密码登录FTP服务器 
    ftp.login(USERNAME,USERPWD)  
    logging.info('login is ok')

    if MONTH != 1:
        file = '/{}'.format(YEAR) # ftp dir
        month_li = ['%d%02d' % (YEAR, MONTH), '%d%02d' % (YEAR, MONTH-1)]
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
    else:
        file = '/{}'.format(YEAR) # ftp dir
        month_li = [str(YEAR)+str(MONTH).rjust(2,'0')]
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表

        file = '/{}'.format(YEAR-1) # ftp dir
        month_li = [str(YEAR-1)+str(12)]
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表

    #关闭FTP客户端连接
    ftp.close()
    # logging.info(AllFile)
    # return True
    for x in AllFile:
        if re.match('^AUDC', x.split('/')[2]):
            count = 0
            while True:
                try:
                    logging.info('Downing file: {}'.format(x)) # 显示目录下所有目录的信息
                    if ftpDown(x, count):
                        break
                except Exception as e:
                    logging.info('error {}'.format(e.args))
                finally:
                    count += 1
                    logging.info('count: {}'.format(count))
                    if count >= 10:
                        logging.info('retry too many: {}'.format(count))
                        break
        # pass
    
    logging.info('Down done')

    logging.info('begin to do run')
    # run('2020')
    if MONTH != 1:
        # file = '/{}'.format(YEAR) # ftp dir
        month_li = ['%d%02d' % (YEAR, MONTH), '%d%02d' % (YEAR, MONTH-1)]
        # getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
        run(str(YEAR), month_li)
    else:
        # file = '/{}'.format(YEAR) # ftp dir
        month_li = [str(YEAR)+str(MONTH).rjust(2,'0')]
        # getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
        run(str(YEAR), month_li)

        # file = '/{}'.format(YEAR-1) # ftp dir
        month_li = [str(YEAR-1)+str(12)]
        # getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
        run(str(YEAR-1), month_li)


#遍历ftp目录，获取所有文件路径
def getAllFTPFile(month_list, file_dir = '/'):
    if file_dir != ftp.pwd():
        ftp.cwd(file_dir)

    dirlist = []
    ftp.dir('.', dirlist.append)   #对当前目录进行dir()，将结果放入列表

    for x in dirlist:
        file_name =  x.split(' ')[-1]
        if x.startswith('d') and re.match('^A', file_name):
            newDir = file_dir + '/' + file_name
            getAllFTPFile(month_list, newDir)
        else:
            file_month = file_name.split('_')[-1].split('.')[0]
            if file_month in month_list:
                AllFile.append(file_dir + '/' + file_name)

#将ftp文件保存到相应的目录
def ftpDown(file, flag):
    ftpDo = FTP()
    ftpDo.set_debuglevel(0)
    ftpDo.connect(FTPIP, FTPPORT, 30)  
    #通过账号和密码登录FTP服务器 
    ftpDo.login(USERNAME,USERPWD)
    try:
        # Local_dir = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\one'
        ftpDri = file.split('/')
        dirs = os.path.join(Local_dir, *ftpDri[:-1])
        DownLocalFilename = os.path.join(dirs, ftpDri[-1])
        if not os.path.exists(dirs):
            os.makedirs(dirs)

        ftpDo.voidcmd('TYPE I')
        fsize=ftpDo.size(file)   
        if fsize==0 : # localfime's site is 0
            return True
             
        #check local file isn't exists and get the local file size   
        lsize=0
        if os.path.exists(DownLocalFilename):
            # if not flag:
            local_file_size = os.stat(DownLocalFilename).st_size
            if not flag and local_file_size==fsize:
                lsize = local_file_size
            elif not flag:
                os.remove(DownLocalFilename)
            else:
                lsize = local_file_size
            
                 
        if lsize >= fsize:   
            logging.info('local file is bigger or equal remote file')
            return True  
        blocksize=1024 * 256
        cmpsize=lsize
        conn = ftpDo.transfercmd('RETR '+file, lsize)
        if lsize:
            lwrite=open(DownLocalFilename, 'ab')
        else:
            lwrite=open(DownLocalFilename, 'wb')
        while True:
            data=conn.recv(blocksize)
            if not data:
                print(' ')
                break
            lwrite.write(data)
            cmpsize+=len(data)
            process_bar(float(cmpsize)/fsize, start_str='', end_str='100%', total_length=25) # 输出进度条
        lwrite.close()
        ftpDo.voidcmd('NOOP')
        ftpDo.voidresp()
        logging.info('File {} is down done'.format(file))
        return True
    except Exception as e:
        print(' ')
        logging.info('error down {}'.format(e.args))
        return False
    finally:
        ftpDo.close()

# 进度条函数
def process_bar(percent, start_str='', end_str='', total_length=0):
    now = datetime.now()  # 这是时间数组格式
    otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bar = ''.join(['=='] * int(percent * total_length)) + ''
    bar = '\r' + otherStyleTime + " : " + start_str + bar.ljust(total_length) + ' {:0>4.1f}%|'.format(percent*100) + end_str
    print(bar, end='', flush=True)

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

# 去重
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


# 数据库保存
def run(file_year, month_list):

    year = file_year
    table_name_pre = 'forextick_'
    # folder = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\{}\\'.format(year)
    folder = os.path.join(Local_dir, year)
    # file_profix = 'DAT_ASCII_{}_T_{}'
    # pattern = 'DAT_ASCII_[A-Z]+_T_[0-9]+.zip'
    month_str = month_list.join('|')
    pattern = 'DAT_ASCII_[A-Z]+_T_{}\{1\}.zip'.format(month_str)
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
                create_tables = get_hypertable()
                if file  not in import_files:
                    start =time.time()
                    myzip=ZipFile(os.path.join(subdir, file))
                    csv_file = '{}.csv'.format(os.path.splitext(file)[0])
                    f=myzip.open(csv_file)
                    df=pd.read_csv(f, header=None)
                    logging.info( 'open csv file: {}'.format(file))
                    df.columns = ['time', 'bid','ask','volume']
                    df = df.drop(columns=['volume'])
                    est_tz = pytz.timezone('EST') # 标注时间的时区
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    df['symbol'] = symbol
                    logging.info( 'symbol: {}'.format(symbol))
                else:
                    rowList=re.compile("(?<={}\s)\d+-\d+-\d+\s\d+:\d+:\d+.\d+-\d+:\d+".format(file))
                    row_arr = rowList.findall(import_files)
                    row = row_arr[-1]
                    
                    start =time.time()
                    myzip=ZipFile(os.path.join(subdir, file))
                    csv_file = '{}.csv'.format(os.path.splitext(file)[0])
                    f=myzip.open(csv_file)
                    df=pd.read_csv(f,header=None)
                    logging.info( 'open csv file: {}'.format(file))
                    df.columns = ['time', 'bid','ask','volume']
                    df = df.drop(columns=['volume'])
                    est_tz = pytz.timezone('EST') # 标注时间的时区
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
                    df['symbol'] = symbol
                    logging.info('len: {}'.format(len(df)))

                    df = df[ df['time']>row]
                    logging.info( 'symbol: {}'.format(symbol))

                df_list = df[df.duplicated(subset=['time'], keep=False) == True]
                repeat_df = df_list.drop_duplicates(subset='time')

                if len(repeat_df) > 0:
                    df = duplicate_df(df, repeat_df)

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


if __name__ == "__main__":
    ftpInit()

    