# -*- coding: utf-8 -*-
from ftplib import FTP
# import schedule
import time
from datetime import datetime, timedelta
import logging
import re
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# def test():
#     now = datetime.datetime.now()
#     ts = now.strftime('%Y-%m-%d %H:%M:%S')
#     print(ts, "I'm working...")

# def run():





FTPIP= "ftpsite.histdata.com"
FTPPORT= 21
USERNAME= "ljwthurstongc"
USERPWD= "dgTn-v4pR_RfFf_"
YEAR = datetime.now().year
MONTH = datetime.now().month
ftp = FTP()
AllFile = []
def ftpInit():
    logging.info('in ftp function')

    #创建ftp对象实例 
    # ftp = FTP()
    ftp.set_debuglevel(0)
    ftp.connect(FTPIP, FTPPORT)  
    logging.info('connect is ok')
    # ftp.set_pasv(False)
    #通过账号和密码登录FTP服务器 
    ftp.login(USERNAME,USERPWD)  
    # ftp.voidcmd('NOOP')
    logging.info('login is ok')
    # mlsd = ftp.mlsd(path='/2020/AUDCAD/')
    # for x in mlsd:
    #     logging.info('in mlsd {}'.format(x))

    if MONTH != 1:
        file = '/{}'.format(YEAR) # ftp dir
        # logging.info('test: {}'.format(ISFIRST))
        # logging.info('USERNAME {}'.format(USERNAME))
        # logging.info('isFirst {}'.format(ISFIRST))
        # if ISFIRST == 1:
        #     month_li = ['%d%02d' % (YEAR, MONTH-1)]
        # else:
        month_li = ['%d%02d' % (YEAR, MONTH), '%d%02d' % (YEAR, MONTH-1)]

        # ISFIRST = 0
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
    else:
        file = '/{}'.format(YEAR) # ftp dir
        month_li = [str(YEAR)+str(MONTH).rjust(2,'0')]
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表

        file = '/{}'.format(YEAR-1) # ftp dir
        month_li = [str(YEAR-1)+str(12)]
        getAllFTPFile(month_li, file) # 获取所有可以下载的文件的文件路径列表
    #关闭FTP客户端连接
    # test_size(AllFile[0])
    ftp.close()
    # pass

    logging.info('all file: {}, len: {}'.format(AllFile, len(AllFile)))

    return
    # logging.info('in dir: {}'.format(dirlist)) # 显示目录下所有目录的信息
    for x in AllFile:
        if re.match('^AUDC', x.split('/')[2]):
            count = 0
            while True:
                try:
                    logging.info('All File: {}'.format(x)) # 显示目录下所有目录的信息
                    if ftpDown(x, count):
                        break
                except Exception as e:
                    logging.info('error {}'.format(e.args))
                finally:
                    count += 1
                    logging.info('count: {}'.format(count))
            # ftpDown(x)
            # pass

    

    #如果参数 pasv 为真，打开被动模式传输 (PASV MODE) ，
    #否则，如果参数 pasv 为假则关闭被动传输模式。
    #在被动模式打开的情况下，数据的传送由客户机启动，而不是由服务器开始。
    #这里要根据不同的服务器配置
    #	ftp.set_pasv(0)
    # print('welcome', ftp.getwelcome) # 打印欢迎信息
    # file = '/2020/AUDCAD'
    # ftp.cwd(file) # 设置FTP当前操作的路径
    # dirlist = []
    # # dir_res = []
    # ftp.dir('.', dirlist.append)   #对当前目录进行dir()，将结果放入列表
    # nlst = ftp.nlst()
    # pwd = ftp.pwd()
    # logging.info('in dir: {}'.format(dirlist)) # 显示目录下所有目录的信息
    # logging.info('in nlst: {}'.format(nlst)) # 获取目录下的文件
    # logging.info('in pwd: {}'.format(pwd)) #返回当前所在位置

    #在FTP连接中切换当前目录 
    #	CURRTPATH= "/home1/ftproot/ybmftp/testupg/payment"
    #	ftp.cwd(CURRTPATH)  
    
    # DownRoteFilename=AllFile[0]
    # DownLocalFilename = AllFile[0].split('/')[-1]
    # #为准备下载到本地的文件，创建文件对象  
    # f = open(DownLocalFilename, 'wb')  
    # #从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小  
    # ftp.retrbinary('RETR ' + DownRoteFilename , f.write , 1024)  

    # DownRoteFilename=AllFile[1]
    # DownLocalFilename = AllFile[1].split('/')[-1]
    # #为准备下载到本地的文件，创建文件对象  
    # f = open(DownLocalFilename, 'wb')  
    # #从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小  
    # ftp.retrbinary('RETR ' + DownRoteFilename , f.write , 1024)  

    logging.info('done')
    #关闭下载到本地的文件  
    #提醒：虽然Python可以自动关闭文件，但实践证明，如果想下载完后立即读该文件，最好关闭后重新打开一次 
    # f.close()  

# def test_size(name):
#     ftp.voidcmd('TYPE I')
#     ftpSize = ftp.size(name)   
#     print("get: {}".format(name))
#     print('filesize [{}]'.format(ftpSize))

#遍历ftp目录，获取所有文件路径
#遍历ftp目录，获取所有文件路径
def getAllFTPFile(month_list, file_dir = '/'):
    if file_dir != ftp.pwd():
        ftp.cwd(file_dir)

    text = get_down()
    
    for x in ftp.mlsd(file_dir):
        new_name = file_dir + '/' + x[0]
        # logging.info('mlsd: {}'.format( x))
        # logging.info('info: {}'.format(x[1]['type']))
        if x[1]['type'] == 'dir' and re.match('^AUDC', x[0]):
            newDir = new_name
            getAllFTPFile(month_list, newDir)
        elif x[1]['type'] == 'file':
            month_str = '|'.join(month_list)
            re_name = 'DAT_ASCII_[A-Z]+_(M1|T)+_({})+.(txt|zip)+'.format(month_str)
            # logging.info('str: {}, file: {}'.format(re_name, x[0]))
            match = re.match(re_name, x[0])

            re_str = re.compile(x[0])
            all_list = re_str.findall(text)

            if match and len(all_list)==0:
                AllFile.append({'name': new_name, 'modify': x[1]['modify'], 'size':  x[1]['size']})

            elif match and len(all_list)>0:
                file_info = all_list.split(' ')
                logging.info("down file info: {}".format(file_info))
                if file_info[1] != x[1]['modify'] or file_info[2] != x[1]['size']:
                    AllFile.append({'name': new_name, 'modify': x[1]['modify'], 'size':  x[1]['size']})
                    # AllFile.append(new_name)
            # if aa:
            # print('time: ', aa)
            # file_month = x[0].split('_')[-1].split('.')[0]
            # if file_month in month_list:
                # AllFile.append(file_dir + '/' + x[0])
    # logging.info('info: {}'.format( ftp.dir()))
    # logging.info('nlst: {}'.format( ftp.nlst()))
    
    # dir_mlsd = dict(ftp.mlsd('.'))
    # logging.info('mlsd: {}'.format(dir_mlsd))
    # logging.info('retrlines: {}'.format( ftp.retrlines('MLSD')))
    # dir_info = dict(ftp.retrlines('MLSD'))   #对当前目录进行dir()，将结果放入列表
    # logging.info('retrlines: {}'.format(ftp.retrlines('MLSD')))
    
    # dirlist = []
    # ftp.dir('.', dirlist.append)   #对当前目录进行dir()，将结果放入列表
    
    # for x in dirlist:
    #     file_name =  x.split(' ')[-1]
    #     if x.startswith('d') and re.match('^AUDCAD', file_name):
    #     # if x.startswith('d'):
    #     # if x.startswith('d'):
    #         newDir = file_dir + '/' + file_name
    #         getAllFTPFile(month_list, newDir)
    #     else:
    #         file_month = file_name.split('_')[-1].split('.')[0]
    #         if file_month in month_list:
    #             AllFile.append(file_dir + '/' + file_name)

# already exist import file
def get_down():
    if os.path.exists('downFile.txt'):
        fp = open("downFile.txt", 'r')
    else:
        fp = open("downFile.txt", 'a+')
    # fp = open("file.txt", 'r')
    text = fp.read()
    fp.close()
    return text


#将ftp文件保存到相应的目录
def ftpDown(file, flag):
    ftpDo = FTP()
    ftpDo.set_debuglevel(0)
    ftpDo.connect(FTPIP, FTPPORT, 30)  
    # ftp.set_pasv(False)
    #通过账号和密码登录FTP服务器 
    ftpDo.login(USERNAME,USERPWD)
    try:
        Local_dir = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\one'
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
            #print '\b'*30,'download process:%.2f%%'%(float(cmpsize)/fsize*100),
            # logging.info('download process:%.2f%%'%(float(cmpsize)/fsize*100))
            process_bar(float(cmpsize)/fsize, start_str='', end_str='100%', total_length=25) # 输出进度条
        lwrite.close()
        ftpDo.voidcmd('NOOP')
        ftpDo.voidresp()
        # conn.close()

        # 旧下载函数
        # DownLocalFilename = os.path.join(dirs, ftpDri[-1])
        # #为准备下载到本地的文件，创建文件对象  
        # f = open(DownLocalFilename, 'wb')  
        # #从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小  
        # ftpDo.retrbinary('RETR ' + file , _retr_callback(f,  __name__) , 1024)

        # f.close()
        # ftpDo.close()
        logging.info('File {} is down done'.format(file))
        return True
    except Exception as e:
        print(' ')
        logging.info('error down {}'.format(e.args))
        return False
    finally:
        # lwrite.close()
        # if lwrite:
        #     lwrite.close()
        ftpDo.close()
    # pass

# 进度条函数
def process_bar(percent, start_str='', end_str='', total_length=0):
    now = datetime.datetime.now()  # 这是时间数组格式
    otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bar = ''.join(['=='] * int(percent * total_length)) + ''
    bar = '\r' + otherStyleTime + " : " + start_str + bar.ljust(total_length) + ' {:0>4.1f}%|'.format(percent*100) + end_str
    print(bar, end='', flush=True)

def _retr_callback(fo,  mod_name):
    return lambda data: _show_progress(data, fo,  mod_name)

def _show_progress(data, fo,  mod_name):
    fo.write(data)
    # ftp.voidcmd('TYPE I')
    # logging.info('file: {}'.format(fo.name))

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
    ftpInit()
