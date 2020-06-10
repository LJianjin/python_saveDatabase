# -*- coding: utf-8 -*-
from ftplib import FTP
import schedule
import time
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test():
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print(ts, "I'm working...")

def run():
	pass




FTPIP= "50.30.37.179"
FTPPORT= 21
USERNAME= "ljwthurstongc"
USERPWD= "dgTn-v4pR_RfFf_"
def ftpDownload() :
 
	#创建ftp对象实例 
	ftp = FTP()  
 
	ftp.connect(FTPIP, FTPPORT)  
	#通过账号和密码登录FTP服务器 
	ftp.login(USERNAME,USERPWD)  
 
	#如果参数 pasv 为真，打开被动模式传输 (PASV MODE) ，
	#否则，如果参数 pasv 为假则关闭被动传输模式。
	#在被动模式打开的情况下，数据的传送由客户机启动，而不是由服务器开始。
	#这里要根据不同的服务器配置
#	ftp.set_pasv(0)
 
	#在FTP连接中切换当前目录 
#	CURRTPATH= "/home1/ftproot/ybmftp/testupg/payment"
#	ftp.cwd(CURRTPATH)
	DownRoteFilename = 'aa.txt'
	DownLocalFilename = 'bb.txt'
 
	#为准备下载到本地的文件，创建文件对象  
	f = open(DownLocalFilename, 'wb')  
	#从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小  
	ftp.retrbinary('RETR ' + DownRoteFilename , f.write , 1024)  
 
	#关闭下载到本地的文件  
	#提醒：虽然Python可以自动关闭文件，但实践证明，如果想下载完后立即读该文件，最好关闭后重新打开一次 
	f.close()  
 
	#关闭FTP客户端连接
	ftp.close()





if __name__ == "__main__":
    schedule.every(10).seconds.do(test)
    schedule.every(1).minutes.do(run)
    # schedule.every().hour.do(job)
    # schedule.every().day.at("10:30").do(job)
    # schedule.every().monday.do(job)
    # schedule.every().wednesday.at("13:15").do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
