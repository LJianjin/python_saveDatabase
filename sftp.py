# -*- coding: utf-8 -*-
# from ftplib import FTP
# import schedule
import time
import datetime
import logging
import re
import os
import paramiko 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# def test():
#     now = datetime.datetime.now()
#     ts = now.strftime('%Y-%m-%d %H:%M:%S')
#     print(ts, "I'm working...")

# def run():
FTPIP= "209.126.123.58"
FTPPORT= 22
USERNAME= "ljwthurstongc"
USERPWD= "dgTn-v4pR_RfFf_"

# host="39.118.162.53"                                #sftp ip
# port=22                                             #sftp端口
# username="test"                                      #sftp用户名
# password="123456"                         #sftp密码
local='C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\test\\DAT_ASCII_AUDCAD_T_202004.zip'#存储路径
remote='/2020/AUDCAD/DAT_ASCII_AUDCAD_T_202004.zip'#目标文件所在路径
t = paramiko.Transport((FTPIP,FTPPORT))
t.connect(username = USERNAME,password = USERPWD)
sftp = paramiko.SFTPClient.from_transport(t)
# if os.path.isdir(local):                                       #判断本地参数是目录还是文件
#     for f in sftp.listdir(remote):                             #遍历远程目录
#          sftp.get(os.path.join(remote+f),os.path.join(local+f))#下载目录中文件
#          sf.close()
# else:
# if not os.path.exists(local):
#     os.makedirs(local)
# local = os.path.join(local, 'aa.txt')
sftp.get(remote, local)                                     #下载文件
t.close()
