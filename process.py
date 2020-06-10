# -*- coding: utf-8 -*-
import time
import datetime
 
#demo1
def process_bar(percent, start_str='', end_str='', total_length=0):
    now = datetime.datetime.now()  # 这是时间数组格式
    otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bar = ''.join(['=='] * int(percent * total_length)) + ''
    bar = '\r' + otherStyleTime + ": " + start_str + bar.ljust(total_length) + ' {:0>4.1f}%|'.format(percent*100) + end_str
    
    print bar,
 
for i in range(101):
    time.sleep(0.05)
    end_str = '100%'
    process_bar(i/100, start_str='', end_str=end_str, total_length=40)
print ' '
print 'aa'
