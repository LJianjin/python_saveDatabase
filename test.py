import pandas as pd
import os
# from datetime import datetime, timezone
from datetime import datetime, timezone, timedelta
import time
import pytz
import re

# for x in range(0, 10):
#     if x%2==0:
#         # print('tiao')
#         continue
#     print(x)

# def get_hypertable():
#     if os.path.exists('file.txt'):
#         fp = open("file.txt", 'r')
#     else:
#         fp = open("file.txt", 'a+')
#     text = fp.read()
#     fp.close()
#     return text

# if __name__ == "__main__":
#     text = get_hypertable()
# #     # for x in text:
#     print('text:{}'.format(text))
# #     if ('forextick_audcad') not in text:
# #         print('not')
# #     else: 
# #         print('in')
#     csv_file = 'DAT_ASCII_[A-Z]+_T_[0-9]+.zip'
#     matchObj = re.match(csv_file, text)
# #     # file_str = "(?<={})\d+".format(csv_file)
#     print('aa', matchObj.group())
#     # aa = re.compile(r'(?DAT_ASCII_[A-Z]+_T_[0-9]+.csv\s)\d+')
#     aa = re.compile("(?<={}\s)\d+".format(matchObj.group()))
#     # re.compile(r"(?<=指定字4102符)\d+")
#     bb = aa.findall(text)
#     print('bb', int(bb[0]))
    # (?<=calibration=)
    # match= aa.search(text)
    # print (match.group(0))

def new_df(all_df, repeat):
    temp_df = all_df
    for index,row in repeat.iterrows():
        flag = False
        # print('in function, index: {}, row: {}, data: {}'.format(index, row, row['time']))
        repeat_list = temp_df[ temp_df['time'].isin([row['time']]) ]
        count = 0
        ok_flag = []
        false_flag = []
        for list_index,list_row in repeat_list.iterrows():
            # print('all: {} {}'.format(list_index, list_row))
            ok_flag.append(list_index)
            new_time = list_row['time'] + timedelta(microseconds=count)*1000
            # print('new Time: {}, all: {}'.format(new_time, temp_df[ temp_df['time'].isin([new_time]) ]))

            if count==0:
                count +=1
                continue
            elif len(temp_df[ temp_df['time'].isin([new_time]) ]) <1:
                temp_df.loc[list_index, ['time']] = new_time
                print('data: {}, count: {}'.format(temp_df.loc[list_index-1, ['time']],  count))
            else: 
                print('do', list_index, count)
                count +=1
                false_flag = repeat_list.loc[list_index:].index.tolist()
                flag = True
                break
            count +=1
        if flag:
            new_count = 0
            add_len = len(repeat_list)-count
            print('oh no, do not do: {}'.format(false_flag))
            for list_index,list_row in repeat_list.iterrows():
                if new_count < count:
                    print('check ', repeat_list, new_count+add_len)
                    temp_df.loc[list_index, ['bid']] = repeat_list.iloc[new_count+add_len, 1]
                    temp_df.loc[list_index, ['ask']] = repeat_list.iloc[new_count+add_len, 2]
                    new_count += 1
                else: 
                    print('dorp??????')
                    temp_df = temp_df.drop(index=false_flag)
                    break
    return temp_df
        # num = len(repeat_list)
        # count = 0
        # while count<num:
        #     count += 1
        #     new_time = row['time'] + timedelta(microseconds=count)*1000
        #     if len(temp_df[ temp_df['time'].isin([new_time]) ]) <2:
        #         temp_df.iloc[count-1:count].values[0][0] + timedelta(microseconds=count)*1000
        #     else: 
        #         flag = False
        #         break
        # print(index, row['bid']) # 输出每一行



# 创建时区UTC+00:00
# tz_utc = timezone(timedelta(hours=0))
# # 获得带时区的UTC时间
# current_time_utc = datetime.utcnow().replace(tzinfo=tz_utc)
# print("current_time_utc:", current_time_utc)

# # 创建时区UTC+08:00
# tz_utc = timezone(timedelta(hours=8))
# # 获得带时区的UTC+08:00时间
# current_time_utc_8 = datetime.utcnow().replace(tzinfo=tz_utc)
# print("current_time_utc_8:", current_time_utc_8)

# # 本地时间
# current_time = datetime.now()
# print("current_time", current_time)

# # 本地时区
# local_time_zone = (datetime.now() - datetime.utcnow()).seconds / 3600
# print("local_time_zone", local_time_zone)

folder = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\2020\\AUDCAD\\DAT_ASCII_AUDCAD_T_202001.csv'
# testFile = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\2020\\AUDCAD\\DAT_ASCII_AUDCAD_T_202001.csv'
# f = os.stat(testFile).st_size
# print('long: {}'.format(f))
symbol=folder.split('\\')[-1]
symbol=symbol.split('_')[2].lower()
df=pd.read_csv(folder, header=None)
df.columns = ['time', 'bid','ask','volume']
df = df.drop(columns=['volume'])
est_tz = pytz.timezone('EST') # 标注时间的时区
df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
df['symbol'] = symbol
# print( 'open csv file: {}, data: {}, all: {}'.format(symbol, df.describe(), df))
# print( 'len csv file: \n {}'.format(df.shape))
df_list = df[df.duplicated(subset=['time'], keep=False) == True]
hahah = df_list.drop_duplicates(subset='time')

print( '所有重复 csv file: \n {}'.format(df_list))
# print( '所有重复 csv file: \n {}'.format(df_list.index.tolist()))
# print( '重复 csv file: \n {}'.format(hahah))
# print( '全部 csv file: \n {}'.format(df))
bb = df_list.iloc[0:1].values[0][0] + timedelta(microseconds=1)*1000
aa = hahah.iloc[-1:].values[0][0]
len_num = df_list.iloc[0:1].index.values[0]
# print( '行数: {}'.format(len_num))
# print( '得到这一行: {}'.format(df.iloc[[len_num]]))

# print( '这一个值: {}'.format(aa))
# print( 'len csv file: {}'.format(df[ df['time'].isin([aa]) ]))
# if len(df['time']) == len(set(df['time'])):
#     print('ok')
# else:
#     print('on ok')

# for index,row in hahah.iterrows():
#     print(index, row['bid']) # 输出每一行
back = new_df(df, hahah)

print('all back: {}'.format(back))



# data= '20200101 170000288'
# # datas = datetime.astimezone(timezone('US/Eastern')) 
# # print(datas)
# df.columns = ['time', 'bid','ask','volume']
# df = df.drop(columns=['volume'])
# print(df['time'])
# print(df['bid'])
# print(pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize('EST'))
# est=pd.to_datetime(data, format='%Y%m%d %H%M%S%f')

# print(df['time'].dt.replace(tzinfo=pytz.timezone('EST')).astimezone(pytz.timezone('EST')))

# tss1 = '20200202 170011202'

# timestr = '2019-01-14 15:22:18.123'
# datetime_obj = datetime.strptime(tss1, "%Y%m%d %H%M%S%f")
# obj_stamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
# print(obj_stamp)


# def est_to_local(est_time_str, est_format='%b %d, %Y %I:%M %p EST'):
#     est_tz = pytz.timezone('EST') # 标注时间的时区
#     local_tz = pytz.timezone('Asia/Chongqing') # 北京时区
#     utc_tz = pytz.timezone('UTC') # 北京时区
#     # local_format = "%Y-%m-%d %H:%M:%S"  # 所需要的时间打印格式
#     est_dt = datetime.strptime(est_time_str, est_format)
#     utc_dt=est_dt.replace(tzinfo=est_tz).astimezone(est_tz)
#     print('est', est_dt)
#     local_dt = est_dt.replace(tzinfo=est_tz).astimezone(local_tz) # 将原有时区换成我们需要的
#     utc_dt=est_dt.replace(tzinfo=est_tz).astimezone(utc_tz)
#     print('utc', utc_dt)
#     return local_dt

# print('local', est_to_local(data, '%Y%m%d %H%M%S%f'))

