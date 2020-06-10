import pandas as pd
import os
# from datetime import datetime, timezone
from datetime import datetime, timezone, timedelta
import time
import pytz
import re

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

if __name__ == "__main__":
    text = get_file()
    print('text: ', text)
    file = 'DAT_ASCII_AUDCAD_T_202001.zip'
    # matchObj = re.match(pattern, file)
    if file in text:
        print('file: ', file)
        rowList=re.compile("(?<={}\s)\d+-\d+-\d+\s\d+:\d+:\d+.\d+-\d+:\d+".format(file))

        row_arr = rowList.findall(text)
        # logging.info("len : {}".format(row_arr[0]))
        row = row_arr[-1]
        print('time: ', row)

        new_str = re.compile('DAT_ASCII_AUDCAD_T_202005.zip.*')
        aa=new_str.findall(text)
        # if aa:
        print('time: ', aa[-1])
        # foundYourWant = re.search("DAT_ASCII_AUDCAD_T_202001.zip\s(?P<contentYourWant>\S+)\n", text)
        # # foundYourWant = re.search("^DAT_ASCII_AUDCAD_T_202001.zip(?P<contentYourWant>\s).*$", text)
        # foundYourWant = re.search("^.*DAT_ASCII_AUDCAD_T_202001.zip(?P<contentYourWant>\s).*$", text)

        # print('foundYourWant: ', foundYourWant, aa)
        # if foundYourWant:
        #     contentYourWant = foundYourWant.group("contentYourWant")
        #     print("contentYourWant: ",contentYourWant)
        # if aa:
        #     haha = aa.group()
        #     print("haha: ",haha)
        #     haha = aa.group()
        #     print("haha: ",haha)
        
    # folder = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\2020\\AUDCAD\\DAT_ASCII_AUDCAD_T_202001.csv'
    # # testFile = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\2020\\AUDCAD\\DAT_ASCII_AUDCAD_T_202001.csv'
    # # f = os.stat(testFile).st_size
    # # print('long: {}'.format(f))
    # symbol=folder.split('\\')[-1]
    # symbol=symbol.split('_')[2].lower()
    # df=pd.read_csv(folder, header=None)
    # df.columns = ['time', 'bid','ask','volume']
    # df = df.drop(columns=['volume'])
    # est_tz = pytz.timezone('EST') # 标注时间的时区
    # df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H%M%S%f').dt.tz_localize(est_tz)
    # df['symbol'] = symbol

    # new_list = df[ df['time']>row]
    # print('end: ', new_list , df.max(0)['time'])