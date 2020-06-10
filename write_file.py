import os

if __name__ == "__main__":
    DownLocalFilename = '/root/windows/2020/AUDCAD/DAT_ASCII_AUDCAD_T_202009.zip'
    # DownLocalFilename = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\down\\DAT_ASCII_AUDCAD_T_202009.zip'
    # Local_dir = 'C:\\Users\\Administrator\\Desktop\\jin\\doc\\pg\\down'
    # dirs = '/root/windows/AUDCAD/'
    #     if not os.path.exists(dirs):
    #         os.makedirs(dirs)
    if os.path.exists(DownLocalFilename):
        print os.path.exists(DownLocalFilename)
    lwrite=open(DownLocalFilename, 'wb')
    print lwrite
    data = 'I am file'
    lwrite.write(data.encode)
    lwrite.close()