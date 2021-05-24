import os
import zipfile
import pdb
import pandas as pd
import datetime
import numpy as np


datapath= '/data/data/futures/um/monthly/klines/' 
kind = 'read' #'write'
namelist = os.listdir(datapath)

coin_data_dict = {}
n = 0
for name in namelist:
    n += 1
    print(name)
    finalfolder = os.path.join(datapath, name, '1m')
    filename_list = [i for i in os.listdir(finalfolder) if i[-4:] == '.zip']
    fineal_filename_list = [os.path.join(finalfolder, i) for i in  filename_list]

    df_list = []
    for filename in fineal_filename_list:
        print(filename)
        if '2021' not in filename:
            print('jump')
            continue
        if kind == 'write':
            z = zipfile.ZipFile(filename, "r")
            content = z.read(z.namelist()[0])
            with open(filename.replace('.zip', '.csv'), 'wb') as f:
               f.write(content)
        elif kind == 'read':
            df = pd.read_csv(filename.replace('.zip', '.csv'), header=None) 
#            df.iloc[:,0] = (df.iloc[:,0]/1000).apply(datetime.datetime.fromtimestamp)
            df.iloc[:,6] = (df.iloc[:,6]/1000).apply(datetime.datetime.fromtimestamp)
            df.columns = ['start','open','high','low','close','vol','datetime','amount','tradenum','volbuy','amountbuy','none']
            df = df.drop(['start','none'], axis=1)
            df = df.set_index(['datetime'])
            df_list.append(df)
        else:
           raise ('not rocognized kind')
    if len(df_list)==0:
        continue
    
    df = pd.concat(df_list, axis=0).sort_index().astype(np.float32)
    df = df[~df.index.duplicated()]
    coin_data_dict[name] = df 

#    if name =='AAVEUSDT':
#        pdb.set_trace()
    print(n)

for part in ['open','high','low','close','vol','amount','tradenum','volbuy','amountbuy']:
    df = pd.concat({i:coin_data_dict[i][part] for i in coin_data_dict.keys()}, axis=0)
    df = df.unstack().T
    df.to_pickle('/data/1m_{}.pickle'.format(part))


