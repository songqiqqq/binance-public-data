import pandas as pd
import pdb


close = pd.read_pickle('/data/1m_close.pickle') 


def get_retn(close, n):
    ret = (close/close.shift(n)-1).shift(-n).shift(-1)
    ret = ret.sub(ret.mean(axis=1), axis=0).div(ret.std(axis=1), axis=0)
    ret[ret>5] = 5
    ret[ret<-5] = -5
    ret.to_pickle('/data/1m_ret{}.pickle'.format(n))
    pdb.set_trace()
    return 

get_retn(close, 1)
get_retn(close, 5)
get_retn(close, 10)
get_retn(close, 30)



