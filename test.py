'''
Created on Aug 7, 2016

@author: vermosen
'''

import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.arima_process import arma_generate_sample
import pandas
import matplotlib



def test(db):

    np.random.seed(12345)
    arparams = np.array([.75, -.25])
    maparams = np.array([.65, .35])
    
    arparams = np.r_[1, -arparams]
    maparam = np.r_[1, maparams]
    nobs = 250
    y = arma_generate_sample(arparams, maparams, nobs)
    
    dates = sm.tsa.datetools.dates_from_range('1980m1', length=nobs)
    y = pandas.Series(y, index=dates)
    arma_mod = sm.tsa.ARMA(y, order=(2, 2))
    arma_res = arma_mod.fit(trend='nc', disp=-1)
    
    print(arma_res.summary())