import pandas as pd
import numpy as np
import pymannkendall as mk
# from cmlib.backtesting.mann_kendall import MannKendall


def fn_mann_kendal(df,str_var,cohort):
    df = df[['measurementPeriodId',str_var,'eadSegment']]
    # calc per year
    df['month'] = df.loc[:, 'measurementPeriodId'].apply(lambda x: str(x)[4:-2])
    df['year'] = df.loc[:, 'measurementPeriodId'].apply(lambda x: str(x)[:-4])
    df_cohort = df[df['month']==str(cohort)]

    #preparing data for test
    df_cohort = df_cohort.drop(['measurementPeriodId', 'month'], axis=1)

    # grouped by year and mean
    df_by_year = df_cohort.groupby(['year']).mean()

    res = mk.original_test(df_by_year[str_var])
    p_value = float("{:.2f}".format(res.p))
    non_norm = float("{:.2f}".format(res.s))
    norm = float("{:.2f}".format(res.z))

    # traffic light
    if p_value == 0.00:
        status = 'Red'
    elif 0.01 <= p_value < 0.05:
        status = 'Amber'
    else:
        status = 'Green'

    dic = {'Product':df.eadSegment.unique(),
           'Variable': str_var,
           'Non-normalized':non_norm,
           'Normalized':norm,
           'p-value':p_value,
           'Traffic Light':status
           }
    df_result = pd.DataFrame(dic,index=[0])

    return df_result