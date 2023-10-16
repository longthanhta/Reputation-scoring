import pandas as pd
from tqdm import tqdm
import os
import pickle
import logging
import numpy as np
import math
import sys

logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[Finalizing] %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info('============FINALIZING================')


def remove_index(df):
    return df.loc[:, ~df.columns.str.contains('index')]   

output_filename='reputation_table'

if len(sys.argv) > 1:
    user_input = sys.argv[1]
    output_filename=user_input

logging.info('read pagerank and info table...')
pr_df=pd.read_csv('pagerank.csv')
logging.info('pagerank:')
print(pr_df.head().to_markdown())

data_df=pd.read_pickle('addr_w_info.pkl')
#data_df=data_df.drop('Address',axis=1) # the pagerank.csv already have address column, the address column in addr_w_info.pkl used for quick check
logging.info('infor table:')
print(data_df.head().to_markdown())

# mergeing the total receive, total transfer and total gas spent (have NOT calculated specifically for contract)
logging.info('merging the result...')
pr_df=remove_index(pr_df.merge(data_df,on='Addr_ID',how='left'))
print(pr_df.head().to_markdown())
del data_df # save memory

pr_df=pr_df.drop(columns=['Addr_ID'], axis=1)

logging.info('re-arranging columns...')
print(pr_df.columns.tolist())
col_list=['Rank','Address', 'Reputation','In Degree', 'Out Degree', 'Degree', 'Total Transfer', 'Total Receive','Total Txn','Total Gas Spent','Total Volume','Reputation (Log Scaled)']

pr_df=pr_df[col_list]


logging.info('all gas spent calculation..')

ags=pr_df['Total Gas Spent'].sum()

with open(output_filename+'_ags.txt', 'w') as f:
        f.write(str(ags))

print(pr_df.columns.tolist())
print(pr_df.head().to_markdown())

logging.info('writing to csv..')
pr_df.to_csv(output_filename+'.csv',index=None)
