import pandas as pd
import networkx as nx # network
import pickle
import logging
from tqdm import tqdm
import numpy as np
import math
logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[Page Rank] %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

###


logging.info('============PAGE RANK CALCULATION================')


logging.info('loading graph...')
G = nx.read_gpickle("graph.pkl")

logging.info('loading personalized weight dictionary...')
with open('per_weight.pkl', 'rb') as handle:
    personalized_weights = pickle.load(handle)


logging.info('calculating page rank...')
pr = nx.pagerank(G,
                 alpha=0.5,
                 personalization=personalized_weights,
                 weight='age_weight')




# dict to panda for ranking
pr_col_name='Reputation'

pr_df=pd.DataFrame(pr.items(),columns=['Addr_ID',pr_col_name])
#pr_df=pd.DataFrame(pr.items(),columns=['Address',pr_col_name]) # for debuging

print(pr_df.head().to_markdown())


logging.info('loading address table...')

addr_df = pd.read_csv('address.csv')
addr_df['Addr_ID']=addr_df.index
print(addr_df.head().to_markdown())

'''

# recover address name
logging.info('recovering address...')

pr_df=pr_df.merge(addr_df,how='left',on='Addr_ID')



'''
# scale the result
pr_df[pr_col_name]=pr_df[pr_col_name]*1000000

###
logging.info('getting in and out degree...')

pr_df['In Degree']=[G.in_degree(node) for node in G]
pr_df['Out Degree']=[G.out_degree(node) for node in G]
pr_df['Degree']=pr_df['In Degree']+pr_df['Out Degree']
pr_df=pr_df.sort_values(pr_col_name,ascending=False)
pr_df=pr_df.reset_index()
pr_df['Rank']=pr_df.index+1

# Adding log

RS_list=pr_df['Reputation'].values.tolist()
fistzero_pos=0
for index,item in enumerate(RS_list):
    if item == 0:
        fistzero_pos=index
        break
min_val_nz=RS_list[fistzero_pos-1] # get min
min_val_nz=min_val_nz/1000000# recover back to normal
base_set=min_val_nz**(1/-1000)
RS_log_list=[np.clip(np.round(math.log((rs/1000000), base_set) + 1000, decimals=6), 0, 1000) if rs!=0 else 0 for rs in tqdm(RS_list)]
pr_df['Reputation (Log Scaled)']=RS_log_list

###
print(pr_df.head().to_markdown())

logging.info('writing pagerank result...')
pr_df.to_csv('pagerank.csv',index=None)
