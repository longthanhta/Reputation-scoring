# NOTE

# The "remove duplicate" have been disable

# ===========================================



import pandas as pd
from tqdm import tqdm
import os
import pickle
import logging
import sys

# CONFIGURATION
# ================================================
txn_path = 'pre_set_txn'
addr_path='pre_set_addr'
num_col=['Total Transfer',
         'Total Receive',
         'Total Txn',
         'Total Gas Spent',
         'Total Volume']
#==================================================
logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[Pre one set]  %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

first_part_reading=True 



dict_list=os.listdir(txn_path)
dict_list.sort()
dict_list_addr=os.listdir(addr_path)
dict_list_addr.sort()

print('all file list',dict_list)


start_point=0
stop_point=999


if len(sys.argv) > 2:
    start_point = int(sys.argv[1])-1
    stop_point = int(sys.argv[2])-1

dict_list=dict_list[start_point:stop_point]
dict_list_addr=dict_list_addr[start_point:stop_point]

print('select cut list',dict_list)

logging.info("--------------------------------------")
logging.info("-------stop point"+str(stop_point)+"-----")
logging.info("--------------------------------------")



# PROCESSING TXN
for index,filename in enumerate(tqdm(dict_list)):
    #logging.info('appending txn' + filename)

    set_path = txn_path + "/" + filename
    if first_part_reading:
        logging.info('reading first file'+filename+'...')
        df=pd.read_pickle(set_path)
        print(df)
        first_part_reading=False
    else:
        logging.info('reading next file...'+filename+'...')
        df_next=pd.read_pickle(set_path)
        logging.info('combine with current file...')
        df=pd.concat([df,df_next])
        logging.info('removing duplicates, keep last')
#        df=df.drop_duplicates(subset=['from_ID','to_ID'],keep='last') # Disable this to keep information for calculationg weighted sum     
    


df = df[df['from_ID'].notna()]
df = df[df['to_ID'].notna()]


print(df.head().to_markdown())
df.to_pickle('txn_graph.pkl')
logging.info('==================================================')
logging.info('YOU NOW CAN INITIATE GRAPH AND CALCULATE PAGE RANK')
logging.info('==================================================')



first_part_reading=True
# PROCESSING ADDRESS

for index,filename in enumerate(tqdm(dict_list_addr)):
    #logging.info('processing address '+filename)
    set_path = addr_path + "/" + filename
    
    if first_part_reading:
        logging.info('reading first file'+filename+'...')
        df=pd.read_pickle(set_path)
        first_part_reading=False
        addr_col=df['Address']
        addr_id=df['Addr_ID']
        df=df[num_col]
        df=df.fillna(0)

    else:
        logging.info('reading next file...'+filename+'...')
        df_next=pd.read_pickle(set_path)
        df_next=df_next[num_col]
        df = df.add(df_next)




df['Addr_ID']=addr_id
df['Address']=addr_col
print(df.head().to_markdown())
df.to_pickle('addr_w_info.pkl')

