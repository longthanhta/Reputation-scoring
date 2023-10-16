import pandas as pd
from tqdm import tqdm
import os
import pickle
import logging
logging.basicConfig(
    filename='log.txt',
    filemode='a',
    format='[Address indexing] %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

data_path = 'database' # use your path

logging.info('============PRE ADDR================')




# reading data by chunk

fields= ['block_number','from_address','to_address','gas_used','gas_price']

addr_set=set()

frames = []

logging.info('reading txn first time, getting address list...')
for filename in tqdm(os.listdir(data_path)):
    #if filename=='bttc_test.csv': continue # for testing only
    csv_file = data_path + "/" + filename

    #logging.info(filename)
    df=pd.read_csv(csv_file,engine='python',usecols=['from_address','to_address'])
    
    # remote null address  in each loop, null address should not appear in the ranking
    df = df[(df['from_address']!='0x0000000000000000000000000000000000000000') & (df['to_address']!='0x0000000000000000000000000000000000000000')]# remove burn add
    df = df[(df['from_address']!='0x000000000000000000000000000000000000dead') & (df['to_address']!='0x000000000000000000000000000000000000dead')]# remove burn add

    # drop nan
    df=df.dropna(subset=['from_address'])
    df=df.dropna(subset=['to_address'])
    
    
    # save to address set
    addr_set.update(df['from_address'].values.tolist())
    addr_set.update(df['to_address'].values.tolist())
    # refresh memory
    del df



logging.info('writing address table...')
addr_df = pd.DataFrame({'Address':list(addr_set)})
addr_df['Addr_ID']=addr_df.index+1
addr_df.to_csv('address.csv',index=None)
