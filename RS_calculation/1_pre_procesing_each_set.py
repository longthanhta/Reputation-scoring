import pandas as pd
from tqdm import tqdm
import os
import pickle
import logging
import sys
# CONFIGURATION
# ================================================
data_path='database/'
#==================================================
logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[pre each set]  %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


try:
    os.mkdir('pre_set_addr')
except:
    pass

try:
    os.mkdir('pre_set_txn')
except:
    pass


specific_set=False

if len(sys.argv) > 1:
    user_input = sys.argv[1]
    specific_set=user_input



logging.info('============PRE TXN================')


# Rading address file, clone address column for later merge
logging.info('reading address file...')
addr_df=pd.read_csv('address.csv')
addr_df['Addr_ID']=addr_df['Addr_ID'].astype('int')
print(addr_df.head().to_markdown())
addr_df['from_ID']=addr_df['Addr_ID'].astype('int')
addr_df['to_ID']=addr_df['from_ID'].astype('int')
addr_df['from_address']=addr_df['Address']
addr_df['to_address']=addr_df['Address']


logging.info(addr_df.head().to_markdown())
logging.info('reading raw txn data...')
# reading data by chunk

dict_list=os.listdir(data_path)
dict_list.sort()

logging.info('reading txn second time, indexing the address...')
for filename in tqdm(dict_list):
        # df: txn set
        # df_out: addr set
        # PROCESS TXN
        csv_file = data_path + "/" + filename
        logging.info('PROCESSING '+filename+'__________________________________________________')

        df=pd.read_csv(csv_file,engine='python',usecols=['block_number','from_address','to_address','gas_spent','gas_spent_usd','volume','token_contract'],na_values=r'\N')
        # when running sql on clichouse to output file to s3, nan value being returned as \N

        logging.info('cleaning...')
        df = df[(df['from_address']!='0x000000000000000000000000000000000000dead') & (df['to_address']!='0x000000000000000000000000000000000000dead')]# remove burn add
        df = df[(df['from_address']!='0x0000000000000000000000000000000000000000') & (df['to_address']!='0x0000000000000000000000000000000000000000')]# remove burn add

        df=df[df['from_address']!=df['to_address']]

        #1 indexing address in each loop
        logging.info('indexing...')
        df=df.merge(addr_df[['from_address','from_ID']],on='from_address',how='left')
        df=df.merge(addr_df[['to_address','to_ID']],on='to_address',how='left')
        df=df.drop(columns=['from_address','to_address']) # save memory
        #print(addr_df)

        #2 dump txn file right after indexing
        logging.info('remove nan...')
        df=df.dropna(subset=['from_ID'])
        df=df.dropna(subset=['to_ID'])
        logging.info('checking nan:')
        print(df[df.isna().any(axis=1)].head().to_markdown())
        logging.info('dumping sorted txn...')
        df[['block_number','from_ID','to_ID','volume','gas_spent_usd']].sort_values('block_number').to_pickle('pre_set_txn/'+filename+'.pkl')


        logging.info('the set before calculation')
        print(df.head().to_markdown())
        
        
        # ADDING INFORMATIN FOR EACH ADDRESS


        logging.info('calculating total transfer...')
        total_transfer_df = df.groupby('from_ID',as_index=False).size()
        total_transfer_df.columns=['Addr_ID','Total Transfer']
        #print(total_transfer_df)

        logging.info('calculating total receives...')
        total_receive_df = df.groupby('to_ID',as_index=False).size()
        total_receive_df.columns=['Addr_ID','Total Receive']
        #print(total_receive_df)

        #3 now volume

        #try:
        logging.info('calculating total volume spent out ...') # 
        total_vol_spent_df_out = df.groupby('from_ID',as_index=False)['volume'].sum()
        total_vol_spent_df_out.columns=['Addr_ID','Total Volume Out']


        logging.info('calculating total volume spent in ...') # 
        total_vol_spent_df_in = df.groupby('to_ID',as_index=False)['volume'].sum()
        total_vol_spent_df_in.columns=['Addr_ID','Total Volume In']
       # except:
       #     logging.info('volume calculating error at file',filename)


        #4 for gas, remove txn with token_addres it not nan
        df=df[df['token_contract'].isnull()] 
        
        
        # now start calculate gas
        logging.info('calculating total gas spent out ...') # calculating normal gas for all txn
        total_gas_spent_df_out = df.groupby('from_ID',as_index=False)['gas_spent'].sum()
        total_gas_spent_df_out.columns=['Addr_ID','Total Gas Spent']

        logging.info('merging...')
        df_out=addr_df[['Addr_ID','Address']]
        
        
        # merge to main table
        df_out = df_out.merge(total_transfer_df, on='Addr_ID', how='left')
        del total_transfer_df
        
        df_out = df_out.merge(total_gas_spent_df_out, on='Addr_ID', how='left')
        del total_gas_spent_df_out 
        
        df_out = df_out.merge(total_vol_spent_df_in, on='Addr_ID', how='left')
        del total_vol_spent_df_in 
        
        df_out = df_out.merge(total_vol_spent_df_out, on='Addr_ID', how='left')
        del total_vol_spent_df_out 
        
        df_out = df_out.merge(total_receive_df, on='Addr_ID', how='left')
        del total_receive_df 
        
        df_out['Contract']=[False for i in range(len(df_out))]
        df_out=df_out.fillna(0) # fill the rest with 0
        df_out['Total Txn']=df_out['Total Transfer']+df_out['Total Receive']
        df_out['Total Volume']=df_out['Total Volume In']+df_out['Total Volume Out']
        df_out=df_out.drop(columns=['Total Volume In','Total Volume Out'])


        # output the calculation
        filename=filename.split('.')[0]
        logging.info('dumping the file')
        df_out.to_pickle('pre_set_addr/'+filename+'.pkl')
        print(df_out.head().to_markdown())
        # release memory
        #break #debug
        del df
        del df_out
        
        # this part for checking
        if not specific_set:
            if filename==specific_set:
                break

