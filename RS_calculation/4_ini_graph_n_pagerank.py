import pandas as pd
import networkx as nx # network
import pickle
import logging
from tqdm import tqdm
import numpy as np
import math
logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[Init graph] %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info('=====INI GRAPH AND NODE WEIGHT=====')


if __name__ == '__main__':


    logging.info('reading txn_w_weight.pkl...')
    data_df = pd.read_pickle('txn_graph_3.pkl')

    logging.info('initiating graph...')
    G=nx.from_pandas_edgelist(data_df, 'from_ID', 'to_ID', create_using=nx.DiGraph, edge_attr='value_weights')
    logging.info('total ' + str(G.number_of_nodes())+' nodes and ' + str(G.number_of_edges()) + ' edges')

    to_weights_df = data_df[['to_ID', 'value_weights','activeness_weights']] # calculating personalized weight
    del data_df

    logging.info('personalized weight generating...')


    node_weights = to_weights_df.groupby('to_ID').sum()
    s_weights = float(node_weights["value_weights"].sum())
    #s_weights = float(node_weights["activeness_weights"].sum())
    personalized_weights = {k: v/s_weights for k, v in node_weights["value_weights"].items()}
    #personalized_weights = {k: v/s_weights for k, v in node_weights["activeness_weights"].items()}
   
    logging.info('============PAGE RANK CALCULATION================')

    logging.info('calculating page rank...')
    pr = nx.pagerank(G,
                     alpha=0.5,
                     personalization=personalized_weights,
                     weight='value_weights')




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
    address_df=pd.read_csv('address.csv')
    pr_df=pr_df.merge(address_df,on='Addr_ID',how='left')


    ###
    print(pr_df.head().to_markdown())

    logging.info('writing pagerank result...')
    pr_df.to_csv('pagerank.csv',index=None)
    logging.info('finished calculating pagerank')
    logging.info('===========================================')
    logging.info('FINISHED')
    logging.info('===========================================')
