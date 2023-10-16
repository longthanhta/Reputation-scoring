import pandas as pd
from tqdm import tqdm
import numpy as np
#import matplotlib.pyplot as plt
import logging
logging.basicConfig(filename='log.txt',
                    filemode='a',
    format='[pre edge weight]  %(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info('============PRE GRAPH, DEGREE AND EDGE WEIGHT================')


def tanh_function(x,
             a=2,
             b=1/256):
    """
    V: convert transfer value to tanh value
    """
    return a / (1.0 + np.exp(-b * x))-1

def logistic(x,
             a=1,
             b=1 / 64):
    """Logistic function"""
    return a / (1.0 + np.exp(-b * x))

# logistic local weights, use this as a reference only
def logistic_local_timestamp_weights(block_max, block_min, blocks_per_day=28800, active_time=90):
    """
    T: Activeness function
    """
    timestamp_range = [i for i in range(int((block_max-block_min)/blocks_per_day)+1)]
    origin = timestamp_range[-1] - active_time
    weights_dict = dict()
    for timestamp in timestamp_range:
        if timestamp not in weights_dict.keys():
            edge_weight = logistic(timestamp-origin)
            weights_dict[timestamp] = edge_weight
    return weights_dict

if __name__ == '__main__':
    logging.info('reading txn_pkl...')
    data_df = pd.read_pickle('txn_graph.pkl')

    ###
    activate_time=90 # half range of the plot, [-90]----0----[90] 
    blockperday_set=7200# 3s per block for bsc

    ### 
    block_min=data_df['block_number'].min()
    block_max=data_df['block_number'].max()
    logging.info('block min:' + str(block_min) + '\nblock max:' + str(block_max))

    ###
    data_df['block_date']=((data_df['block_number']-block_min)/blockperday_set-activate_time).astype(int) 

    ###
    
    logging.info(' block date generating....')

    date_min=data_df['block_date'].min()
    date_max=data_df['block_date'].max()
    logging.info('block date min:' + str(date_min) + '\nblock date max:' + str(date_max))

    # ACTIVENESS WEIGHT
    logging.info('prober block date generating....')
    zero_pos=date_max-activate_time
    data_df['block_date']=data_df['block_date']-zero_pos

    date_min=data_df['block_date'].min()
    date_max=data_df['block_date'].max()
    logging.info('block date min:' + str(date_min) + '\nblock date max:' + str(date_max))

    logging.info('generating weight...')
    a=1
    b=1/64
    data_df['activeness_weights']=a / (1.0 + np.exp(-b * data_df['block_date']))

    # VALUE WEIGHT,


    # 1 Handle each txn
    
    # 1a get sum of volume and gas (both in usd)
    data_df['volume']=data_df['volume'].fillna(0.0)
    data_df['gas_spent_usd']=data_df['gas_spent_usd'].fillna(0.0)
    data_df['gas_volume_sum'] = data_df.apply(lambda row: row['volume']+row['gas_spent_usd'], axis=1)
    
    # 1b now get tank value from gas volume sum
    # conduct tanh function first time
    data_df['tanh_value'] = data_df.apply(lambda row: tanh_function(row['gas_volume_sum']), axis=1)
    
    # 1c now time that tank value to activeness weight
    data_df['value_weights'] = data_df.apply(lambda row: row['tanh_value']*row['activeness_weights'], axis=1)
    
    # 2a dump this one to check 
    logging.info('get value weight')
    logging.info(data_df)
    data_df.to_pickle('txn_graph_2.pkl')

    # 2b store other table to deal with getting the last activeness weight
    # later this table will be merged back with data_df
    data_aw_df=data_df[['from_ID','to_ID','activeness_weights']].drop_duplicates(['from_ID','to_ID'],keep='last')
    logging.info('attention weight df')
    logging.info(data_aw_df)

    data_aw_df.to_pickle('txn_graph_aw.pkl') # dump to test also
    
    # now use a group by and sum on data df
    data_df = data_df[['from_ID','to_ID','value_weights']].groupby(["from_ID", "to_ID"])['value_weights'].sum().reset_index()

    # conduct tanh function second time
    # data_df['tanh_value'] = data_df.apply(lambda row: tanh_function(row['value_weights']), axis=1)

    # merged back the activeness weight to data_df
    data_df = data_df.merge(data_aw_df,on=['from_ID','to_ID'],how='left')

    logging.info('merge back activeness weight')
    logging.info(data_df)

    # now get the final weight
    data_df['final_weights']=data_df['value_weights'] # make space for customization the weight function here

    logging.info('get the final weight')
    logging.info(data_df)

    #plot_line()
    logging.info('dumping txn file with age weight...')
    data_df.to_pickle('txn_graph_3.pkl')


    
