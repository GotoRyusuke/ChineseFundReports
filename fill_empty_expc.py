'''
A programme to fill the empty records of expc part in aggregate panel

'''

import pandas as pd
import numpy as np
import math
from tqdm import tqdm

type_dict = {'基金季报': 'Q',
             '基金半年报': 'H',
             '基金年报': 'A'}

# combine full and expc panels to get agg panel

full_df_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/full_panel(dict ver4).xlsx'
full_df = pd.read_excel(full_df_path)
full_df.drop_duplicates(subset = ['code', 'year', 'quarter']).sort_values(by = ['code', 'year', 'quarter']).reset_index(drop = True, inplace = True)
full_df = full_df.loc[full_df['num_num_words'] != 0,:].reset_index(drop = True)

expc_df_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/expc_panel(dict ver4).xlsx'
expc_df = pd.read_excel(expc_df_path)
expc_df = expc_df.loc[expc_df['num_words'] != 0, :].reset_index(drop = True)

full_code_list = list(full_df['code'].drop_duplicates())
expc_code_list = list(expc_df['code'].drop_duplicates())
var_names = list(expc_df.columns)[5:]
expc_var_names = [name + '_expec' for name in var_names]

basic_info = ['code','name','type','date','year','quarter','info']  
full_df.columns = basic_info + var_names

agg_df = pd.DataFrame()

for code in tqdm(expc_code_list):
    code_full_df = full_df.loc[full_df['code'] == code,:]
    code_expc_df = expc_df.loc[expc_df['code'] == code,:]
    
    for record in list(code_expc_df['info']):
        if record in code_full_df['info'].values:
            code_full_df.loc[code_full_df['info'] == record,
                             expc_var_names] = code_expc_df.loc[code_expc_df['info'] == record, var_names].values
    
    agg_df = pd.concat([agg_df, code_full_df])
    
agg_df.reset_index(drop = True, inplace = True)
agg_df.drop_duplicates(subset = ['code','year','quarter'], inplace = True)

code_list = list(agg_df['code'].drop_duplicates())
agg_df.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金LM词频句频(综合-dict ver4).xlsx', index = False)

for code in tqdm(code_list):   
    code_df = agg_df.loc[agg_df['code'] == code, :]
    for idx in code_df.index:
        if np.isnan(code_df.loc[idx,'num_words_expec']):
            quarter = code_df.loc[idx, 'quarter']
            if quarter == 1:
                year = code_df.loc[idx, 'year'] - 1
                
                sup_report_idx = np.multiply(code_df['year'] == year,
                                         code_df['quarter'] == 6)
                
            elif quarter in [2,3]:
                year = code_df.loc[idx, 'year']
                sup_report_idx = np.multiply((code_df['year'] == year),
                                             (code_df['quarter'] == 5))
            elif quarter == 4:  
                year = code_df.loc[idx, 'year']  
                sup_report_idx = np.multiply(code_df['year'] == year,
                                         code_df['quarter'] == 6)
            else: continue
            
            if sup_report_idx.sum()==1:
                agg_df.loc[idx, expc_var_names] = code_df.loc[sup_report_idx,expc_var_names].values[0]

agg_df.dropna(subset = ['num_words_expec'], inplace = True)
agg_df.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（综合-已替代-dict ver4）.xlsx', index = False)
# agg_ante08.drop_duplicates(subset = ['code','year','quarter'], inplace = True)
           
expc_sub = agg_df.loc[:, basic_info + expc_var_names]
expc_sub.columns = basic_info + var_names
expc_sub = expc_sub.loc[expc_sub['type'] == '基金季报']
expc_sub.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（展望-季报-已替代-dict ver4）.xlsx', index = False)

full_sub = agg_df.loc[:,basic_info + var_names]
full_sub = full_sub.loc[full_sub['type'] == '基金季报',:]
full_sub.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（全文-季报-已替代-dict ver4）.xlsx', index = False)

