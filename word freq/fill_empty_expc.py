# -*- coding: utf-8 -*-
'''
A module to 
1. combine full and expc panel dataframes;
2. fill the empty records of expc part in aggregate panel, and
3. export new full and expc panels

'''

import pandas as pd
import numpy as np
import math
from tqdm import tqdm

type_dict = {'基金季报': 'Q',
             '基金半年报': 'H',
             '基金年报': 'A'}

class get_agg_df:
    def __init__(self, full_df_path:str, expc_df_path:str):
        self.full_df = pd.read_excel(full_df_path)

        expc_df = pd.read_excel(expc_df_path)
        self.expc_df = expc_df.loc[expc_df['num_words'] != 0, :].reset_index(drop = True)

        self.var_names = list(expc_df.columns)[5:]
        self.expc_var_names = [name + '_expec' for name in self.var_names]

        self.basic_info = ['code','name','type','date','year','quarter','info']  
        self.full_df.columns = self.basic_info + self.var_names

        print('INITIALISED SUCCESSFULLY.')
    
    def comb_full_n_expc(self):
        # this method matches the indicators for the expc with that for the full text in the same report
        expc_var_names = self.expc_var_names
        var_names = self.var_names

        self.full_df.drop_duplicates(subset = ['code', 'year', 'quarter']).sort_values(by = ['code', 'year', 'quarter']).reset_index(drop = True, inplace = True)
        self.full_df = self.full_df.loc[self.full_df['num_num_words'] != 0,:].reset_index(drop = True)
        expc_code_list = list(self.expc_df['code'].drop_duplicates())
        
        agg_df = pd.DataFrame()
        for code in tqdm(expc_code_list):
            code_full_df = self.full_df.loc[self.full_df['code'] == code,:]
            code_expc_df = self.expc_df.loc[self.expc_df['code'] == code,:]
            
            for record in list(code_expc_df['info']):
                if record in code_full_df['info'].values:
                    code_full_df.loc[code_full_df['info'] == record,
                                    expc_var_names] = code_expc_df.loc[code_expc_df['info'] == record, var_names].values
            
            agg_df = pd.concat([agg_df, code_full_df])
            
        agg_df.drop_duplicates(subset = ['code','year','quarter']).reset_index(drop = True, inplace = True)
    
        return agg_df

    def fill_empty_expc(self, agg_df):
        # this method fills the empty indicators for the expc part with expc indicators from other reports
        code_list = list(agg_df['code'].drop_duplicates())
        for code in tqdm(code_list):   
            code_df = agg_df.loc[agg_df['code'] == code, :]
            for idx in code_df.index:

                # the following section of codes applies the substitution rules described in the log file
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
                        agg_df.loc[idx, self.expc_var_names] = code_df.loc[sup_report_idx, self.expc_var_names].values[0]

        agg_df.dropna(subset = ['num_words_expec'], inplace = True)
        return agg_df

    def get_expc_df(self, agg_sub_df):
        expc_sub = agg_sub_df.loc[:, self.basic_info + self.expc_var_names]
        expc_sub.columns = self.basic_info + self.var_names
        expc_sub = expc_sub.loc[expc_sub['type'] == '基金季报']

        return expc_sub
    
    def get_full_df(self, agg_sub_df):
        full_sub = agg_sub_df.loc[:,self.basic_info + self.var_names]
        full_sub = full_sub.loc[full_sub['type'] == '基金季报',:]

        return full_sub

if __name__ == '__main__':
    full_df_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/full_panel(dict ver4).xlsx'
    expc_df_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/expc_panel(dict ver4).xlsx'
    
    gad = get_agg_df(full_df_path = full_df_path,
                     expc_df_path = expc_df_path)

    # export aggregated dataframe
    agg_df = get_agg_df.comb_full_n_expc()
    agg_df.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金LM词频句频(综合-dict ver4).xlsx', index = False)

    # export aggregated dataframe, where empty expc indicators have been substituted with that from annual or mid-term reports
    agg_sub_df = get_agg_df.fill_empty_expc(agg_df)
    agg_sub_df.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（综合-已替代-dict ver4）.xlsx', index = False)
    
    # extract & export new expc panel
    expc_sub = get_agg_df.get_expc_df(agg_sub_df)
    expc_sub.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（展望-季报-已替代-dict ver4）.xlsx', index = False)

    # extract & export new full panel
    full_sub = get_agg_df.get_full_df(agg_sub_df)
    full_sub.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/outputs/基金报告LM词频统计（全文-季报-已替代-dict ver4）.xlsx', index = False)

