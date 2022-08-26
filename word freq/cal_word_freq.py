# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A modul to calculate LM word-freq for the whole content in reports.

CONTENTS
--------
- <CLASS> LM_freq_count

OTHER INFO.
-----------
- Last upate: R4/8/19(Kim)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: yuhang1012yong@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237
'''

import os
import pandas as pd
from chinese_counter import cn_counter
from joblib import Parallel, delayed

class LM_freq_count:
    def __init__(self,
                 panel_df_path: str,
                 lm_dict_path: str,
                 all_file_path: str,
                 stop_words_path: str):
        
        self.panel_df = pd.read_excel(panel_df_path)
        self.path_all_file = all_file_path
        self.code_list = list(self.panel_df['code'].drop_duplicates(keep = 'first'))
        
        # initialise the Chinese counter
        self.cn_counter = cn_counter(lm_dict_path = lm_dict_path,
                                     stop_words_path = stop_words_path)

    
    def count_by_code(self, code: str):
        file_list = self.panel_df.loc[self.panel_df['code'] == code,
                                      'file_name']
        
        preamble_info = self.panel_df.loc[self.panel_df['code'] == code,
                                          ['code','name','type', 'date', 'year', 'quarter','info']]
        
        count_df = pd.DataFrame()
        
        for idx in preamble_info.index:
            file_path = self.path_all_file + '/' + code.split('.')[0] + '/' + file_list[idx]
            with open(file_path, 'r', encoding = 'utf-8') as f:
                content = f.read()
            
            wf_result = self.cn_counter.count_wf_by_dict(content)
            sf_result = self.cn_counter.count_sf_by_dict(content)
            
            # record wf
            for dict_name, wf in wf_result.items():
                dict_name = 'num_' + dict_name
                count_df.loc[idx, dict_name] = wf

            # record sf
            for dict_name, sf in sf_result.items():
                dict_name = 'num_' + dict_name
                count_df.loc[idx, dict_name] = sf
        
        output_df = pd.concat([preamble_info, count_df], axis = 1)
        return output_df
    
    def threading(self, jobs: int):
        code_list = self.code_list
        num_per_job = int(len(code_list) / jobs)
        code_list_cut = []
        for i in range(jobs):
            if i != jobs - 1:
                code_list_cut.append(code_list[i * num_per_job: (i + 1) * num_per_job])
            else:
                code_list_cut.append(code_list[i * num_per_job:])
                
        def multi_run(sub_code_list):
            sub_df = pd.DataFrame()
            for code in sub_code_list:
                result = self.count_by_code(code = code)
                sub_df = pd.concat([sub_df, result])
                
            return sub_df
                
        all_dfs = Parallel(n_jobs=jobs, verbose=1)(delayed(multi_run)(sub_list) for sub_list in code_list_cut)
        output = pd.DataFrame()
        for df in all_dfs:
            output = pd.concat([output, df])
        output.reset_index(drop = True, inplace = True)
        return output

if __name__ == '__main__':
    
    # path_panel_df = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/agg_full.xlsx'
    # path_all_file = 'F:/eastmoney/SupFiles'
    # path_lm_dict = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/LM_expanded_dictV3.xlsx'
    # path_stopwords = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec/my_stop_words.xlsx'
    
    panel_df_path = './agg_full.xlsx'
    path_all_file = './FullTexts'
    lm_dict_path = './LM_expanded_dictV3.xlsx'
    stopwords_path = './my_stop_words.xlsx'
      
    freq = LM_freq_count(panel_df_path, path_all_file, lm_dict_path, stopwords_path)
    
    results = freq.threading(32)
    results.to_excel('./full_panel(dict ver3).xlsx', index = False)