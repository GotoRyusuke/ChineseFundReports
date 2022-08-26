# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A modul to calculate LM word-freq for expectation part in reports.

CONTENTS
--------
- <CLASS> LM_freq_count_expc

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

class LM_freq_count_expc:
    def __init__(self,
                 expc_panel_path: str,
                 lm_dict_path: str,
                 stop_words_path: str):
        # initialise the panel df
        self.panel_df = pd.read_excel(expc_panel_path)
        # initialise the Chinese counter
        self.cn_counter = cn_counter(lm_dict_path = lm_dict_path,
                                     stop_words_path = stop_words_path)

    def count_by_code(self, code: str):
        '''
        Calculate the word freq and sent freq for all contents under the same
        fund(identified by the fund code).

        Parameters
        ----------
        content: str
            A fund code.
        
        Returns
        -------
        output_df: a 
            A dict of premable info and count resutls.

        '''
        type_dict = {'基金季报': 'Q',
                     '基金半年报': 'H',
                     '基金年报': 'A'}
        

        content_list = self.panel_df.loc[self.panel_df['code'] == code ,
                                      'expc_text']
        
        preamble_info = self.panel_df.loc[self.panel_df['code'] == code,
                                          ['code','type', 'year', 'quarter']]
        
        preamble_info['info'] = ['-'.join([type_dict[preamble_info.loc[idx,'type']],
                                    str(preamble_info.loc[idx,'year']),
                                    str(preamble_info.loc[idx,'quarter'])])
                               for idx in preamble_info.index]
        
        count_df = pd.DataFrame()
        
        # record wf and sf results
        for idx in preamble_info.index:
            content = content_list[idx]
            wf_result = self.cn_counter.count_wf_by_dict(content)
            sf_result = self.cn_counter.count_sf_by_dict(content)
            
            # record wf
            for dict_name, wf in wf_result.items():
                if dict_name in self.cn_counter.lm_dict.keys():
                    dict_name = 'num_' + dict_name
                count_df.loc[idx, dict_name] = wf

            # record sf
            for dict_name, sf in sf_result.items():
                if dict_name in self.cn_counter.lm_dict.keys():
                    dict_name = 'num_' + dict_name
                count_df.loc[idx, dict_name] = sf
        
        output_df = pd.concat([preamble_info, count_df], axis = 1)
        return output_df
    
    def threading(self, jobs: int):
        self.code_list = list(self.panel_df['code'].drop_duplicates(keep = 'first'))
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
            
        all_sub_dfs = Parallel(n_jobs=jobs, verbose=5)(delayed(multi_run)(sub_list) for sub_list in code_list_cut)
        output = pd.DataFrame()
        for df in all_sub_dfs:
            output = pd.concat([output, df])
        output.reset_index(drop = True, inplace = True)

        return output
    
if __name__ == '__main__':
    # expc_panel_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/agg_expc.xlsx' 
    # lm_dict_path = 'C:/Users/niccolo/Desktop/词句频统计（3个版本词典）/去除多义词词典/LM_expanded_dictV3.xlsx'
    # stop_words_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/my_stop_words.xlsx'

    expc_panel_path = './agg_expc.xlsx'
    lm_dict_path = './LM_expanded_dictV3.xlsx'
    stop_words_path = './my_stop_words.xlsx'
        
    freq_expc = LM_freq_count_expc(expc_panel_path = expc_panel_path,
                                 lm_dict_path = lm_dict_path,
                                 stop_words_path = stop_words_path)
    
    results = freq_expc.threading(32)
    results.to_excel('./expc_panel(dict ver3).xlsx', index = False)
    

    