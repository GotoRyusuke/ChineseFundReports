'''
DESCRIPTION
-----------
A modul to calculate LM word-freq for the whole content in reports.

CONTENTS
--------
- <CLASS> LM_freq_count

OTHER INFO.
-----------
- Last upate: R4/8/15(Getsu)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: 1155169839@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237
'''

import os
import pandas as pd
import jieba
import time
import re
from utils import load_stopwords, load_jieba, cut_sentence, cal_certain_tone, load_stopwords
from collections import Counter
from joblib import Parallel, delayed

class LM_freq_count:
    def __init__(self,
                 path_panel_df: str,
                 path_lm_dict: str,
                 path_all_file: str,
                 path_stopwords: str):
        
        self.panel_df = pd.read_excel(path_panel_df)
        self.path_all_file = path_all_file
        self.code_list = list(self.panel_df['code'].drop_duplicates(keep = 'first'))
        self.stop_words = load_stopwords(path_stopwords)
        
        dict_lm = pd.read_excel(path_lm_dict)
        dict_names = dict_lm.columns
        self.dict_lm = dict([(dict_name, 
                              dict_lm[dict_name].dropna().values) 
                             for dict_name in dict_names])
        load_jieba(path_lm_dict)
    
    def count_wf_by_dict(self, content: str):
        spaces = re.compile(r'\s{2,}')
        content = re.sub(spaces, ' ', content)
        sents_old = cut_sentence(content, mode = 'original')
        
        content_new = []
        for sent in sents_old:
            temp_sent = ' '.join(sent)
            for symbol in self.stop_words:
                temp_sent = temp_sent.replace(symbol, '')
            new_sent = [word for word in temp_sent.strip().split(' ') if word != '']
            content_new += new_sent

        count_result = {}
        count_result['num_words'] = len(content_new)
        counter = dict(Counter(content_new))
        counter_keys = list(counter.keys())
        for dict_name, dict_wl in self.dict_lm.items():
            dict_count = 0
            for kw in dict_wl:
                if kw in counter_keys:
                    dict_count += counter[kw]
            
            count_result[dict_name] = dict_count
        
        count_result['ct_uncertainty'] = cal_certain_tone(count_result, 'ct_uncertainty')
        count_result['ct_certainty'] = cal_certain_tone(count_result, 'ct_certainty')
        count_result['ct_all'] = cal_certain_tone(count_result, 'ct_all')
        count_result['ct_modals'] = cal_certain_tone(count_result, 'ct_modals')
        count_result['ct_certains'] = cal_certain_tone(count_result, 'ct_certains')
            
        return count_result

    def count_sf_by_dict(self, content: str):
        spaces = re.compile(r'\s{2,}')
        content = re.sub(spaces, ' ', content)
        
        output = {}
        for mode in ['original', 'sub-sent']:
            sents_old = cut_sentence(content, mode = mode)
            
            content_new = []
            for sent in sents_old:
                temp_sent = ' '.join(sent)
                for symbol in self.stop_words:
                    temp_sent = temp_sent.replace(symbol, '')
                new_sent = [word for word in temp_sent.strip().split(' ') if word != '']
                content_new.append(new_sent)
            # start counting
            temp_result = dict([(key,0) for key in self.dict_lm.keys()])
           
            for sent in content_new:
                counter = dict(Counter(sent))
                counter_keys = list(counter.keys())
                for dict_name, dict_wl in self.dict_lm.items():
                    dict_count = 0
                    for kw in dict_wl:
                        if kw in counter_keys:
                            dict_count += 1
                            break
                    
                    count_result[dict_name] += dict_count
                    
            count_result = {}
            for key in temp_result.keys():
                count_result[mode + '_' + key] = temp_result[key]  
                
            count_result[mode + '_sen_uncertainty'] = cal_certain_tone(count_result, 'ct_uncertainty')
            count_result[mode + '_sen_certainty'] = cal_certain_tone(count_result, 'ct_certainty')
            count_result[mode + '_sen_all'] = cal_certain_tone(count_result, 'ct_all')
            count_result[mode + '_sen_modals'] = cal_certain_tone(count_result, 'ct_modals')
            count_result[mode + '_sen_certains'] = cal_certain_tone(count_result, 'ct_certains')
            
            output = output | count_result
        return output
    
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
            
            wf_result = self.count_wf_by_dict(content)
            sf_result = self.count_sf_by_dict(content)
            
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
    
    path_panel_df = './agg_full.xlsx'
    path_all_file = './FullTexts'
    path_lm_dict = './LM_expanded_dictV3.xlsx'
    path_stopwords = './my_stop_words.xlsx'
      
    freq = LM_freq_count(path_panel_df = path_panel_df,
                        path_all_file = path_all_file,
                        path_lm_dict = path_lm_dict,
                        path_stopwords = path_stopwords)
    
    results = freq.threading(32)
    results.to_excel('./full_panel(dict ver3).xlsx', index = False)
    
    # new_panel.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/comp_panel_wf_details.xlsx', index = False)
    
    # text = '哈哈哈！埃里克森的房间阿里山地方。什么是精确？什么是不精确？精确就是明确，明确就是精确；不精确就是不明确，不明确就是不精确。类似的词还有不严密、不可信、不完全，对立的则是严密、可信与完全。'
    
    # test_wf, test_sf = freq.count_single_file(text)