'''
DESCRIPTION
-----------
A modul to calculate LM word-freq for expectation part in reports.

CONTENTS
--------
- <FUNC> pre_process
- <CLASS> LM_freq_count_expc

OTHER INFO.
-----------
- Last upate: R4/8/1(Getsu)
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
from utils import load_stopwords, load_jieba, cut_sentence, cal_certain_tone,load_stopwords
from collections import Counter
from joblib import Parallel, delayed
from tqdm import tqdm


class LM_freq_count_expc:
    def __init__(self,
                 expc_panel_path: str,
                 lm_dict_path: str,
                 stop_words_path: str):

        # initialise the panel df
        self.panel_df = pd.read_excel(expc_panel_path)

        # laod the stop words
        self.stop_words = load_stopwords(stop_words_path)
        # claim the dict words in case that they would be split by jieba
        load_jieba(lm_dict_path)  
        
        # initialise the LM dict
        dict_lm = pd.read_excel(lm_dict_path)
        dict_names = dict_lm.columns
        self.dict_lm = dict([(dict_name, 
                              dict_lm[dict_name].dropna().values) 
                             for dict_name in dict_names])

    def count_wf_by_dict(self, content: str): 
        '''
        Read a string, count the word freqs for all 4 sub-dicts, and finally
        calculate the 5 indicators.

        Parameters
        ----------
        content: str
            a string, which contains the content from an expectation part.
        
        Returns
        -------
        count_result: dict
            A dict of count resutls

        '''     
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
        
        # start counting
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
        '''
        Read a string, and count the sent freqs for all 4 sub-dicts, regardless
        whether there is the prob of double polairty in the sent.

        Parameters
        ----------
        content: str
            a string, which contains the content from an expectation part.
        
        Returns
        -------
        count_result: dict
            A dict of count results
        
        '''
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
                    
                    temp_result[dict_name] += dict_count
            
            count_result = {}
            for key in temp_result.keys():
                count_result[mode + '_' + key] = temp_result[key]
            count_result[mode + '_num_sents'] = len(content_new)       
            count_result[mode + '_sen_uncertainty'] = cal_certain_tone(temp_result, 'ct_uncertainty')
            count_result[mode + '_sen_certainty'] = cal_certain_tone(temp_result, 'ct_certainty')
            count_result[mode + '_sen_all'] = cal_certain_tone(temp_result, 'ct_all')
            count_result[mode + '_sen_modals'] = cal_certain_tone(temp_result, 'ct_modals')
            count_result[mode + '_sen_certains'] = cal_certain_tone(temp_result, 'ct_certains')
            
            output = output | count_result
        return output
    
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
            wf_result = self.count_wf_by_dict(content)
            sf_result = self.count_sf_by_dict(content)
            
            # record wf
            for dict_name, wf in wf_result.items():
                if dict_name in self.dict_lm.keys():
                    dict_name = 'num_' + dict_name
                count_df.loc[idx, dict_name] = wf

            # record sf
            for dict_name, sf in sf_result.items():
                if dict_name in self.dict_lm.keys():
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
    # lm_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/LM_expanded_dictV2.xlsx'
    # stop_words_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec/my_stop_words.xlsx'

    expc_panel_path = './agg_expc.xlsx'
    lm_dict_path = './LM_expanded_dictV3.xlsx'
    stop_words_path = './my_stop_words.xlsx'
    
  
        
    freq_expc = LM_freq_count_expc(expc_panel_path = expc_panel_path,
                                 lm_dict_path = lm_dict_path,
                                 stop_words_path = stop_words_path)
    results = freq_expc.threading(32)
    results.to_excel('./expc_panel(dict ver3).xlsx', index = False)
    

    