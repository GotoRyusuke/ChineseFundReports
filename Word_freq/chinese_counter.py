# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A modul to calculate LM word-freq.

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
import pandas as pd
import re
import re
from utils import load_stopwords, cut_sentence, cal_certain_tone, load_stopwords, load_jieba
from collections import Counter

class cn_counter:
    def __init__(self, lm_dict_path: str, stop_words_path: str):
        # initialise the LM dict
        lm_dict = pd.read_excel(lm_dict_path)
        dict_names = lm_dict.columns
        self.lm_dict = dict([(dict_name, 
                              lm_dict[dict_name].dropna().values) 
                             for dict_name in dict_names])
        # laod the stop words
        self.stop_words = load_stopwords(stop_words_path)
        
        # claim the dict words in case that they would be split by jieba
        load_jieba(lm_dict_path) 
    
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
        for dict_name, dict_wl in self.lm_dict.items():
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
        Read a string, and count the sent freqs in the following two ways
        1. for all 4 sub-dicts, regardless whether there is the prob of double polairty in the sent;
        2. cut the sentences into sub-sents(divided by ","";"".""?""!") and count, 
        regardless whether there is the prob of double polairty in the sent,

        and  then calculate the certain tone.

        Parameters
        ----------
        content: str
            A string of text
        
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
            temp_result = dict([(key,0) for key in self.lm_dict.keys()])

            for sent in content_new:
                counter = dict(Counter(sent))
                counter_keys = list(counter.keys())
                for dict_name, dict_wl in self.lm_dict.items():
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
    
if __name__ == '__main__':
    lm_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/LM_expanded_dictV2.xlsx'
    stop_words_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/my_stop_words.xlsx'
    
    cnc = cn_counter(lm_dict_path, stop_words_path)
    
    text = '哈哈哈！埃里克森的房间阿里山地方。什么是精确？什么是不精确？精确就是明确，明确就是精确；不精确就是不明确，不明确就是不精确。类似的词还有不严密、不可信、不完全，对立的则是严密、可信与完全。'
    test_wf = cnc.count_wf_by_dict(text)
    test_sf = cnc.count_sf_by_dict(text)

    