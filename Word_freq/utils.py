# -*- coding: utf-8 -*-
'''
Some useful functions.

'''
import pandas as pd
import re
import numpy as np
import jieba

def get_type(t):
    type_dict = {'A': '年报',
                 'Q': '季报',
                 'H': '半年报',
                 'M': '月报'}
    if t not in type_dict.keys():
        return ''
    return '基金' + type_dict[t]

def num_cn2eng(date:str):
    cn2eng_date_dict = {'一':1,
                        '二':2,
                        '三':3,
                        '四':4,
                        '五':5,
                        '六':6,
                        '七':7,
                        '八':8,
                        '九':9,
                        '零':0}
    output = ''
    for num in date:
        if num in cn2eng_date_dict.keys():
            num = cn2eng_date_dict[num]
        
        output += str(num)
    return output

def convert_num2code(code_list:list):
    output = []
    for code in code_list:
        code = str(code)
        code = '0'*(6-len(code)) + code 
        output.append(code)
        
    return output

def find_info(column:str):
    year_detector = re.compile(r'[0-9,一,二,三,四,五,六,七,八,九,零]{4}年')
    quarter_detector = re.compile(r'[1-4,一,二,三,四]季')
    annual_detector = re.compile(r'年[度,报]')
    mid_detector = re.compile(r'(半年|中期)')
    
    year = re.findall(year_detector, column)
    quarter = re.findall(quarter_detector, column)
    annual = re.findall(annual_detector, column)
    mid = re.findall(mid_detector,column)
    
    if len(year) > 0:
        year = num_cn2eng(year[0][:-1])
        
    if len(quarter) > 0:
        quarter = num_cn2eng(quarter[0][0])
        report_type = 'Q'
    elif len(mid) > 0:
        quarter = 5
        report_type  = 'H'     
    elif len(annual) > 0:
        report_type = 'A'
        quarter = 6
    
    return '-'.join([report_type, year, quarter])
def code_info_dict_initialiser(code_info_dict_path: str):
    raw_dict = pd.read_csv(code_info_dict_path, encoding = 'gbk')
    raw_dict.columns = ['code','name', 'inv_typeI', 'inv_type_II', 'fund_type']
    code_info_dict = dict([(raw_dict.iloc[idx,0].split('.')[0], raw_dict.iloc[idx,:]) for idx in range(len(raw_dict))])
    
    return code_info_dict

def compare_list(sample: list, tofind: list):
    output = []
    for code in tofind:
        if code not in sample:
            output.append(code)
    
    return output

def load_stopwords(stopwords_path: str):
    stop_words = [str(symbol) for symbol in pd.read_excel(stopwords_path)['symbol']]
    return stop_words

def cut_sentence(content: str, mode: str):
    if mode == 'original':
        eos = ['。','！','？']
    elif mode == 'sub-sent':
        eos = ['。','！','？','，','；']
    words = [word for word in jieba.cut(content)]
    
    word_count = 0
    eos_idx = 0
    
    sentences = []
    for word in words:
        if word in eos:
            sentence = words[eos_idx:word_count]
            sentences.append(sentence)
            eos_idx = word_count
        word_count += 1
    return sentences

def load_jieba(jieba_add_word_path):
    word_df = pd.read_excel(jieba_add_word_path)
    jieba_add_word = []
    for col in word_df.columns:
        jieba_add_word += list(word_df[col].dropna())

    for word in jieba_add_word:
        jieba.add_word(word)

def cal_certain_tone(count_result: dict, cal_type: str):
    sm = count_result['strong_modal']
    wm = count_result['weak_modal']
    un = count_result['uncertainty']
    ce = count_result['certainty']
    
    if cal_type == 'ct_uncertainty':
        nom = sm - wm - un
        denom = sm + wm + un
    elif cal_type == 'ct_certainty':
        nom = sm - wm + ce
        denom = sm + wm + ce
        
    elif cal_type == 'ct_all':
        nom = sm + ce - wm - un
        denom = sm + ce + wm + un
    
    elif cal_type == 'ct_modals':
        nom = sm - wm
        denom = sm + wm
        
    elif cal_type == 'ct_certains':
        nom = ce - un
        denom = ce + un
    
    if denom == 0: return 0        
    
    return nom/denom

def two_digits(num:str):
    return '0'*(2-len(num)) + num
        
    
