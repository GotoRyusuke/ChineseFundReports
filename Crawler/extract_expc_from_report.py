# -*- coding: utf-8 -*-
'''
A module to extract the expectation part from the report, and save as an excel file

'''
import pandas as pd
import os
import re
import numpy as np
from tqdm import tqdm
from Word_freq.cal_expc_word_freq import LM_freq_count_expc

comp_df_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/comp_panel_wf_simple.xlsx'
comp_df = pd.read_excel(comp_df_path)

ante08_q_idx = np.multiply(comp_df['type'] == '基金季报', comp_df['year']<2008)
ante08_q_df = comp_df.loc[ante08_q_idx, :]

ante08_ah_idx = np.multiply(comp_df['type'] != '基金季报', comp_df['year']<2008)
ante08_ah_df = comp_df.loc[ante08_ah_idx, :]


with open('F:/eastmoney/SupFiles/070001/嘉实成长2005年年度报告_2006-03-28.txt', encoding = 'utf-8') as f:
    text = f.read()

def expc_detector(text: str, report_type: str):
    # for annual or mid-term reports
    if report_type == 'AH':    
        find_start_expc = re.compile(r'未来展望|市场展望(与投资策略)?|对未来的展望|\d{4}年的?展望|投资管理展望|简要展望|行业走势(的|等)?(简要)?展望|基金管理展望 ')
        find_end_expc = re.compile(r'托管人报告|内部监察报告')
    
    # for quarterly report
    elif report_type == 'Q':
        find_start_expc = re.compile(r'投资策略和业绩表现(的说明及简要展望)?|后市展望及对策|.{1}季度展望|展望和操作计划|业绩表现和投资策略|\d{2}年第?.{1}季度投资策略|基金市场展望及对策|经理工作报告|\d{4}年(上|下)?半?年?展望|行业走势的简要展望|基金管理展望|市场展望和(基金)?投资策略|市场展望和操作思路|市场展望和下阶段投资策略|行业走势等的简要展望|市场及下一阶段操作展望|\d{4}年第?.{1}季度展望|基金管理展望')
        find_end_expc = re.compile(r'投资组合报告|开放式基金份额变动')

    start_idx = [start.end() for start in re.finditer(find_start_expc, text)]
    end_idx = [end.start() for end in re.finditer(find_end_expc, text)]
    
    if len(start_idx) > 0 and len(end_idx) > 0:
        temp_start_idx = 0
        start_expc = start_idx[temp_start_idx]
        # find_dots = re.compile(r'\.{6,}')
        temp_text = text[start_expc:start_expc+100]
        # while re.match(find_dots, temp_text):
        while '...' in temp_text or '……' in temp_text or '······'in temp_text: 
            temp_start_idx += 1
            start_expc = start_idx[temp_start_idx]
            temp_text = text[start_expc:start_expc+100]
        try:
            end_expc = [end for end in end_idx if end > start_expc][0]
        except: return 0,0
    else: return 0,0
    
    return start_expc, end_expc



parent_path = 'F:/eastmoney/SupFiles'
 
code_list = list(ante08_ah_df['code'].drop_duplicates())
for code in tqdm(code_list):
    file_list = ante08_ah_df.loc[ante08_ah_df['code'] == code,'filename']
    for idx in file_list.index:
        file = parent_path + '/' + code.split('.')[0] + '/' + file_list.loc[idx]
        with open(file,'r', encoding = 'utf-8') as f:
            text = f.read()
        
        start, end = expc_detector(text, 'AH')
        ante08_ah_df.loc[idx,'expc_text'] = text[start:end]
        

code_list = list(ante08_q_df['code'].drop_duplicates())
for code in tqdm(code_list):
    file_list = ante08_q_df.loc[ante08_q_df['code'] == code,'filename']
    for idx in file_list.index:
        file = parent_path + '/' + code.split('.')[0] + '/' + file_list.loc[idx]
        with open(file,'r', encoding = 'utf-8') as f:
            text = f.read()
        
        start, end = expc_detector(text, 'Q')
        ante08_q_df.loc[idx,'expc_text'] = text[start:end]
        
ante08_df = pd.concat([ante08_q_df,ante08_ah_df])
ante08_df.reset_index(drop = True, inplace = True)
ante08_df.sort_values(by = ['code','year','quarter'], inplace = True)
ante08_df = ante08_df.loc[ante08_df['expc_text'] != '',:]

ante08_df.drop_duplicates(subset = ['code','year','quarter'], inplace = True)

ante08_df.to_excel('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/expec_ante08.xlsx', index = False)

    
    

    
    
    