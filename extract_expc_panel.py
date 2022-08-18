'''
A programme to extract info from expec.xlsx, and match the fund code with 
fund info in FundCode.csv
'''

import pandas as pd
import os
import openpyxl
import re

def num_cn2arab(date:str):
    cn2eng_date_dict = {'一':1,
                        '二':2,
                        '三':3,
                        '四':4,
                        '五':5,
                        '六':6,
                        '七':7,
                        '八':8,
                        '九':9,
                        '零':0,
                        '〇':0}
    output = ''
    for num in date:
        if num in cn2eng_date_dict.keys():
            num = cn2eng_date_dict[num]
        
        output += str(num)
    return output

def find_info(column:str):
    date_df = pd.DataFrame(columns = ['report_type','year', 'quarter'])
    find_quarter = re.compile(r'第[一,二,三,四]季度')
    find_year = re.compile(r'\[年度\]\s[0-9]{4}')
    find_mid = re.compile(r'\[报告期\] 中报')
    find_annual = re.compile(r'\[报告期\] 年报')
    
    year = re.findall(find_year, column)
    quarter = re.findall(find_quarter, column)
    mid = re.findall(find_mid, column)
    annual = re.findall(find_annual, column)
    
    date_df.loc[0,'year'] = year[0][-4:]
    if len(quarter) > 0:
        flag = quarter[0][1]
        
        flag = num_cn2arab(flag)
        
        date_df.loc[0,'report_type'] = 'Q'
        date_df.loc[0,'quarter'] = flag
    
    elif len(mid) > 0:
        date_df.loc[0,'report_type'] = 'H'
        date_df.loc[0,'quarter'] = 5
    elif len(annual) > 0:
        date_df.loc[0,'report_type'] = 'A'
        date_df.loc[0,'quarter'] = 6
        
    return date_df
def code_info_dict_initialiser(code_info_dict_path: str):
    raw_dict = pd.read_csv(code_info_dict_path, encoding = 'gbk')
    raw_dict.columns = ['code','name', 'inv_typeI', 'inv_type_II', 'fund_type']
    code_info_dict = dict([(raw_dict.iloc[idx,0].split('.')[0], raw_dict.iloc[idx,:]) for idx in range(len(raw_dict))])
    
    return code_info_dict

code_info_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/FundCode.csv'
code_info_dict = code_info_dict_initialiser(code_info_dict_path)

expc_panel_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/expec.xlsx'

expc_panel = pd.read_excel(expc_panel_path).transpose()
original_index = expc_panel.index
expc_panel.index = ['code','name'] + list(range(194))

date_per_code = pd.DataFrame()
for date in original_index[2:]:
    date_per_code = pd.concat([date_per_code, find_info(date)])

date_per_code = date_per_code.reset_index(drop = True)
code_list = expc_panel.iloc[0,:13216]
new_code_list = []
for idx in range(len(code_list)):
    code = code_list[idx].split('.')[0]
    try:
        int(code)
    except:
        continue
    new_code_list.append(code)

fail_list = []
new_expc_panel = pd.DataFrame()
count = 0
per = 0
for code in new_code_list:
    if count > len(new_code_list)//100*per:
        print(f'{per}% done!')
        per += 1
    count += 1
    
    if code not in list(code_info_dict.keys()):
        print(f'{code} not in code2name dict!')
        fail_list.append(code)
        continue
        
    text_df = expc_panel.loc[0:,expc_panel.loc['code'] == code + '.OF']
    basic_info = pd.DataFrame(code_info_dict[code]).transpose()
    basic_info_df = pd.concat([basic_info]*len(text_df)).reset_index(drop = True).reset_index(drop = True)

    single_df = pd.concat([basic_info_df, date_per_code, text_df], axis = 1)
    single_df = single_df.dropna()

    single_df.columns = ['code', 'name','inv_typeI', 'inv_type_II', 'fund_type','report_type', 'year','quarter', 'text']
    single_df = single_df.sort_values(['year', 'quarter']).reset_index(drop = True)
    
    new_expc_panel = pd.concat([new_expc_panel, single_df]).reset_index(drop = True)

new_expc_panel.to_csv('C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/expc_panel_ver3.csv',
                      index = False,
                      encoding = 'gbk')
