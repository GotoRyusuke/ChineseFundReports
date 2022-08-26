# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A module to extract the expectation part from the report, and save as an excel file

CONTENTS
--------
- <CLASS> expc_extractor

OTHER INFO.
-----------
- Last upate: R4/8/18(Moku)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: yuhang1012yong@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237
'''
import pandas as pd
import os
import re
import numpy as np
from tqdm import tqdm

class expc_extractor:
    def __init__(self, store_path: str, panel_path: str):
        self.store_path = store_path
        self.panel_path = panel_path

    @staticmethod
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
            temp_text = text[start_expc:start_expc+100]
            while '...' in temp_text or '……' in temp_text or '······'in temp_text: 
                temp_start_idx += 1
                start_expc = start_idx[temp_start_idx]
                temp_text = text[start_expc:start_expc+100]
            try:
                end_expc = [end for end in end_idx if end > start_expc][0]
            except: return 0,0
        else: return 0,0
        
        return start_expc, end_expc

    def run_extractor(self):
        panel_df = pd.read_excel(self.panel_path)

        # get the sub-df for quarterly reports
        q_df = panel_df.loc[panel_df['type'] == '基金季报', :]

        # get the sub-df for annual or mid-term reports
        ah_df = panel_df.loc[panel_df['type'] != '基金季报', :]

        output = pd.DataFrame()
        for df in [q_df, ah_df]:
            code_list = list(df['code'].drop_duplicates())
            for code in tqdm(code_list):
                file_list = df.loc[ah_df['code'] == code,'filename']
                for idx in file_list.index:
                    file = self.store_path + '/' + code.split('.')[0] + '/' + file_list.loc[idx]
                    with open(file,'r', encoding = 'utf-8') as f:
                        text = f.read()

                    start, end = self.expc_detector(text, 'AH')
                    df.loc[idx,'expc_text'] = text[start:end]

            output = pd.concat([output, df])
        
        output.loc[output['expc_text'] != '',:].sort_values(by = ['code','year','quarter']).drop_duplicates(subset = ['code','year','quarter'], inplace = True)
        output.to_excel(self.store_path + '/extracted_expc.xlsx', index = False)
        return output

    
    

    
    
    