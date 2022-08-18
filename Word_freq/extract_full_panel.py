'''
This is a module to 
1.extract the info about fund code, report date, filed date,
from the title and path of a report;
2. aggregate all info into a single panel data format, and;
3. export as an excel file
'''

import os
from joblib import Parallel, delayed
import pandas as pd
import numpy as np
import re
from tqdm import tqdm

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

class panel_info_extractor: 
    def __init__(self,
                 code_info_dict_path: str, 
                 all_file_path: str):
        
        self.all_file_path = all_file_path
        
        raw_dict = pd.read_csv(code_info_dict_path, encoding = 'gbk')
        raw_dict.columns = ['code','name', 'inv_typeI', 'inv_type_II', 'fund_type']
        code_info_dict = dict([(raw_dict.iloc[idx,0].split('.')[0], raw_dict.iloc[idx,:]) for idx in range(len(raw_dict))])
        self.code_info_dict = code_info_dict
        
        self.code_list = os.listdir(all_file_path)
        self.code_file_path_dict = dict([(code, all_file_path + '/' + code) for code in self.code_list])
        
    def process_single_code_info(self,code: str):
        code_file_path = self.code_file_path_dict[code]
        code_file_list = os.listdir(code_file_path)
        
        report_info_df = pd.DataFrame(columns = ['file_name','report_type', 'year', 'quarter',
                                                 'date','info','type',
                                              'report_year', 'month', 'day'])
        
        year_detector = re.compile(r'[0-9,一,二,三,四,五,六,七,八,九,零]{4}年')
        quarter_detector = re.compile(r'[1-4,一,二,三,四]季')
        annual_detector = re.compile(r'年[度,报]')
        mid_detector = re.compile(r'(半年|中期)')
        month_detector = re.compile(r'年[0-9,一,二,三,四,五,六,七,八,九,十]{1,2}月')
        
        for file in code_file_list:
            if '摘要' in file: continue
            
            single_report_info_df = pd.DataFrame(columns = ['file_name','report_type', 'year', 'quarter',
                                                            'date','info','type',
                                                            'report_year', 'month', 'day'])
            single_report_info_df.loc[0,'file_name'] = file
        
            report_date = file.split('_')[1].split('.')[0]
            single_report_info_df.loc[0,'date'] = report_date
            single_report_info_df.loc[0,['report_year', 'month', 'day']] = [int(num) for num in report_date.split('-')]
            
            title = file
            year = re.findall(year_detector, title)
            quarter = re.findall(quarter_detector, title)
            annual = re.findall(annual_detector, title)
            mid = re.findall(mid_detector, title)
            month = re.findall(month_detector, title)
            
            if len(year) > 0:
                single_report_info_df['year'] = num_cn2eng(year[0][:-1])
            if len(quarter) > 0:
                quarter = num_cn2eng(quarter[0][0])
                    
                single_report_info_df['report_type'] = 'Q'
                single_report_info_df['quarter'] = quarter
                
            if len(mid) > 0:
                single_report_info_df['report_type'] = 'H'
                single_report_info_df['quarter'] = '5'
            elif len(annual) > 0:
                single_report_info_df['report_type'] = 'A'
                single_report_info_df['quarter'] = '6'
            elif len(month) > 0:
                month = num_cn2eng(month[0][1:-1])
                
                single_report_info_df['report_type'] = 'M'
                single_report_info_df['quarter'] = num_cn2eng(month)
        
            if len(single_report_info_df) > 0:
                try:
                    single_report_info_df.loc[0,'info'] = '-'.join([single_report_info_df['report_type'].values[0],
                                                                    single_report_info_df['year'].values[0],
                                                                    single_report_info_df['quarter'].values[0]])
                except: pass
                single_report_info_df.loc[0,'type'] = get_type(single_report_info_df['report_type'].values[0])
                report_info_df = pd.concat([report_info_df, single_report_info_df], axis = 0)
                
        
        report_info_df = report_info_df.reset_index(drop = True)
        
        if len(report_info_df) == 0: return report_info_df
        
        basic_info_df = pd.concat([self.code_info_dict[code]]*len(report_info_df), ignore_index = True,axis = 1).transpose()
        output = pd.concat([basic_info_df, report_info_df], axis = 1)
        output = output.sort_values(['report_year','month','day'])
        return output
    
    def process_code_list(self, code_list: list):
        aggregate_info = pd.DataFrame()
        for code in tqdm(code_list):
            code_info_df = self.process_single_code_info(code)
            if len(code_info_df) > 0:
                aggregate_info = pd.concat([aggregate_info, code_info_df])
        
        output = aggregate_info.reset_index(drop = True)
        return(output)
    
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
            sub_info_df = self.process_code_list(sub_code_list)
            return sub_info_df
            
        output = Parallel(n_jobs=jobs, verbose=1)(delayed(multi_run)(sub_list) for sub_list in code_list_cut)
        return output
        
            
if __name__ == "__main__":
    code_info_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/FundCode.csv'
    all_file_path = 'F:/eastmoney/SupFiles' 
    save_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/comp_panel.xlsx'
    
    # code_info_dict_path = '/home/users/michaelfan/data/ConfCall/eastmoney/word_freq/FundCode.xlsx'
    # all_file_path = '/home/users/michaelfan/data/ConfCall/eastmoney/FullTexts'
    # save_path = '/home/users/michaelfan/data/ConfCall/eastmoney/word_freq/panel.xlsx'
    
    extractor = panel_info_extractor(code_info_dict_path, all_file_path)
    
    threading_results = extractor.threading(2)
    # test = extractor.process_code_list(extractor.code_list)
    # test = extractor.process_single_code_info('000001')
    
    final_df = pd.DataFrame()
    for df in threading_results:
        final_df = pd.concat([final_df, df])
    
    final_df.to_excel(save_path, index = False, encoding = 'gbk')

            
        
        