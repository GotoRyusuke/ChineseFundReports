# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A new module to convert pdf file to txt.

CONTENTS
--------
- <FUNC> convert_num2code
- <CLASS> My_pdf2txt

OTHER INFO.
-----------
- Last upate: R4/8/9(Ka)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: yuhang1012yong@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237

'''

import PyPDF2
import os
import pandas as pd
from tqdm import tqdm
import time

def convert_num2code(code_list:list):
    '''
    A func to convert fund code to standard 6-digit format, e.g 1 -> 000001

    Parameters
    ----------
    code_list : list
        A list of fund codes to be processed.

    Returns
    -------
    output : list
        A list of und codes in standard format.

    '''
    
    output = []
    for code in code_list:
        code = str(code)
        code = '0'*(6-len(code)) + code 
        output.append(code)
        
    return output


class My_pdf2txt:
    def __init__(self,
                 pdf_file_path: str,
                 store_path: str,
                 summary_df_path: str,
                 identifier,
                 identified):
        
        self.pdf_file_path = pdf_file_path
        self.store_path = store_path
        
        '''
        Extract observations whose 'identified' satisfies 'identifier', e.g 
        by setting:
            - identified = 'source', and
            - identifier = 'eastmoney',
        we are trying to extract observations from eastmoney database.
        
        Removing these two args has no effect on the methods followed.
        
        '''
        
        summary_df = pd.read_excel(summary_df_path)
        self.summary_df = summary_df.loc[summary_df[identified] == identifier,]
        
        self.summary_df['code'] = convert_num2code([code for code in self.summary_df['code'].values])
        self.code_list = list(self.summary_df['code'].drop_duplicates())
        
    def process_single_file(self, code: str, file_name: str):
        '''
        Convert a single pdf file to a string of content in the file.

        Parameters
        ----------
        code : str
            Fund code of the report.
        file_name : str
            File name of the report.

        Returns
        -------
        text : str
            Returns the error info if failed to decode to utf-8.

        '''
        
        # read pdf file as binary codes
        file_path = '/'.join([self.pdf_file_path,code, file_name])
        pdf_file = open(file_path,'rb')
        
        # initialise PdfFileReader instance
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        # get the num of pages in the file
        num_page = pdf_reader.numPages
        
        # read the file content in iteration
        text = ''
        for page in range(num_page):
            page_obj = pdf_reader.getPage(page)
            text += page_obj.extractText().replace('\n', '')
        
        pdf_file.close()
        
        # A series of encode-decoding procedures to insure the content can be decoded by utf-8
        # meaningless symbols removed in this step
        text = text.encode('gbk',errors='ignore').decode('gbk').encode('utf-8').decode('utf-8')
        
        # save the txt file to the folder named as the fund code
        store_path = '/'.join([self.store_path, code, file_name.split('.')[0]])
        try:
            with open(store_path + '.txt', 'w', encoding = 'utf-8') as f:
                f.write(text)
        except UnicodeEncodeError: text = 'UnicodeEncodeError'
        
        return text
    
    def process_single_code(self, code:str):
        '''
        Process a list of reports from a fund

        Parameters
        ----------
        code : str
            Fund code.

        Returns
        -------
        extract_suc : list
            A list of dummies to record whether the reports are processed successfully.
            = 1 if succeed.

        '''
        # get the list of reports under a single fund
        code_file_list = list(self.summary_df.loc[self.summary_df['code'] == code, 'file_name'].values)
        code_path = f'{self.pdf_file_path}/{code}'
        file_list = os.listdir(code_path)
        
        # create a folder to save the processed reports under the fund, if not exists
        code_store_path = f'{self.store_path}/{code}'
        if not os.path.exists(code_store_path):
            os.makedirs(code_store_path)
        extract_suc = []
        
        # apply process_single_file method to each of the report in the list
        for file in file_list:
            if file not in code_file_list: continue
            esuc = 1
            formt = file.split('.')[1]
            if formt == 'pdf':
                try:
                    text = self.process_single_file(code, file)
                except: continue
                
                if text == 'UnicodeEncodeError':
                    esuc = 0
            
            extract_suc.append(esuc)
        return extract_suc
          
if __name__ == '__main__':
    path_cninf = 'F:/eastmoney/CNINF'
    path_cninf_ante08 = 'F:/eastmoney/CNINF_ante08'
    path_eastmoney = 'F:/eastmoney/Reports'
    path_summary = 'F:/eastmoney/sup_round3_summary_ante08.xlsx'
    path_store = 'F:/eastmoney/SupFiles' 
    
    my_pdf2txt = My_pdf2txt(pdf_file_path = path_eastmoney, store_path = path_store,
                            summary_df_path = path_summary,
                            identifier = 'eastmoney', identified = 'source')
    
    test_summary = my_pdf2txt.summary_df


    