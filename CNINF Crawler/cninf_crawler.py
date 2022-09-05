# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A new crawler to download reports from CNINF(http://www.cninfo.com.cn/)

WorkFlow:
    i. prepare the code2orgid dict file;
    ii. for a single fund, use crawl_single_fund to get the report info you need;
        alternatively, use it in a loop;
    iii. use save_file to download the file(s) you just got; for a list of fund report info, use it in a loop

CONTENTS
--------
- <FUNC> unix2date
- <FUNC> find_info_in_title
- <FUNC> num_cn2eng
- <CLASS> cninf_crawler

OTHER INFO.
-----------
- Last upate: R4/8/24(Sui)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: 1155169839@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237
    
'''
import requests
import random
import time
import pandas as pd
import os
import datetime
import re
from cninf_orgid import find_info

def unix2date(unix_time):
    '''
    A func to convert 13-digit unix timestamp to human date
    
    Parameters
    ----------
    unix_time: datetime
        A 13-digit unix timestamp.
    
    Returns
    -------
    output: str
        human date in 'YYYY-MM-DD' format
        
    '''
    dt_obj = datetime.datetime.fromtimestamp(unix_time/1000)
    output = dt_obj.strftime('%Y-%m-%d')
    
    return output

def find_info_in_title(title:str):
    '''
    A func to find info in the title of a given report; 
    Format:
        [report_type]-[year]-[quarter],
        where:
            - report_type:
                - 'Q': quarterly report;
                - 'H': mid-term report;
                - 'A': annual report
            - year: year of issue
            - quarter: 1-4 for quarterly report; 5 for mid-term report; 6 for 
            annual report
            
    Parameters
    ----------
    title: str
        The title of a given report
    
    Returns
    -------
    A str in the format described above
        
    '''
    
    year_detector = re.compile(r'[0-9,一,二,三,四,五,六,七,八,九,零]{4}年')
    quarter_detector = re.compile(r'[1-4,一,二,三,四]季')
    annual_detector = re.compile(r'年[度,报]')
    mid_detector = re.compile(r'(半年|中期)')
    
    year = re.findall(year_detector, title)
    quarter = re.findall(quarter_detector, title)
    annual = re.findall(annual_detector, title)
    mid = re.findall(mid_detector, title)
    
    if len(year) > 0:
        year = num_cn2eng(year[0][:-1])
    else: year = ''
        
    if len(quarter) > 0:
        quarter = num_cn2eng(quarter[0][0])
        report_type = 'Q'
    elif len(mid) > 0:
        quarter = 5
        report_type  = 'H'     
    elif len(annual) > 0:
        report_type = 'A'
        quarter = 6
    else:
        report_type = ''
        quarter = ''
    
    return '-'.join([report_type, str(year), str(quarter)])

def num_cn2eng(date:str):
    '''
    A func to convert Chinese num to Arab num, e.g '二〇一二' -> 2012
    
    Parameters
    ----------
    date: str
        A str of Chinese num 
        
    Returns
    -------
    output: str
        A str of Arab num
        
    '''
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

    
class cninf_crawler:
    def __init__(self,
                 code2orgid_dict_path: str,
                 target_weblink: str):
        self.target_weblink = target_weblink
        
        ''' header-specification '''
        # a list of user_agents to be chosen randomly when posting
        self.user_agents = ["Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
                        
                            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
                        
                            "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
                            
                            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
                            
                            "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
                            
                            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
                            
                            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0"
                            ]

        # other headers info except user-agent
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                       "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                       "Accept-Encoding": "gzip, deflate",
                       "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6,zh-TW;q=0.5",
                       'Host': 'www.cninfo.com.cn',
                       'Origin': 'http://www.cninfo.com.cn',
                       'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice',
                       'X-Requested-With': 'XMLHttpRequest'
                       }
        
        ''' initialise code-orgId dict '''
        raw_dict = pd.read_excel(code2orgid_dict_path)
        self.code2orgid_dict = dict([(raw_dict.loc[idx,'code'], raw_dict.loc[idx,'post']) for idx in raw_dict.index])
        
        ''' report-type dicts for fund and stock '''
        fund_type_dict = {'all':'category_ndbg_jjgg;category_bndbg_jjgg;category_jdbg_jjgg',
                          'quarter':'category_jdbg_jjgg',
                          'annual':'category_ndbg_jjgg',
                          'mid-term':'category_bndbg_jjgg'}
        
        stock_type_dict = {'all':'category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh',
                           'quarter': 'category_yjdbg_szsh, category_sjdbg_szsh',
                           'annual': 'category_ndbg_szsh',
                           'mid-term': 'category_bndbg_szsh'}
        
        self.report_type_dicts = {'fund': fund_type_dict,
                                  'stock': stock_type_dict}
        
        print('Initialised successfully')
        print('-'*25)  
        
    def find_single_info(self, key: str, type_: str, mode: str, verbose = True):
        '''
        A method to find info about a fund/stock

        Parameters
        ----------
        key : str
            Fund code/name 
        type_: str
            Fund or stock to be found; should be one of the following:
                - 'fund'
                - 'stock'
        mode : str
            Find with fund code /name; should be one of the following:
                - 'code'
                - 'name'
        verbose: bool, default True
            Whether to show some info about the designated stock/fund

        Returns
        -------
        output: dict
            A dict containing the following info:
                - code
                - name
                - orgId
                - type(stock/fund)
                - type_d: detail type

        '''
        output = find_info(random.choice(self.user_agents), 
                           key, 
                           type_, 
                           mode)
        
        if verbose:
            print('Found')
            for key, value in output.items():
                print(f'- {key}: {value}')
                
        return output
    
    def crawl_single_fund(self,
                          code:str, 
                          start:str, 
                          end:str,
                          report_type:str,
                          type_: str,
                          verbose = False):
        '''
        A method to get the list of all reports of a given fund between 
        [start] and [end]
        
        Parameters
        ----------
        code: str
            code of the fund
        start: str
            Start of time; format: 'YYYY-MM-DD'
        end: str
            End of time; format: 'YYYY-MM-DD'
        report_type: str
            Should be one of the following:
                'quarter': quarterly report
                'annual': annual report
                'mid-term': mid-term report
                'all': all periodical reports
        type_: str
            Fund or stock to be crawled. Should be one of the following:
                - 'fund';
                - 'stock'
                
        Returns
        -------
        report_list: list
            A list of all reports satisfying the requirements above
            
        '''
        query_path = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        headers = self.headers
        headers['User-Agent'] = random.choice(self.user_agents)
        
        type_dict = self.report_type_dicts[type_]
        type_flag = type_dict[report_type]
            
        period = start+'~'+end
        
        print(f'Start crawling {code}...')
        print(f'Requirements:\n -Time:{start}\n      ~{end}\n -Report type: {report_type}\n')
        
        page_num = 1
        if type_ == 'fund':
            try:
                stock = self.code2orgid_dict[code]
                column = 'fund'
            except KeyError:
                print(f'{code} not in the code2orgid dict. Use orgId finder first.')
                
        else:
            stock = code
            column = 'szse'
        
        query = {'pageNum': page_num,
                 'pageSize': 30,
                 'tabName': 'fulltext',
                 'column': column,  
                 'stock': stock,
                 'searchkey': '',
                 'secid': '',
                 'plate': '',   
                 'category': type_flag,
                 'trade': '',
                 'seDate': period 
                 }

        # get the first page first
        namelist = requests.post(query_path, headers=headers, data=query)
        report_list = namelist.json()['announcements']
        print(f'Page {page_num} completed')
        
        # check if there are more pages, if True, continue the crawling
        while namelist.json()['hasMore']:
            time.sleep(1)
            
            # update page numbre
            page_num += 1
            query['pageNum'] = page_num
            
            # reset user-agent
            headers['User-Agent'] = random.choice(self.user_agents) 
            
            # get report list in the next page
            namelist = requests.post(query_path, headers=headers, data=query)
            report_list += namelist.json()['announcements']
            
            print(f'Page {page_num} completed')

        print('All pages completed')
        print('-'*25)
        
        if verbose:
            print('Reports found:\n')
            for record in report_list:
                print(record['announcementTitle'])
            print('-' * 35)
        return report_list
        
    def save_file(self,
                  code: str,
                  report_list: list,
                  store_path: str):
        
        '''
        A method to download files in report_list to local directory
        
        Parameters
        ----------
        code: str
            Code of the fund
        report_list: list
            A list generated by crawl_single_fund
        store_path: str
            The directory where the downloaded files will be saved
        
        Returns
        -------
        file_info: pd.DataFrame
            A df saving the info about the file downloaded, including code, filed dates, and paths
        
        '''
        print(f'Start downloading reports for {code}...')
        store_path = store_path
        target_weblink = self.target_weblink
        
        headers = {'Host': 'static.cninfo.com.cn',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': random.choice(self.user_agents),
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                   'Cookie': 'routeId=.uc1'
                   }
        file_info = pd.DataFrame(columns = ['code', 'filed_date', 'file_path', 'info'])
        idx = 0
        for report in report_list:
            title = report['announcementTitle']
            if '摘要' in title:
                # print(f'SKIP {title}: REPORT ABSTRACT')
                continue
            elif '提示性' in title:
                # print(f'SKIP {title}: SUGGESTIVE ANNOUNCEMENT')
                continue
            elif '推迟' in title:
                # print(f'SKIP {report}: POSTPONEMENT OF ISSUE')
                continue
            elif '延缓' in title:
                continue
            else:
                download_url = target_weblink + report["adjunctUrl"]
                
                try:
                    '''
                    Occasionaly some of the info necessary for initialising the 
                    file cannot be found; skip such files
                    
                    '''
                    # initialise the file name
                    filed_date = unix2date(report['announcementTime'])
                    file_format = report['adjunctType'].lower()
                    file_name = f'{title}_{filed_date}.{file_format}'
                    
                except: continue
            
                # initialise the folder
                folder_name = store_path + '/' +  code
                os.makedirs(folder_name, exist_ok = True)
                    
                # initialise the full path of the file
                file_path = folder_name + '/' + file_name
    
                time.sleep(random.randint(2,4))
                headers['User-Agent'] = random.choice(self.user_agents)
                r = requests.get(download_url, headers=headers)
                time.sleep(5)
                with open(file_path, 'wb') as f:
                    f.write(r.content)
                
                file_info.loc[idx, :] = code, filed_date, file_path, find_info_in_title(title)
                idx += 1
                print(f'{idx}/{len(report_list)} done')
                

        print('Saving procedure completed')
        print(f'Skipped {len(report_list)-idx} file(s)')
        print('-'*35)
        return file_info
    
    
if __name__ == '__main__':
    target_weblink = 'http://static.cninfo.com.cn/'
    store_path = 'F:/eastmoney/test'
    code2orgid_dict_path = 'F:/eastmoney/cninf_orgid_dict.xlsx'
    
    my_crawler = cninf_crawler(code2orgid_dict_path = code2orgid_dict_path,
                               target_weblink = target_weblink)
    test_find = my_crawler.find_single_info('000001', type_ = 'stock', mode = 'code')
    
    
        
            
    
    
