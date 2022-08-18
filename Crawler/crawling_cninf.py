# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A new crawler to download reports from CNINF(http://www.cninfo.com.cn/)

WorkFlow:
    i. prepare a list of fund names to be crawled -> name_list
    ii. self.init_code_orgid_dict(name_list) -> code_orgid_dict
    iii. 
        (i). given a fund code, get the 'code,orgid' pair using code_orgid_dict
        (ii). self.crawl_single_fund('code,orgid', *other_args) -> report_list
        (iii). self.saving(report_list)

CONTENTS
--------
- <FUNC> unix2date
- <FUNC> find_info
- <FUNC> num2eng
- <CLASS> cninf_crawler

OTHER INFO.
-----------
- Last upate: R4/8/1(Getsu)
- Author: GOTO Ryusuke 
- Contact: 
    - Email: yuhang1012yong@link.cuhk.edu.hk (preferred)
    - WeChat: L13079237
'''
import requests
import random
import time
import pandas as pd
import os
import datetime
import re
from tqdm import tqdm

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

def find_info(title:str):
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
                 code_info_dict_path: str):
        print('START INITIALISING OBJECT...')
        
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
        
        raw_dict = pd.read_csv(code_info_dict_path, encoding = 'gbk')
        raw_dict.columns = ['code','name', 'inv_typeI', 'inv_type_II', 'fund_type']
        self.code_info_dict = dict([(raw_dict.iloc[idx,1], raw_dict.iloc[idx,0].split('.')[0]) for idx in range(len(raw_dict))])
        
        print('OBJECT INITIALISED SUCCESSFULLY.\n')
        print('='*35)

    def get_orgid(self, fund_name:str):
        '''
        A method to find the 'orgid' of a fund that is required when posting
        
        Parameters
        ----------
        fund_name: str
            Name of the fund
        
        Returns
        -------
        orgid: str
            orgid of the fund
            
        '''

        url = 'http://www.cninfo.com.cn/new/information/topSearch/detailOfQuery'
        hd = {
            'Host': 'www.cninfo.com.cn',
            'Origin': 'http://www.cninfo.com.cn',
            'Pragma': 'no-cache',
            'Accept-Encoding': 'gzip,deflate',
            'Connection': 'keep-alive',
            'Content-Length': '70',
            'User-Agent':  random.choice(self.user_agents),
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        
        data = {'keyWord': fund_name,
                'maxSecNum': 10,
                'maxListNum': 5,
				}
        
        r = requests.post(url, headers=hd, data=data)
        try:
            org_id = r.json()['keyBoardList'][0]['orgId']
        except:
            url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
            hd['User-Agent'] = random.choice(self.user_agents)
            data = {'keyWord': self.code_info_dict[fund_name],
                    'maxNum': 10}
            
            r = requests.post(url, headers=hd, data=data)
            for record in r.json():
                if '基金' in record['category'] or 'QDII' in record['category'] :
                    org_id = record['orgId']
                    return org_id
            
            return 'error'
        
        return org_id
    
    def get_orgid_by_code(self, code: str):
        '''
        A method to find the 'orgid' of a fund that is required when posting
        
        Parameters
        ----------
        code: str
            Code of the fund
        
        Returns
        -------
        orgid: str
            orgid of the fund
            
        '''

        url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
        hd = {
            'Host': 'www.cninfo.com.cn',
            'Origin': 'http://www.cninfo.com.cn',
            'Pragma': 'no-cache',
            'Accept-Encoding': 'gzip,deflate',
            'Connection': 'keep-alive',
            'Content-Length': '70',
            'User-Agent':  random.choice(self.user_agents),
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        
        data = {'keyWord': code,
               'maxNum': 10}
        
        r = requests.post(url, headers=hd, data=data)
        for record in r.json():
            if '基金' in record['category'] or 'QDII' in record['category'] :
                org_id = record['orgId']
                return org_id
        return 'error'
    
    def init_code_orgid_dict(self, name_list:list):
        '''
        A method to find the orgids for a list of funds given their names.
        
        Parameters
        ----------
        name_list: list
            A list of fund names
        
        Returns
        -------
        code2orgid_dict: dict
            A dict in the following format:
                {code: 'code,orgid'}
        
        '''
        print('START INITIALISING CODE-2-CODE-ORGID DICT...')
        code_orgid_list = []

        for name in tqdm(name_list):   
            try:
                orgid = self.get_orgid(name)
            except:
                print('STOP FOR A WHILE...')
                time.sleep(10)
                print('RESUME...')
                orgid = self.get_orgid(name)
                
            code = self.code_info_dict[name]
            
            if orgid == 'error': 
                print(f'{name} NOT FOUND!')
                value = 'not found'
                code_orgid_list.append((code, value))
                continue
            
            value = ','.join([code,
                              orgid])
            code_orgid_list.append((code, value))
            time.sleep(2)
        
        code_orgid_dict = dict(code_orgid_list)
        print('CODE-2-CODE-ORGID DICT INISITALISED.\n')
        
        return code_orgid_dict        
    
    def init_code_orgid_dict_by_code(self,code_list:list):
        '''
        A method to find the orgids for a list of funds given their codes
        
        Parameters
        ----------
        name_list: list
            A list of fund codes.
        
        Returns
        -------
        code2orgid_dict: dict
            A dict in the following format:
                {code: 'code,orgid'}
        
        '''
        print('START INITIALISING CODE-2-CODE-ORGID DICT...')
        
        code_orgid_list = []

        for code in tqdm(code_list): 
            try:
                orgid = self.get_orgid_by_code(code)
            except:
                print('STOP FOR A WHILE...')
                time.sleep(10)
                print('RESUME...')
                orgid = self.get_orgid_by_code(code)
                
            if orgid == 'error': 
                value = 'not found'
                code_orgid_list.append((code, value))
                continue
            value = ','.join([code,
                              orgid])
            code_orgid_list.append((code, value))
            time.sleep(2)
        
        code_orgid_dict = dict(code_orgid_list)
        print('CODE-2-CODE-ORGID DICT INISITALISED.\n')
        
        return code_orgid_dict          
    
    def crawl_single_fund(self,
                          stock:str, 
                          start:str, 
                          end:str,
                          report_type:str):
        '''
        A method to get the list of all reports of a given fund between 
        [start] and [end]
        
        Parameters
        ----------
        stock: str
            format: 'FundCode,orgid'
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
                
        Returns
        -------
        fund_code: str
            Code of the fund
        report_list: list
            A list of all reports satisfying the requirements above
            
        '''
        query_path = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        headers = self.headers
        headers['User-Agent'] = random.choice(self.user_agents)
        
        type_dict = {'all':'category_ndbg_jjgg;category_bndbg_jjgg;category_jdbg_jjgg',
                     'quarter':'category_jdbg_jjgg',
                     'annual':'category_ndbg_jjgg',
                     'mid-term':'category_bndbg_jjgg'}
        
        type_flag = type_dict[report_type]
            
        period = start+'~'+end
        
        fund_code =  stock.split(',')[0]
        print('START CRAWLING FUND ' + fund_code + '...')
        print(f'REQUIREMENTS:\n -TIME:{period}\n -REPORT TYPE: {report_type}\n')
        
        page_num = 1
        
        query = {'pageNum': page_num,
                 'pageSize': 30,
                 'tabName': 'fulltext',
                 'column': 'fund',  
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
        print(f'PAGE {page_num} COMPLETED.')
        
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
            
            print(f'PAGE {page_num} COMPLETED.')

        # print('\n -NUM. OF REPORTS FOUND: %s \n'%(len(report_list)))
        print('CRAWLING PROCEDURE COMPLETED.')
        print('='*35)
        return fund_code, report_list

    def saving(self, 
               fund_code: str,
               saving_path: str, 
               target_weblink: str,
               report_list: list):
        
        '''
        A method to download files in report_list to local directory
        
        Parameters
        ----------
        fund_code: str
            Code of the fund
        saving_path: str
            Local directory where the files will be saved
        target_weblink: str
            Main url of cninf
        report_list: star
            A list generated by crawl_single_fund
        
        Returns
        -------
        None
        
        '''
        print(f'START DOWNLOADING REPORTS FOR {fund_code}...')
        
        headers = {'Host': 'static.cninfo.com.cn',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': random.choice(self.user_agents),
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                   'Cookie': 'routeId=.uc1'
                   }
        
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
                folder_name = saving_path + '/' +  fund_code
                if not os.path.exists(folder_name):
                    os.makedirs(folder_name)
                    
                # initialise the full path of the file
                file_path = folder_name + '/' + file_name
    
                time.sleep(random.randint(2,4))
                headers['User-Agent'] = random.choice(self.user_agents)
                r = requests.get(download_url, headers=headers)
                time.sleep(5)
                with open(file_path, 'wb') as f:
                    f.write(r.content)
                    
                # print(f'{title} SAVED SUCCESSFULLY.')

        print('SAVING PROCEDURE COMPLETED.')
        print('\n' + '='*35)

if __name__ == '__main__':
    target_weblink = 'http://static.cninfo.com.cn/'
    saving_path = 'F:/eastmoney/CNINF'
    code_info_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/raw_data/FundCode.csv'

    
    # creat cninf_crawler instance
    my_crawler = cninf_crawler(code_info_dict_path = code_info_dict_path)
    
    panel_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word_freq/tables/panel2.csv'
    
    # load code2post dict
    code2post_dict = pd.read_excel('F:/eastmoney/cninf_orgid_dict.xlsx')
    keys = [key for key in code2post_dict['code']]
    values = [value for value in code2post_dict['post']]   
    code2post_dict = dict([(keys[idx], values[idx]) for idx in range(len(keys))])
    
     
    before_08 = pd.DataFrame(columns = ['code', 'date', 'year',
                                        'fund_name','quarter','type',
                                        'filename','info'])
    
    '''
    The following section is to create a df to save the info of crawling task
    specific to my needs, therefore no need to understand it thoroughly if you
    have a different task to do. 
    
    Maybe you can try to incorporate the function of generating a pandas df
    into the class, which makes the procedure tidier.
    
    '''
    idx = 0
    for code in keys:
        code = keys[1]
        stock = code2post_dict[code]
        test_fund_code, test_rl = my_crawler.crawl_single_fund(stock = stock,
                                                               start = '2004-01-01',
                                                               end = '2008-06-01',
                                                               report_type = 'all')
       
        for record in test_rl:
            title = record['announcementTitle']
            if '摘要' in title or '推迟' in title or '提示性' in title: continue
        
            before_08.loc[idx, 'code'] = record['secCode']
            before_08.loc[idx, 'date'] = unix2date(record['announcementTime'])
            before_08.loc[idx, 'filename'] = title
            before_08.loc[idx, 'fund_name'] = record['secName']
            
            try:
                info = find_info(title)
                before_08.loc[idx, 'info'] = info
                before_08.loc[idx, 'type'] = info.split('-')[0]
                before_08.loc[idx, 'year'] = info.split('-')[1]
                before_08.loc[idx, 'quarter'] = info.split('-')[2]
            except: pass
            idx += 1
        
            
    
    
