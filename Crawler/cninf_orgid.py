# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
A class to fatch orgid for each code, either its fund name or fund code

NOTE: Some fund codes and their respective orgids have been saved to the file named cninf_orgid_dict.xlsx.
Check whether it contains the orgid you are looking for.

WorkFlow:
    i. prepare a list of fund names or codes
    ii. use init_key2orgid_dict

CONTENTS
--------
- <CLASS> cninf_orgid_finder

'''
import requests
import random
import time

class cninf_orgid_finder:
    def __init__(self):
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
        
        print('\nOBJECT INITIALISED SUCCESSFULLY.')
        print('='*32)
        
    def get_orgid(self, key:str, mode: str):
        '''
        A method to find the 'orgid' of a fund that is required when posting
        
        Parameters
        ----------
        key: str
            Code or Name of the fund
        mode: str
            Must be one of the followings:
                - 'name': use name as the key, or
                - 'code': use fund code as the key
        
        Returns
        -------
        orgid: str
            orgid of the fund
            
        '''
        
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
        
        try:
            if mode == 'name':
                url = 'http://www.cninfo.com.cn/new/information/topSearch/detailOfQuery'
                data = {'keyWord': key,
                        'maxSecNum': 10,
                        'maxListNum': 5,
        				}
                r = requests.post(url, headers=hd, data=data)
                org_id = r.json()['keyBoardList'][0]['orgId']
                
            elif mode == 'code':
                url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
                data = {'keyWord': key,
                       'maxNum': 10}
                r = requests.post(url, headers=hd, data=data)
                
                org_id = 'error'
                for record in r.json():
                    if '基金' in record['category'] or 'QDII' in record['category'] :
                        org_id = record['orgId']
                        break            
        except:
            org_id = 'error'
        
        return org_id
    
    def init_key2orgid_dict(self, key_list: list, mode: str):
        '''
        A method to find the orgids for a list of funds given their keys
        
        Parameters
        ----------
        key_list: list
            A list of keys
        mode: str
            Must be one of the followings:
                - 'name', or
                - 'code'
        
        Returns
        -------
        output_dict: dict
            A dict in the following format:
                {code: 'code,orgid'}
        failed_keys: list
            A list of failed keys, either because the fund does not exist or 
            failed internet connection
        
        '''
        
        output_dict = {}
        failed_keys = []
        for key in key_list:
            try:
                org_id = self.get_orgid(key, mode)
            except:
                print('SLEEP FOR A WHILE...')
                time.sleep(10)
                print('RESUMED...')
                org_id = self.get_orgid(key, mode)

            if org_id == 'error':
                print(f'{key} not found or internet connection failed.')
                failed_keys.append(key)
                continue
            
            output_dict[key] = org_id
            time.sleep(2)
        return output_dict, failed_keys

if __name__ == '__main__':
    finder = cninf_orgid_finder()
    
    test_keys = ['000001', '000003', '0000019']
    test_orgids, test_fails = finder.init_key2orgid_dict(test_keys, mode = 'code')
