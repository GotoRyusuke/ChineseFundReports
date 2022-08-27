# -*- coding: utf-8 -*-
'''
Test & sample usage of the crawler modules

'''
from cninf_orgid import cninf_orgid_finder
from cninf_crawler import cninf_crawler

finder = cninf_orgid_finder()

'''
Find the orgID for the fund '华夏成长'(000001) using:
    i. fund name;
    ii. fund code
'''
# i
orgid_i = finder.get_orgid('华夏成长', mode = 'name')
# >>> 'jjjl0000031'

# ii
orgid_ii = finder.get_orgid('000001')
# >>> 'jjjl0000031'

'''
The method can also be used to find orgID for listed stock.
Suppose we want to find orgID for '平安银行'(000001).

'''
# i
stockid_i = finder.get_orgid('平安银行', mode = 'name', type_ = 'stock')
# >>> 'gssz0000001'

# ii
stockid_ii = finder.get_orgid('000001', mode = 'code', type_ = 'stock')
# >>> 'gssz0000001'


'''
Try to crawl info about all reports of '华夏成长'(000001) from 2020-01-01
to 2022-01-01
The orgID is already known: jjjl0000031

'''
target_weblink = 'http://static.cninfo.com.cn/'
store_path = 'F:/eastmoney/test'
code2orgid_dict_path = 'F:/eastmoney/cninf_orgid_dict.xlsx'
crawler = cninf_crawler(code2orgid_dict_path, store_path, target_weblink)

fund_reports = crawler.crawl_single_fund('000001', '2020-01-01', '2022-01-01', 
                                         'all', 'fund', True)

stock_reports = crawler.crawl_single_fund('000001,gssz0000001', '2020-01-01', '2022-01-01', 
                                         'all', 'stock', True)
