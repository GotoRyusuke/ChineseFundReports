# ChineseFundReports
This is a project to:
- :heavy_check_mark: crawl open fund reports from mainly two source:
  - [CNINF](http://www.cninfo.com.cn/), and
  - [EastMoney](https://www.eastmoney.com/);
- :heavy_check_mark: extract the expectation part from the reports;
- :heavy_check_mark: do some word freq calculation based on a specific dictionary

## Structure of the projects
### 1. CNINF CRAWLER
A **crwaler** is constructed to crawl info and reports from both source of reports. A **pdf-to-txt convertor module** specific to my task is also introduced.
Basically, The convertor module can be revised slightly to fit in with any other similar needs.

This part consists of two modules:
1. [`cninf_crawler`](./Crawler/crawling_cninf.py): the module to crawl info and reports from CNINF website
2. [`pdf2txt`](./Crawler/pdf2txt.py): the module to convert fund reports in .pdf format to .txt format.
