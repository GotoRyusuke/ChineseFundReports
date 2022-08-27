# ChineseFundReports(巨潮资讯·东方财富基金报表爬取及项目提取)
--------------------------------------------------------------------
### (R4/8/27) Updates
`cninf_orgid` and `cninf_crawler` now support crawling info for stocks. Only a few stocks are tested, so some post data may have to be changed
if you find it returns an *error*.

### (R4/8/24) Updates
Methods for fatching orgID, which is necessary when we are posting data to the server, is separated from cralwer class to a new class named cninf_orgid_finder;
the workflow is now simpler and the codes more compact.

### (R4/8/19) Updates
Main logic for counting word & sent freqs has been separated to a new module named [`chinese_counter`](./Word_freq/chinese_counter.py) to make the main classes under `Word_freq` tidier.

## Introduction
This is a project to:
- :heavy_check_mark: crawl open fund reports from mainly two sources:
  - [CNINF](http://www.cninfo.com.cn/), and
  - [EastMoney](https://www.eastmoney.com/);
- :heavy_check_mark: convert pdf reports to txt file, and extract the expectation part from the reports;
- :heavy_check_mark: train a **word2vec** model and get some similar words for dicts at hand based on the corpus constructed using fund reports;
- :heavy_check_mark: do some word freq calculation based on a specific dictionary

## Structure of the project
### 1. CNINF CRAWLER
A **crawler** is constructed to crawl info and reports from both source of reports. A **pdf-to-txt convertor class** specific to my task is also introduced.
Basically, the convertor class can be revised slightly to fit in with any other similar needs.

This part consists of 3 classes:
1. [`cninf_crawler`](./Crawler/crawling_cninf.py): the class to crawl info and reports from CNINF website;
2. [`pdf2txt`](./Crawler/pdf2txt.py): the class to convert fund reports in .pdf format to .txt format;
3. [`extract_expc_from_report`](./Crawler/extract_expc_from_report.py): the class to extract the expectation part from a certain report, if any.

### 2. WORD2VEC
This part provides two ways to train a word2vec model based on the corpus at hand

1. method I:
    1. [`process_1`](./word2vec/pre_process_1.py)reads the available reports one by one, aggregates all processed reports;
    into a single file, where each line is a sentence
    2. [`process_2`](./word2vec/pre_process_2.py)imports files and removes stop words;
    3. [`word2vec_train`](./word2vec/word2vec_train.py)trains word2vec one the single file derived in step 2, and finds the synonyms
    for the words in our preliminary Chinese LM dicts
2. method II:
    Instead of first aggregating all texts in the corpus and training the model, [`iter_train`](./word2vec/iter_train.py) uses an iterable to provide content to the model for training, which is more recommended by gensim. Refer to [gensim](https://radimrehurek.com/gensim/apiref.html#api-reference) for more details.

After the model params are set, use [`get_similar`](./word2vec/get_similar.py) class to get similar words for our dicts at hand. This class provides two ways (implemented with 4 methods) to get similar words. Check the comments for detailed usage.

### 3. EASTMONEY CRAWLER
This module is a crawler built by someone else in the team and I just take it. EastMoney does not have as many reports as CNINF does, so I hardly use it since my cninf crawler was built. The codes are not easy to read, and I **highly recommend** you to use cninf crawler instead as it is more organised.


### 4. WORD FREQ CALCULATOR
This part provides two classes to extract text and construct panel data based on existing reports files(in .txt format). 
- Since some of the texts under the expectation part in a report are directly exported from WIND terminal, I developed the module [`extract_expc_panel`](./Word_freq/extract_expc_panel.py) to extract panel info directly from a excel table, where the texts under expec as well as fund info are saved.
- After saving the reports downloaded by the crawler and converted to .txt format(refer to [`Crawler` repository](./Crawler/)), use [`extract_full_panel`](./Word_freq/extract_full_panel.py) to generate a excel table that summarises the report info.

After we get two panels, use
- [`cal_expc_word_freq`](./Word_freq/cal_expc_word_freq.py), and
- [`cal_word_freq`](./Word_freq/cal_word_freq.py)

to calculate word and sent freq based on the content of the reports. The [`utils`](./Word_freq/utils.py) module saves some funcs that I think should be isolated from the two classs mentioned above to make the modules tidier.

**(R4/8/19) Updates**: the main logic for counting word & sent freq has been isolated to a new module named [`chinese_counter`](./Word_freq/chinese_counter.py).

Finally, after linking the results of expc to full panel, use [`fill_empty_expc`](./Word_freq/fill_empty_expc.py) to fill in the reports where the expectation part is not filed. The filling rules are summarised in the Chinese version logs which is not open to the public.
