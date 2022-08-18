# ChineseFundReports
This is a project to:
- :heavy_check_mark: crawl open fund reports from mainly two sources:
  - [CNINF](http://www.cninfo.com.cn/), and
  - [EastMoney](https://www.eastmoney.com/);
- :heavy_check_mark: extract the expectation part from the reports;
- :heavy_check_mark: do some word freq calculation based on a specific dictionary

## Structure of the projects
### 1. CNINF CRAWLER
A **crawler** is constructed to crawl info and reports from both source of reports. A **pdf-to-txt convertor module** specific to my task is also introduced.
Basically, The convertor module can be revised slightly to fit in with any other similar needs.

This part consists of two modules:
1. [`cninf_crawler`](./Crawler/crawling_cninf.py): the module to crawl info and reports from CNINF website;
2. [`pdf2txt`](./Crawler/pdf2txt.py): the module to convert fund reports in .pdf format to .txt format;
3. [`extract_expc_from_report`](./Crawler/extract_expc_from_report.py): the module to extract the expectation part from a certain report, if any.

### 2. WORD2VEC
This part provides two methods to train a word2vec model based on the corpus at hand

1. method I:
    1. [`process_1`](./word2vec/pre_process_1.py)read the available reports one by one, aggregate all processed reports;
    into a single file, where each line is a sentence
    2. [`process_2`](./word2vec/pre_process_2.py)import files, remove stop words;
    3. [`word2vec_train`](./word2vec/word2vec_train.py)train word2vec one the single file derived in step 2, and find the synonyms
    for the words in our preliminary Chinese LM dicts
2. method II:
    Instead of first aggregating all texts in the corpus and training the model, [`iter_train`](./word2vec/iter_train.py) uses an iterable to provide content to the model for training, which is more recommended by gensim. Refer to gensim for more details.

### 3. EASTMONEY CRAWLER
This module is a crawler built by someone else in the team and I just take it. EastMoney does not have as many reports as CNINF does, so I hardly use it since my cninf crawler was built. The codes are not easy to read, and I **highly recommend** you to use cninf crawler instead as it is more organised.


### 4. WORD FREQ CALCULATOR
This part provides two modules to extract text and construct panel data based on existing reports files(in .txt format). 
- Since some of the texts under the expectation part in a report are directly exported from WIND terminal, I developed the module [`extract_expc_panel`](./Word_freq/extract_expc_panel.py) to extract panel info directly from a excel table, where the texts under expec as well as fund info are saved.
- After saving the reports downloaded by the crawler and converted to .txt format(refer to [`Crawler` repository](./Crawler/)), use [`extract_full_panel`](./Word_freq/extract_full_panel.py) to generate a excel table that summarises the report info.

After we get two panels, use
- [`cal_expc_word_freq`](./Word_freq/cal_expc_word_freq.py), and
- [`cal_word_freq`](./Word_freq/cal_word_freq.py)

to calculate word and sent freq based on the content of the reports. The [`utils`](./Word_freq/utils.py) module saves some funcs that I think should be isolated from the two moduls mentioned above to make the modules tidier. 

Finally, after link the results of expc to full panel, use [`fill_empty_expc`](./Word_freq/fill_empty_expc.py) to fill in the reports where the expectation part is not filed. The filling rules are summarised in the Chinese version logs which is not open to the public.
