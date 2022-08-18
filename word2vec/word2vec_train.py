'''
Expand the translated LM dict (strong modal, weak modal, uncertainty, and certain)
Steps:
    1. read the available reports one by one, aggregate all processed reports
    into a single file, where each line is a sentence
    2. import files, remove stop words
    3. train word2vec one the single file derived in step 2, and find the synonyms
    for the words in our preliminary Chinese LM dicts

This file is for the step 3
'''
import os
from gensim.models import word2vec
import pandas as pd
import numpy as np
import openpyxl
import time 

# model_path = "C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec"
model_path = '/home/users/michaelfan/data/ConfCall/eastmoney/word2vec_model/modelver1'

raw_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/lm_cn_dict2.csv'
# raw_dict_path = '/home/users/michaelfan/data/ConfCall/eastmoney/lm_cn_dict2.csv'
content_path = "all_reports_sentences.txt" 
model_name = model_path + "/120vec_5win.model"
similar_word_df_name = model_path + "/similar_word"

#  hyperparameters
sg = 0  
hs = 0  
vector_size = 120 
window_size = 5  
min_count = 20  
workers = 32 
epochs = 25  
batch_words = 100000 

train_data = word2vec.LineSentence(content_path)
model = word2vec.Word2Vec(
    train_data,
    min_count=min_count,
    vector_size=vector_size,
    workers=workers,
    epochs=epochs,
    window=window_size,
    sg=sg,
    hs=hs,
    batch_words=batch_words
)
start_time = time.time()
model.save(model_name)
print("--- %s seconds ---" % (time.time() - start_time))

def get_similar_words(raw_words, model):
    similar_word_df = pd.DataFrame()
    for word in raw_words:
        try:
            similar_words_list = []
            similar_words_score_list = []
            for item in model.wv.most_similar(word.lower(), topn=200):
                similar_words_list.append(item[0])
                similar_words_score_list.append(item[1])
            similar_word_df[word] = similar_words_list
            similar_word_df[word + "_score"] = similar_words_score_list
            print(word, "done!")
        except KeyError:
            print(word, "not found!")
            similar_word_df[word] = np.nan
            similar_word_df[word + "_score"] = np.nan
          
    return similar_word_df
model_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/eastmoney_word2vec_model'
model_name = model_path + "/v3/120vec_10win.model"
similar_word_df_name = model_path + "/v3/similar_word"

print("load model...")
model = word2vec.Word2Vec.load(model_name)

raw_dict_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/dicts/lm_cn_dict2.csv'
raw_dict = pd.read_csv(raw_dict_path, encoding = 'gbk')

raw_sm = raw_dict.iloc[:,0].dropna()
raw_wm = raw_dict.iloc[:,1].dropna()
raw_unce = raw_dict.iloc[:,2].dropna()
raw_ce = raw_dict.iloc[:,3].dropna()


similar_sm_df = get_similar_words(raw_sm, model = model)
similar_wm_df = get_similar_words(raw_wm, model = model)
similar_unce_df = get_similar_words(raw_unce, model = model)
similar_ce_df = get_similar_words(raw_ce, model = model)

similar_sm_df.to_excel(similar_word_df_name + '_sm_ver3.xlsx', index = False, encoding = 'gbk')
similar_wm_df.to_excel(similar_word_df_name + '_wm_ver3.xlsx' ,index = False, encoding = 'gbk')
similar_unce_df.to_excel(similar_word_df_name + '_unce_ver3.xlsx' ,index = False, encoding = 'gbk')
similar_ce_df.to_excel(similar_word_df_name + '_ce_ver3.xlsx' ,index = False, encoding = 'gbk')


raw_ex = pd.read_excel(model_path + '/v0/raw_new_dict_v0.xlsx')
raw_ex_sm = raw_ex['strong_modal']
raw_ex_wm = raw_ex['weak_modal']
raw_ex_un = raw_ex['uncertainty']
raw_ex_ce = raw_ex['certainty']

new_ex_sm = list(raw_ex_sm.drop_duplicates(keep = 'first').dropna())
new_ex_wm = list(raw_ex_wm.drop_duplicates(keep = 'first').dropna())
new_ex_un = list(raw_ex_un.drop_duplicates(keep = 'first').dropna())
new_ex_ce = list(raw_ex_ce.drop_duplicates(keep = 'first').dropna())

new_ex = [new_ex_sm, new_ex_wm, new_ex_un, new_ex_ce]
new_ex = pd.DataFrame(new_ex).transpose()
new_ex.to_excel(model_path + '/v0/new_dictv0.xlsx', index = False, encoding = 'gbk')
