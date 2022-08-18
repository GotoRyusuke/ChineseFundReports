# -*- coding: utf-8 -*-
'''
Train Word2Vec using a iterator instead of a txt file
'''
import os
from gensim.models import word2vec
import pandas as pd
import jieba
import openpyxl
import time 

def load_stopwords(stopwords_path: str):
    stop_words = [str(symbol) for symbol in pd.read_excel(stopwords_path)['symbol']]
    return stop_words

def cut_sentence(content: str):
    words = [word for word in jieba.cut(content)]
    eos = ['。','！','？']
    
    word_count = 0
    eos_idx = 0
    
    sentences = []
    for word in words:
        if word in eos:
            sentence = words[eos_idx:word_count]
            sentences.append(sentence)
            eos_idx = word_count
        word_count += 1
    return sentences
 
class MySentences(object):
    def __init__(self, dirname, stopwords_path):
        self.dirname = dirname
        self.stop_words = load_stopwords(stopwords_path)
     
    def __iter__(self):
        for code in os.listdir(self.dirname):
            code_path = self.dirname + '/' + code
            for file in os.listdir(code_path):
                full_path = code_path + '/' + file
                with open(full_path, 'r', encoding = 'utf-8') as file:
                    content = file.read()
                    
                sentences = cut_sentence(content)
                for sentence in sentences:
                    temp_sent = ' '.join(sentence)
                    for symbol in self.stop_words:
                        temp_sent = temp_sent.replace(symbol, '')
                    new_sent = [word for word in temp_sent.strip().split(' ') if word != '']
                    yield new_sent
                            
      
model_path = "C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec/model"
content_path = "C:/Users/niccolo/Desktop/QLFtask/eastmoney/FullTexts" 
model_name = model_path + "/120vec_5win.model"
stopwords_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec/my_stop_words.xlsx'
jieba_add_word_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/word2vec/jieba.xlsx'

# model_path = '/home/users/michaelfan/data/ConfCall/eastmoney/model'
# content_path = '/home/users/michaelfan/data/ConfCall/eastmoney/model/FullTexts'
# model_name = model_path + '/120vec_5win.model'
# stopwords_path = '/home/users/michaelfan/data/ConfCall/eastmoney/my_stop_words.xlsx'
# jieba_add_word_path = ''

jieba_add_word = pd.read_excel(jieba_add_word_path)['word']
for word in jieba_add_word:
    jieba.add_word(word)

os.makedirs(model_path, exist_ok = True)

# initialise the iterable
sentences = MySentences(content_path, stopwords_path)

# hyperparameters
sg = 0  
hs = 0  
vector_size = 120 
window_size = 10  
min_count = 30  
workers = 4
epochs = 25  
batch_words = 100000 

start_time = time.time()
model = word2vec.Word2Vec(
    sentences,
    min_count=min_count,
    vector_size=vector_size,
    workers=workers,
    epochs=epochs,
    window=window_size,
    sg=sg,
    hs=hs,
    batch_words=batch_words
)
print("--- %s seconds ---" % (time.time() - start_time))

# model.save(model_name)
