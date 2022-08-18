'''
Expand the translated LM dict (strong modal, weak modal, uncertainty, and certain)
Steps:
    1. read the available reports one by one, aggregate all processed reports
    into a single file, where each line is a sentence
    2. import files, remove stop words
    3. train word2vec one the single file derived in step 2, and find the synonyms
    for the words in our preliminary Chinese LM dicts

This file is for the step 1
'''
import os
import jieba
from joblib import Parallel, delayed
from itertools import chain
import time

def load_stopwords(stopwords_path: str):
    with open(stopwords_path, 'r') as f:
        stop_words = f.readlines()
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

class process_code:
    def __init__(self, code_path: str, jobs: int):
        self.jobs = jobs
        self.path_list = [code_path + '/' + single_path for single_path in os.listdir(code_path)]
    
    def process_single_file(self, file_path:str):
        '''
        This func reads a single report file in txt format, return the sentences
        in the text as lists in list
        
        Parameters
        ----------
        file_path : str
            the path of a report file in txt format

        Returns
        -------
        output: list
            a list of sentences in a text, every sentence a list of words in that
            sentence

        '''
        with open(file_path, 'r', encoding = 'utf-8') as f:
            content = f.read()
        sentences = cut_sentence(content)
        
        output = []
        symbols = ['。','！','？','，','“','”','‘','’','%', '.','、','（','）','(',')','-', '的','地','；','：'] + [str(number) for number in list(range(10))]


        for sentence in sentences:
            temp_sent = ' '.join(sentence)
            for symbol in symbols:
                temp_sent = temp_sent.replace(symbol, '')
            new_sent = [word for word in temp_sent.split(' ') if word != '']
                        
            output.append(new_sent)
            
        return output

    def process_all_files(self):
        path_list = self.path_list
        jobs = self.jobs
        
        num_per_job = int(len(path_list) / jobs)
        path_list_cut = []
        for i in range(jobs):
            if i != jobs - 1:
                path_list_cut.append(path_list[i * num_per_job: (i + 1) * num_per_job])
            else:
                path_list_cut.append(path_list[i * num_per_job:])
                
        def multi_run(sub_path_list):
            sub_all_sents = []
            for path in sub_path_list:
                sub_all_sents += self.process_single_file(file_path = path)
            return sub_all_sents
            
        output = Parallel(n_jobs=jobs, verbose=1)(delayed(multi_run)(sub_list) for sub_list in path_list_cut)
        return output
    
if __name__ == "__main__":
    start_time = time.time()
    # file_path = 'C:/Users/niccolo/Desktop/QLFtask/eastmoney/ExtractResults'
    file_path = '/home/users/michaelfan/data/ConfCall/eastmoney/ExtractResults'
    code_path_list = [file_path + '/' + code_path for code_path in os.listdir(file_path)]
    results = []
    for code_path in code_path_list:
        process_one_code = process_code(code_path, 16)
        results += process_one_code.process_all_files()
        
    all_sents = list(chain(*results))
    single_list = [' '.join(single_sent) for single_sent in all_sents]
    single_file = '\n'.join(single_list)
    
    with open('./all_reports_sentences.txt', 'w', encoding = 'utf-8') as f:
        f.write(single_file)
    print("--- %s seconds ---" % (time.time() - start_time))
        
        
        

