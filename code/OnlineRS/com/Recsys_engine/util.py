# -*- coding: UTF-8 -*-

import jieba
import jieba.analyse, jieba.posseg

def hasMeaningfulWords(x):
    flag = x.flag
    target_set = ['n','nt', 'nz', # nt--机构团体名, nz=其他专有名
                  'a','ad','an','d',
                  'v','vd','vn','vi','vl']

    if flag in target_set:
        return True
    return False

def preprocess_per_news(news):

        content = jieba.posseg.cut(news)  # 精确模式分词
        content = filter(lambda x: hasMeaningfulWords(x), content)  # 留下名词，形容词，副词
        content = [i.word for i in content]
        content = filter(lambda x: len(x) > 1, content)  # 过滤掉单字
        stopword_file = "/Users/luoyi/Scala/OnlineRS/com/Recsys_engine/data/stop_word.txt"
        stopw = [line.strip().decode('utf-8') for line in open(stopword_file).readlines()]

        parsed = set(content) - set(stopw)
        return ' '.join(parsed)