# -*- coding: UTF-8 -*-
import jieba
import io
import jieba.analyse

def writeInFile(content, filename):
    file = io.open(filename, 'w', encoding='utf-8')
    for news in content:
        file.write(news + "\n")
    file.close()

def preprocess(news):
    content = jieba.cut(news, cut_all=False)  # 精确模式分词
    content = filter(lambda x: len(x) > 1, content)  # 过滤掉长度为1 的词（排除干扰）
    content = filter(lambda x: not x.isdigit(), content)  # 过滤掉长度为1 的词（排除干扰）
    stopw = [line.strip().decode('utf-8') for line in open('/Users/luoyi/Documents/Python/RecommendationSystem/data/stopwords.txt').readlines()]

    parsed = set(content) - set(stopw)
    return ' '.join(parsed)