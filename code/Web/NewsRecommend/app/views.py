# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import jieba, os
import pandas as pd
from django.shortcuts import render
from django.http import Http404, HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import Context
from django.contrib import auth
from models import News, Users, Records, RecommendSet, TNews, ClickRecords, CaixinNews, UserRecords
import re, json, datetime
import simplejson
from django.template.context import RequestContext
from forms import LoginForm
from django.core import serializers
from django.core.serializers.json import DjangoJSONEncoder
from json import dumps, loads, JSONEncoder, JSONDecoder
import sys
import numpy as np

reload(sys)
sys.setdefaultencoding('utf8')

def news(request):
    date = datetime.date.today()
    news = list(CaixinNews.objects.filter(pub_date__range=(datetime.datetime.combine(date, datetime.time.min),
                            datetime.datetime.combine(date, datetime.time.max))))

    for n in news:
        n.content = n.content.replace('?', '')
        n.content = n.content.replace('　　', '\n　　')

        # wordlist_after_jieba = jieba.cut(n.content, cut_all=True)
        # wl_space_split = " ".join(wordlist_after_jieba)
        #
        # font = "/Users/luoyi/Downloads/msyh.ttf"
        #
        # my_wordcloud = WordCloud(font_path=font, background_color='white',
        #                          stopwords=STOPWORDS.copy(), margin=10,
        #                          random_state=1).generate(wl_space_split)
        #
        # plt.imshow(my_wordcloud)
        # plt.axis("off")
        # plt.savefig("/Users/luoyi/Documents/Python/Capstone_Project/"
        #             "Content-Based-News-Recommendation-System-in-Spark/code/Web/NewsRecommend/media/" + n.news_id + ".png"
        #             , bbox_inches='tight')

    if 'name' in request.session:
        username = request.session['name']
    else:
        username = ""

    return render_to_response('main.html', {"news_list": news, 'username': username})

def click_data_collect(request, news_id):

    username = request.session["name"]
    time = datetime.datetime.today()
    news_url = CaixinNews.objects.get(news_id__exact=news_id).url
    x = np.int64(pd.Timestamp(time).value)/1000000000

    # 外键的字段需要这样子初始化
    user_o = Users.objects.get(user_id__exact=username)
    news_o = CaixinNews.objects.get(news_id__exact=news_id)

    # 存入数据库
    r = UserRecords(user=user_o, news=news_o, read_time=time, read_ts=x)
    r.save()

    # 跳转到财新网该新闻页面
    return HttpResponseRedirect(news_url)

def login(request):
    if request.method == 'POST':
        uf = LoginForm(request.POST)
        if uf.is_valid():
            # 获取表单用户密码
            username = uf.cleaned_data['username']
            password = uf.cleaned_data['password']
            # 获取的表单数据与数据库进行比较
            try:
                user = Users.objects.get(user_id__exact=username, passwd__exact=password)
            except Users.DoesNotExist:
                return HttpResponseRedirect('/login/', {'error', True})
            request.session['name'] = username
            return HttpResponseRedirect('/user/')
    else:
        uf = LoginForm()
    return render_to_response('login.html', {'uf': uf, 'error': False, 'username':""})

def logout(request):
    try:
        del request.session['name']
    except KeyError:
        pass
    return HttpResponseRedirect('/login/')

def user_sys(request):

    username = request.session['name']
    records = Records.objects.filter(user_id__exact=username).order_by('read_time')
    readlist = []
    for r in records:
        news = News.objects.get(news_id__exact=r.news_id)
        news.content = news.content.replace('?', '')
        news.content = news.content.replace('　　', '\n　　')

        readlist.append(news)
    readlist = zip(readlist, records)
    return render_to_response("user.html", {"read_list": readlist, "username": username})

def recommend(request):

    username = request.session['name']
    cr = ClickRecords.objects.filter(user_id__exact = username).order_by('read_time')

    cr_timestamp_list = []
    for c in cr:
        ts = pd.Timestamp(c.read_time).value
        cr_timestamp_list.append(ts)

    # recommendnews = []
    #
    # for clickrecord in cr:
    #     time = clickrecord.read_time
    #     recommend_list_this_time = RecommendSet.objects.filter(user_id__exact=username, read_time__exact = time)
    #     recommend_newsobject_this_time = []
    #     for r in recommend_list_this_time:
    #         recommend_newsobject_this_time.append(TNews.objects.get(news_id__exact = r.news_id))
    #     recommendnews.append( recommend_newsobject_this_time)
    #
    # return render_to_response("recommend.html",
    #                           {"recommendSet": recommendnews,
    #                            "username": username, 'clicktimes': cr})
    return render_to_response("recommend.html",
                              {"username": username, 'clicktimes': cr, 'clickts': json.dumps(cr_timestamp_list)})

def recommend_news(request, username, time):
        username = username
        clicktime = time

        recommend_list_this_time = RecommendSet.objects.filter(user_id__exact=username, read_ts__exact=clicktime)
        res = []
        for r in recommend_list_this_time:
            news = TNews.objects.get(news_id__exact = r.news_id)
            news.content = news.content.replace('?', '')
            news.content = news.content.replace('　　', '\n　　')
            res.append(news)
        res = sorted(res, key=lambda x:x.pub_date)
        return render_to_response("recommend_newsList.html",
                                  {'username':username,'date': recommend_list_this_time.first().read_time, 'News': res})