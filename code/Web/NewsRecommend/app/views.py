# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.shortcuts import render
from django.http import Http404, HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import Context
from django.contrib import auth
from models import News, Users, Records, RecommendSet, TNews, ClickRecords
import re, json
import simplejson
from django.template.context import RequestContext
from forms import LoginForm
from django.core import serializers
from django.core.serializers.json import DjangoJSONEncoder
from json import dumps, loads, JSONEncoder, JSONDecoder
import pickle
import sys
import numpy as np
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf8')

def news(request):
    news = list(News.objects.all()[:5])
    for n in news:
        n.content = n.content.replace('?', '')
        n.content = n.content.replace('　　', '\n　　')
    if 'name' in request.session:
        username = request.session['name']
    else:
        username = ""
    return render_to_response('main.html', {"news_list": news, 'username': username})

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
    records = Records.objects.filter(user_id__exact=username)
    readlist = []
    for r in records:
        news = News.objects.get(news_id__exact=r.news_id)
        news.content = news.content.replace('?', '')
        news.content = news.content.replace('　　', '\n　　')

        readlist.append(news)
    return render_to_response("user.html", {"read_list": readlist,  "username": username})

def recommend(request):

    username = request.session['name']
    cr = ClickRecords.objects.filter(user_id__exact = username)
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
        return render_to_response("recommend_newsList.html",
                                  {'username':username,'date': recommend_list_this_time.first().read_time, 'News': res})