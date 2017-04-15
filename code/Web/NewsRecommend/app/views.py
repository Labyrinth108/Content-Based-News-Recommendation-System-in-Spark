# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.shortcuts import render
from django.http import Http404, HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import Context
from django.contrib import auth
from models import News, Users, Records, RecommendSet, TNews
import re
from django.template.context import RequestContext
from forms import LoginForm

def news(request):
    news = News.objects.all()[:10]
    for n in news:
        n.content = n.content.replace('&nbsp', '<br>')

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
    return render_to_response('login.html', {'uf': uf, 'error': False})

def user_sys(request):

    username = request.session['name']
    records = Records.objects.filter(user_id__exact=username)
    readlist = []
    for r in records:
        readlist.append(News.objects.get(news_id__exact=r.news_id))
    return render_to_response("user.html", {"read_list": readlist,  "username": username})

def recommend(request):

    username = request.session['name']

    rs = RecommendSet.objects.filter(user_id__exact = username, read_time__exact= '2014-03-10 04:10:00.000000')
    recommendnews = []
    for r in rs:
        recommendnews.append(TNews.objects.get(news_id__exact = r.news_id))
    return render_to_response("recommend.html", {"recommendSet": recommendnews, "username": username})