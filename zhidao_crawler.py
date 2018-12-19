import requests
from pyquery import PyQuery
import queue
import threading
import sys
from functools import partial
from pymongo import MongoClient
from urllib.parse import urlparse
import time

client = MongoClient()
db = client.zhidao
eprint = partial(print, file=sys.stderr)

STATUS_QUEUE = 'queue'
STATUS_ERROR = 'error'
STATUS_ONGOING = 'ongoing'
STATUS_SUCCESS = 'success'

def url_get(url, encoding = 'gbk'):
    print('GET ' + url)
    header = dict()
    header['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    header['Accept-Encoding'] = 'gzip,deflate,sdch'
    header['Accept-Language'] = 'en-US,en;q=0.8'
    header['Connection'] = 'keep-alive'
    header['DNT'] = '1'
    header['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36'
    #header['User-Agent'] = 'Mozilla/12.0 (compatible; MSIE 8.0; Windows NT)'
    return requests.get(url, timeout = 20, headers = header).content.decode(encoding)

def tn(text):
    text = text.replace('展开全部', '')
    text = text.replace('\n', '#N#')
    text = ' '.join(text.split())
    return text

def norm_url(url):
    return urlparse(url).path

def save_page(page):
    try:
        db.pages.insert(page)
    except Exception as e:
        print(e)

def push_url(url):
    try:
        if db.urls.find_one({'_id': url}) == None:
            db.urls.insert({'_id': url, 'status': STATUS_QUEUE})
    except Exception as e:
        print(e)

def update_status(url, status):
    try:
        db.urls.update_one({'_id': url}, {'$set': {'status': status}}, upsert = True)
    except Exception as e:
        print(e)

def page_exists(url):
    return db.pages.find_one({'_id': url}) != None

def parse_page(page_html):
    d = PyQuery(page_html)
    ask_title = d('.ask-title').text()
    ask_con = d('.conReal').text()
    best_text = tn(d('.best-text').text())
    answers = [best_text]
    for answer in d('.answer-text'):
        answers.append(tn(d(answer).text()))
    related = []
    for relitem in d('.related-link'):
        title = d(relitem).find('.related-restrict-title').text()
        url = norm_url(relitem.get('href'))
        related.append((url, title))
    return dict(
        title = ask_title,
        context = ask_con,
        answers = answers,
        related = related)

def keywords_match(keywords, title):
    for keyword in keywords:
        if title.lower().find(keyword) < 0:
            return False
    return True

def extract_url_by_query(page):
    keywords = set(query.split())
    urls = []
    for url, title in page['related']:
        if keywords_match(keywords, title):
            urls.append(url)

    print('Get {} urls'.format(len(urls)))
    return urls

def crawler_thread(queue):
    while True:
        try:
            url = queue.get()
            if page_exists(url):
                print('url: exists' + url)
                update_status(url, STATUS_SUCCESS)
                continue
            update_status(url, STATUS_ONGOING)
            abs_url = base_url + url
            page_html = url_get(abs_url)
            page = parse_page(page_html)
            page['_id'] = norm_url(url)
            page['query'] = query
            save_page(page)
            next_urls = extract_url_by_query(page)
            for next_url in next_urls:
                # print(next_url)
                push_url(next_url)
            update_status(url, STATUS_SUCCESS)
        except Exception:
            update_status(url, STATUS_ERROR)

def find_related_urls():
    keywords = set(query.split())
    for page in db.pages.find():
        for url, title in page['related']:
            if keywords_match(keywords, title):
                push_url(url)

base_url = 'https://zhidao.baidu.com'
#start_url = '/question/1836179785453660940.html'
start_url = '/question/460722126824700365.html'
queries = ['保险 理赔']

q = queue.Queue(256)
ts = [threading.Thread(target = crawler_thread, args = (q, )) for _ in range(10)]
for t in ts:
    t.start()
q.put(start_url)

for query in queries:
    find_related_urls()
    while True:
        if db.urls.find_one({'status': STATUS_QUEUE}) == None:
            break
        for url in db.urls.find({'status': STATUS_QUEUE}):
            q.put(url['_id'])
        time.sleep(10)
