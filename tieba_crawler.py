import requests
from pyquery import PyQuery
import queue
import threading
import sys
from functools import partial
import time

eprint = partial(print, file=sys.stderr)

def url_get(url, encoding = 'utf8', retries = 3):
    # print('GET ' + url)
    header = dict()
    header['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    header['Accept-Encoding'] = 'gzip,deflate,sdch'
    header['Accept-Language'] = 'en-US,en;q=0.8'
    header['Connection'] = 'keep-alive'
    header['DNT'] = '1'
    header['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36'
    #header['User-Agent'] = 'Mozilla/12.0 (compatible; MSIE 8.0; Windows NT)'
    try:
        return requests.get(url, timeout = 20, headers = header).content.decode(encoding)
    except Exception as e:
        eprint('GET {} failed, retries = {}'.format(url, retries))
        time.sleep(5)
        if retries <= 0:
            raise e
        return url_get(url, retries = retries - 1)


def get_title(page, page_html):
    d = PyQuery(page_html)
    eprint('Total {} titles'.format(len(d('a.j_th_tit'))))
    for tit_a in d('a.j_th_tit'):
        print('{}\t{}\t{}'.format(page, tit_a.text, tit_a.get('href')))

def get_reply(url, title):
    page_html = url_get('https://tieba.baidu.com{}'.format(url))
    d = PyQuery(page_html)
    replies = []
    for post_div in d('div.d_post_content'):
        reply_text = post_div.text.strip()
        if reply_text != '':
            replies.append(post_div.text.replace('\n', ' '))
    text = '#N#'.join(replies)
    text = ' '.join(text.split())
    print('{}\t{}\t{}'.format(url, title, text))

def get_page_thread(queue):
    while True:
        item = queue.get()
        title = item['title']
        url = item['url']
        try:
            get_reply(url, title)
        except:
            pass

def crawl_titles():
    for i in range(0, 2000):
        try:
            #url = 'http://tieba.baidu.com/f?kw=%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8&ie=utf-8&pn={}'.format(i * 50)
            url = 'https://tieba.baidu.com/f?kw=%E9%98%B3%E5%85%89%E4%BF%9D%E9%99%A9&ie=utf-8&pn={}'.format(i * 50)
            page_html = url_get(url)
            page_html = page_html.replace('<!--', '')
            page_html = page_html.replace('-->', '')
            eprint('Get: ' + url)
            get_title(i, page_html)
        except:
            pass

def crawl_page():
    q = queue.Queue(256)
    ts = [threading.Thread(target = get_page_thread, args = (q, )) for _ in range(10)]
    for t in ts:
        t.start()

    with open('titie_url.tsv') as fd:
        for line in fd:
            try:
                title, url = line.strip().split()
                q.put(dict(url = url, title = title))
            except:
                pass
        sys.exit(0)

crawl_titles()