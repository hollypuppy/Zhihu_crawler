
import json
import os, sys
import random
import threading
import time
import keyboard
from html.parser import HTMLParser
import pandas as pd
from lxml import etree

BASE_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

import requests

class MainThread(threading.Thread):
    def __init__(self, url_token):
        super().__init__()
        self.url = "https://www.zhihu.com/answer/" + url_token
        self.comment_url = "https://www.zhihu.com/api//v4/comment_v5/answers/" + url_token + "/root_comment?"

    def run(self):
        global df_answers, df_comments, df_child_comments

        # fetch answers and upvote
        while True:
            try:
                page_text = requests.get(url=self.url, headers=headers, proxies=proxyServer, timeout=10).text
                if '系统监测到您的网络环境存在异常，为保证您的正常访问，请输入验证码进行验证。若频繁出现此页面，可尝试登录后访问知乎' not in page_text:
                    break # quit the loop if successful; If not successful, then try again until succeed
                else:
                    print("频繁")
                    time.sleep(random.randint(1, 3))
                    continue
            except Exception as e:
                print(e)

                time.sleep(random.randint(1, 3))
                continue

        tree = etree.HTML(page_text)
        # get the answers
        if_pro = tree.xpath('//span[@class="KfeCollection-PurchaseBtn-text"]')
        if "最低 0.3 元/天开通会员，查看完整内容" in if_pro:
            answer = "需要会员" # 标记需要会员的答案
        else:
            answer_text = tree.xpath('//div[@class="RichContent RichContent--unescapable"]/span/div/span//text()')
            answer = '\n'.join(answer_text)  # format the answer
        # get the upvote
        try:
            upvote = tree.xpath('//div[@class="ContentItem AnswerItem"]/meta[@itemprop="upvoteCount"]/@content')[0]
        except IndexError:
            upvote = ""
        print("An answer is fetched")
        row = {
            'answer_url': [url_token],
            'answer_content': [answer],
            'upvote': [upvote],
        }
        df_answers = pd.concat([df_answers, pd.DataFrame(row)], axis=0, ignore_index=True)

        # fetch comments and child comments

        while True:
            try:
                comment_text = requests.get(url=self.comment_url, headers=headers, proxies=proxyServer, timeout=10).text
                if '系统监测到您的网络环境存在异常，为保证您的正常访问，请输入验证码进行验证。若频繁出现此页面，可尝试登录后访问知乎' not in comment_text:
                    break
                else:
                    print("频繁")
                    time.sleep(random.randint(1, 3))
                    continue  # quit the loop if successful; If not successful, then try again until succeed
            except:
                time.sleep(random.randint(1, 3))
                continue
        # get comments
        res = json.loads(comment_text)
        try:
            if res['data']:
                for comment in res['data']:
                    answer_url = url_token
                    comment_id = comment['id']
                    comment_author_id = comment['author']['id']
                    comment_author_url = comment['author']['url_token']
                    content = HTMLParser(comment['content']).text()
                    created_time = comment['created_time']
                    row = {
                        'answer_url': [answer_url],
                        'comment_id': [comment_id],
                        'comment_author_id': [comment_author_id],
                        'comment_author_url': [comment_author_url],
                        'content': [content],
                        'created_time': [created_time],
                    }
                    df_comments = pd.concat([df_comments, pd.DataFrame(row)], axis=0, ignore_index=True)
                    # child comments
                    if comment['child_comments']:
                        for child_comment in comment['child_comments']:
                            answer_url = url_token
                            comment_id = comment['id']
                            child_comment_id = child_comment['id']
                            child_comment_author_id = child_comment['author']['id']
                            child_comment_author_url = child_comment['author']['url_token']
                            child_comment_content = HTMLParser(child_comment['content']).text()
                            created_time = child_comment['created_time']
                            child_comment_row = {
                                'answer_url': [answer_url],
                                'comment_id': [comment_id],
                                'child_comment_id': [child_comment_id],
                                'child_comment_author_id': [child_comment_author_id],
                                'child_comment_author_url': [child_comment_author_url],
                                'child_comment_content': [child_comment_content],
                                'created_time': [created_time],
                            }
                            df_child_comments = pd.concat([df_child_comments, pd.DataFrame(child_comment_row)],
                                                          axis=0,
                                                          ignore_index=True)

        except KeyError:
            pass

        print("Comments and child comments of an answer is fetched")

        if self in tasks:
            tasks.remove(self)

def stop():
    global running
    if running:

        running = False
    else:
        running = True

    print("任务终止！------------------")


if __name__ == "__main__":
    running = True
    # proxy host
    proxyHost = "e214.kdltps.com"
    proxyPort = 15818

    # proxy pass
    proxyUser = ""
    proxyPass = ""

    proxyServer = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }
    proxyServer = {"http": proxyServer, "https": proxyServer}
    headers = {
        "Accept-Encoding": "Gzip",
        'Host': 'www.zhihu.com',
        'Connection': 'close',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',

    }
    # store comments and child comments
    if os.path.exists('answers test.csv'):
        df_answers = pd.read_csv('answers test.csv')
    else:
        df_answers = pd.DataFrame(columns=['answer_url', 'answer_content', 'upvote'])

    if os.path.exists('comments test.csv'):
        df_comments = pd.read_csv('comments test.csv')
    else:
        df_comments = pd.DataFrame(
            columns=['answer_url', 'comment_id', 'comment_author_id', 'comment_author_url', 'content', 'created_time'])

    if os.path.exists('child_comments test.csv'):
        df_child_comments = pd.read_csv('child_comments test.csv')

    else:
        df_child_comments = pd.DataFrame(
            columns=['answer_url', 'comment_id', 'child_comment_id', 'child_comment_author_id',
                     'child_comment_author_url', 'child_comment_content', 'created_time'])

    # import data
    df = pd.read_csv('./raw data/small_0-1000.csv')
    df['url_token'] = df['url_token'].apply(str)

    tasks = []
    num_answers = len(df['url_token'])
    taskSum = 60 #
    # Add tasks
    start_time = time.time()
    monitor = threading.Thread(target=lambda: keyboard.add_hotkey('ctrl+1', stop)) 
    monitor.start()
    if not os.path.exists("remember_run_count.txt"):
       with open("remember_run_count.txt", "w") as f:
            f.write("0")
    with open("remember_run_count.txt", "r") as f:
        remember_run_count = f.read()

    for i in range(int(remember_run_count), num_answers):
        while len(tasks) >= taskSum:
            print("线程上限，请等待！", "运行状态：", running)
            print("剩余线程数：" + str(num_answers - i))
            end_time = time.time()
            run_time = end_time - start_time
            h = int(run_time // 3600)
            m = int((run_time - h * 3600) // 60)
            s = int((run_time - h * 3600 - m * 60))
            print("当前耗时：{}时{}分{}秒".format(h, m, s))
            if not running:
                break
            time.sleep(10)
        url_token = df['url_token'][i]
        thread = MainThread(url_token=url_token)
        tasks.append(thread)
        thread.start()
        remember_run_count = i
        if not running:
            break

    running = True
    # output data
    while len(tasks):
        if not running:
            remember_run_count = remember_run_count - len(tasks)
            break

        end_time = time.time()
        run_time = end_time - start_time
        h = int(run_time // 3600)
        m = int((run_time - h * 3600) // 60)
        s = int((run_time - h * 3600 - m * 60))
        print("当前耗时：{}时{}分{}秒".format(h, m, s))
        print("线程未完成，等待中！，剩余线程数：" + str(len(tasks)), "运行状态：", running)
        time.sleep(5)

    sava1 = threading.Thread(
        target=lambda: df_answers.to_csv('answers test.csv', mode='w', index=False, encoding="utf-8-sig"))
    sava2 = threading.Thread(
        target=lambda: df_child_comments.to_csv('child_comments test.csv', mode='w', index=False, encoding="utf-8-sig"))
    sava3 = threading.Thread(
        target=lambda: df_comments.to_csv('comments test.csv', mode='w', index=False, encoding="utf-8-sig"))
    sava1.start()
    sava2.start()
    sava3.start()

    while sava1.is_alive() or sava2.is_alive() or sava3.is_alive():
        time.sleep(5)
        print("保存中！")

    end_time = time.time()
    run_time = end_time - start_time
    h = int(run_time // 3600)
    m = int((run_time - h * 3600) // 60)
    s = int((run_time - h * 3600 - m * 60))
    print("done～耗时：{}时{}分{}秒".format(h, m, s))

    with open("remember_run_count.txt", "w") as f:
        f.write(str(remember_run_count))

    sys.exit(0)

