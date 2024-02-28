#! usr/bin/env python
# writer: yueji0j1anke

import requests
import config
import colorama
import datetime
import pandas as pd
from time import sleep
import os
import threading

# 填入爬取评论目标id
pins = ['1459060852','844773892','1075120156']

# 炫酷
def title():
    print("                                        ")
    print(colorama.Fore.BLUE + "|    | \ _ / | \\ ___ /(/  \\_ __ _| |")
    print(colorama.Fore.BLUE + "|____|__\_/__|  \\ __/( /__\\ _    | |")
    print(colorama.Fore.BLUE + "|    |__/ \__|   \\ /(  /  \\      | |   ")
    print(colorama.Fore.BLUE + "|    | /   \ |    |(___/_ \\  spider_ZhiHu_|\\_\\")
    print(colorama.Fore.YELLOW + "                                 writer: yuejinjianke")


# 获取数据
def get_data(pin,file_path):

    comment_num = 0
    offset = 0

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Cookie': config.cookie,
        'x-zse-93': config.x_zse_93,
        'x-zse-96': config.x_zse_96
    }
    nickname_list = []
    gender_list = []
    place_list = []
    user_id_list = []
    comment_time_list = []
    content_list = []
    content_level_list = []
    comment_num = 0

    while True:
      if comment_num == 0:
          url = 'https://www.zhihu.com/api/v4/answers/{}/root_comments?order=normal&limit=20&offset=0&status=open'.format(pin)
          offset = offset + 20
      else:
          url = 'https://www.zhihu.com/api/v4/answers/{}/root_comments?order=normal&limit=20&offset={}&status=open'.format(pin,offset)
          offset = offset + 20
          print(url)

      try:
          response= requests.get(url=url,headers=headers)
          if response.status_code == 403:
             print(colorama.Fore.RED + '[error] 请更换两个x-zse参数，进行下一次爬取')
          else:
             json_text = response.json()
             total = json_text['common_counts']
             print(colorama.Fore.GREEN + '[info] 一共有 {} 条评论'.format(total))
             max_page = int(total / 20)
             print(colorama.Fore.GREEN + '[info] 翻页总次数为: {}'.format(max_page))

             comments = json_text['data']
             for comment in comments:
                 list_append_1(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list,comment)
                 comment_num += 1
             # print(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list)
                 if int(comment['child_comment_count']):
                    for child_comment in comment['child_comments']:
                        child_num = 0
                        print(colorama.Fore.GREEN + '[info] 正在爬取第{}条子评论'.format(child_num + 1))
                        child_num += 1
                        list_append_2(nickname_list, gender_list, place_list, user_id_list, comment_time_list, content_list,content_level_list, child_comment)
                        comment_num += 1

             if  offset >= int(total):
                 data_save(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list,file_path)
                 print(colorama.Fore.GREEN + '[info] id为 {} 的评论区爬取完成'.format(pin))
                 break


      except Exception as e:
        print(colorama.Fore.RED + '[error] 出现故障：{}'.format(e))
        break

def list_append_1(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list,comment):
    nickname_list.append(comment['author']['member']['name'])
    gender_list.append(tran_gender(int((comment['author']['member']['gender']))))
    place_list.append(comment['address_text'].replace("IP 属地",""))
    user_id_list.append(comment['author']['member']['id'])
    content_list.append(comment['content'])
    content_level_list.append("父级评论")
    comment_time_list.append(datetime.datetime.fromtimestamp(int(comment['created_time'])).strftime("%Y-%m-%d %H:%M:%S"))

def list_append_2(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list,comment):
    nickname_list.append(comment['author']['member']['name'])
    gender_list.append(tran_gender(int((comment['author']['member']['gender']))))
    place_list.append(comment['address_text'].replace("IP 属地",""))
    user_id_list.append(comment['author']['member']['id'])
    content_list.append(comment['content'])
    content_level_list.append("子级评论")
    comment_time_list.append(datetime.datetime.fromtimestamp(int(comment['created_time'])).strftime("%Y-%m-%d %H:%M:%S"))

def tran_gender(gender):
    if gender == 1:
        return '男'
    elif gender == 0:
        return '女'
    else:  # -1
        return '未知'

def get_offset_value(url):
    params_part = url.split("?")[1]
    for param in params_part.split("&"):
        key, value = param.split("=")
        if key == "offset":
            return value

def data_save(nickname_list,gender_list,place_list,user_id_list,comment_time_list,content_list,content_level_list,file_path):
    df = pd.DataFrame(
        {
            '评论者昵称': nickname_list,
            '评论者id': user_id_list,
            '性别': gender_list,
            '地区': place_list,
            '评论内容': content_list,
            '评论时间': comment_time_list,
            '评论等级': content_level_list,
        }
    )
    if os.path.exists(file_path):
        header = False
    else:
        header = True
    df.to_csv(file_path, mode="a+", header=header, index=False, encoding='utf_8_sig')

# 多线程
def multi_thread(pins):
    threads = []
    for pin in pins:
        threads.append(
            threading.Thread(target=get_data,args=(pin,f"{pin}.csv"))
        )

    for task in threads:
        task.start()

    for task in threads:
        task.join()


if __name__ == '__main__':
    title()
    print(colorama.Fore.GREEN + '[info] 开始爬取')
    multi_thread(pins)