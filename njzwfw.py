'''
- 从下面网址中获取土地成交数据，需包含出让日期，地块编号，地块名称、土地面积、容积率、竞得人等相关信息：
    http://ggzy.njzwfw.gov.cn/njweb/tdkc/072002/landmineral2.html

    1. 数据库采用elasticsearch 或 mysql，每个城市的结果保存在自己表里面。
    2. 程序要能提取公共模块。
    3. 需要有断点重连功能：网络中断后可以自动重连获取数据
    4. 能够抓取2019年1月1日至今的所有数据
    5. 需要有每日更新功能：每天主动去更新
    6. 需要有每日数据修正功能：获取一定时间区间内（如三个月）的数据和现有数据对比，查看是否数据有更改
           
    7. 要能够翻页，以确保数据抓取完整。

    思路： 
    - 数据库操作，网页内容抓取（参数为url）可以作为公共类 
    - 每个网站： 1. 获取所有要采集的url 2. 采集清洗 存储 
      - 第一次： 1. 根据所要求的日期(2019年1月1日至今) 获取所有url  2. 采集清洗存储
      - 每日定时： 【要求每日主动更新+三个月内数据修正】 
            初始思路： 主动更新：每日对比数据库最后一条与网站最新一条（根据关键字地块编号,不能根据日期，因为同一天可能有多条） 
                  如果不一致， 从该条开始向前对比 地块编号 找到第一条更新，确定日期 获取所有url开始更新。
                  数据修正： 日期-三个月 ， 获取到该日期的url， 采集，逐一对比数据库，更新
             fianl： 每日向后采集一定时间区间内（如三个月），像数据库提交。 数据库设置 地块编号为主键
                    如果重复，数据库自动更新不同之处。 
'''

import requests
from bs4 import BeautifulSoup
import re
import dbtool
import time
import datetime
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# obtain one page content
def get_page(url, headers=None):
    try:
        r = requests.get(url, headers=headers,timeout=2)
        r.raise_for_status() # let this function throw HTTPError  error 
        return r.text
    except requests.exceptions.RequestException as e:
        print(e.args)
        print('网络连接错误，重新连接....')
        time.sleep(3)
        return get_page(url, headers)





# extract data and convert result as dict
def prasePage(html):
    soup = BeautifulSoup(html, 'lxml')
    # 出让日期，地块编号，地块名称、土地面积、容积率、竞得人
    td = soup.find_all(name='td')
    infoDict = {
        'name': soup.find(name='h1').text.strip('成交信息'),
        'FloorOrder': td[1].text.strip(),
        'address': td[3].text.strip(),
        'FARatio': td[5].text.strip(),
        'size': td[11].text.strip(),
        'soldDate': td[17].text.strip(),
        'price': td[19].text.strip(),
        'owner': td[21].text.strip()
    }

    return infoDict


# obtain total number of page
def numberOfPage(url, headers=None):
    if headers == None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
            # 'X-Requested-With': 'XMLHttpRequest' #  removeable
        }
    html = get_page(url, headers)
    soup = BeautifulSoup(html, 'lxml')
    num = soup.select('#index072002')[0].text
    num = re.match('\d*/(\d*)', num).group(1)
    return int(num)

#  obtain all the url
def getAllurl(base_url, end_date, NumOfPage, endPage=None):
    currentDate = end_date+1
    currentPage = 1
    ListOfUrl = []

    while currentDate > end_date and currentPage <= NumOfPage:
        url = ''
        if currentPage == 1:
            url = base_url + 'moreinfotdkc.html'
        else:
            url = base_url + str(currentPage)+'.html'

        html = get_page(url)
        soup = BeautifulSoup(html, 'lxml')
        onePage = soup.findAll(class_='ewb-info-item2')
        for item in onePage:
            if currentDate < end_date:
                break
            tmpUrl = item['onclick']
            r = re.search('072002/(\d{8})/(.*)\'', tmpUrl)
            currentDate = int(r.group(1))
            if currentDate > end_date:
                realUrl = base_url+str(currentDate)+'/'+r.group(2)
                ListOfUrl.append(realUrl)
        currentPage += 1
    return ListOfUrl



#  removeable 没用到 
# def getLatestItemFromWeb(base_url):
#     html = get_page(base_url+'moreinfotdkc.html')
#     soup = BeautifulSoup(html, 'lxml')
#     item = soup.find(class_='ewb-info-item2')

#     tmpUrl = item['onclick']
#     r = re.search('072002/(\d{8}/.*)\'', tmpUrl)
#     itemURL = base_url+r.group(1)
#     html2 = get_page(itemURL)
#     return prasePage(html2)


def main():
    print('==================start==================')
    start = time.process_time()

    datetime_now = datetime.datetime.now()
    tmp = datetime_now - relativedelta(months=checkInterval)
    check_date=int(tmp.strftime('%Y%m%d'))

    if not dbtool.DBexist(database_name ):
        print("Initialing Databse....")
        dbtool.createDB(table_name)
        end_date=initial_end_date
    else:
        print("Updating Databse....")
        end_date=check_date

    # obtain number of pages  
    numUrl = base_url+'moreinfotdkc.html'
    num = numberOfPage(numUrl, headers)

    # obtain all the url after date 20190100
    listurl = getAllurl(base_url, end_date, num)

    # get data ,extract and store in db
    # obatin data form latest date to  end_date, use oppsit oder to store data
    for realUrl in listurl[::-1]:
        print('obtaining : ', realUrl)
        html = get_page(realUrl)
        oneLandInfo = prasePage(html)
        dbtool.insertData(oneLandInfo, 'NanJing')

    total = (time.process_time() - start)
    print("Time used:", total)
    print('==================END==================')


if __name__ == "__main__":
   
    database_name='LandInfo'
    table_name ='NanJing'
    initial_end_date = 20190100
    checkInterval = 3 # months 
    base_url = 'http://ggzy.njzwfw.gov.cn/njweb/tdkc/072002/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
    }
    

    main()
    sched = BlockingScheduler()
    sched.add_job(main, 'cron', hour='9',minute='59') 
    sched.start()