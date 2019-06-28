

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
    num = soup.select('.huifont')[0].text
    num = re.match('\d*/(\d*)', num).group(1)
    return int(num)

#  obtain all the url
def getAllurl(base_url, end_date, NumOfPage, endPage=None):
    currentDate = end_date+1
    currentPage = 1
    ListOfUrl = []

    while currentDate > end_date and currentPage <= NumOfPage:
        url = base_url + str(currentPage)

        html = get_page(url)
        soup = BeautifulSoup(html, 'lxml')
        onePage = soup.findAll(class_='ewb-com-item')
        for item in onePage:
            if currentDate < end_date:
                break
            Url = item.a['href']
            currentDate= int(item.span.text.replace('-',''))
            
            if currentDate > end_date:
                realUrl = host_url+Url
                ListOfUrl.append(realUrl)
        currentPage += 1
    return ListOfUrl





def main():
    print('==================start==================')
    start = time.process_time()

    datetime_now = datetime.datetime.now()
    tmp = datetime_now - relativedelta(months=checkInterval)
    check_date=int(tmp.strftime('%Y%m%d'))
    
    if not dbtool.DBexist(database_name ):
        print("Initialing Databse....")
        dbtool.createDB(table_name,database_name)
        end_date=initial_end_date
    else:
        if dbtool.tableExist(table_name,database_name):
            if dbtool.getItemNum(table_name,database_name)>0 :
                 print("Updating Databse....")
                 end_date=check_date
        else: 
            print("Initialing Table....")
            dbtool.createDB(table_name,database_name)
            end_date=initial_end_date
        
       

    # obtain number of pages  
    
    num = numberOfPage(base_url, headers)

    # obtain all the url after date 20190100
    listurl = getAllurl(base_url, end_date, num)

    # get data ,extract and store in db
    # obatin data form latest date to  end_date, use oppsit oder to store data
    for realUrl in listurl[::-1]:
        print('obtaining : ', realUrl)
        html = get_page(realUrl)
        oneLandInfo = prasePage(html)
        dbtool.insertData(oneLandInfo, table_name)

    total = (time.process_time() - start)
    print("Time used:", total)
    print('==================END==================')


if __name__ == "__main__":
   
    database_name='LandInfo'
    table_name ='HeFei'
    initial_end_date = 20180100
    checkInterval = 3 # months 
    host_url = 'http://ggzy.hefei.gov.cn'
    base_url = host_url+'/hftd/showinfo/moreinfo.aspx?CategoryNum=001003008&Paging='
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
    }
    hour='9'
    minute='59'

    main()
    # sched = BlockingScheduler()
    # sched.add_job(main, 'cron', hour,minute) 
    # sched.start()