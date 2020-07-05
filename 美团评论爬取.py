

from selenium import webdriver
import time
from bs4 import BeautifulSoup
from pymysql import connect
from selenium.webdriver.common.keys import Keys
import threading

database_name = 'meituan'

def soup_meituan_comment(html,sql):
    soup = BeautifulSoup(html,'lxml')
    # 获取店铺id
    shop_id_url = soup.select('.poi-base-area > .link')[0]['href']
    shop_id = shop_id_url.split('/')[-2]
    shop_name = soup.select('.poi-base-area > .link')[0].text
    items = soup.select('.comment-item')
    for item in items:
        try:
            print(item)
            usr_image = item.select('.user-photo>.image')[0]['src']
            usr_name = item.select('.user-name')[0].text
            try:
                usr_level = item.select('.user-level')[0]['src']
            except:
                usr_level = 0
            deal_name = item.select('.deal-name')[0].a['href']
            comment_time = item.select('.comment-date')[0].text
            comment_detail_link = item.select('.comment-date')[0]['href']
            comment_score = item.select('.rate-stars-light')[0]['style']
            comment_detail = item.select('.user-comment>span')[0].text
            comment_photo_temp_list = item.select('.comment-photo-box')
            comment_photo = ''
            if len(comment_photo_temp_list) >0:
                for item_photo in comment_photo_temp_list:
                    comment_photo = comment_photo +item_photo.img['src']+','
            comment_op = item.select('.op-txt')[0].text
            comment_reply = item.select('.reply-txt')[0].text
            # 当前时间
            creat_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            sql_url = "INSERT  INTO meituan_comment_new VALUES(null ,'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(shop_id,shop_name,usr_image,usr_name,usr_level,deal_name,comment_time,comment_detail_link,comment_score,comment_detail,comment_photo,comment_op,comment_reply,creat_time)
            sql.insert_comment_info(sql_url)

        except:
            print('下面是报错的内容\n\n\n')
            print(item)
            print('\n\n\n')
            continue

def soup_meituan_shop(html,sql):
    soup = BeautifulSoup(html, 'lxml')
    # 获取店铺id
    shop_id_url = soup.select('.poi-base-area > .link')[0]['href']
    shop_id = shop_id_url.split('/')[-2]
    shop_name = soup.select('.poi-base-area > .link')[0].text
    shop_score = soup.select('.score-num')[0].text
    shop_comment = soup.select('.score .text')[0].text
    # 快速提取数字
    shop_comment_count = ''.join(filter(str.isdigit, shop_comment))
    shop_cognition_div = soup.select('.labels-box')[0]
    shop_cognition = []
    print(shop_cognition_div)
    for item in shop_cognition_div:
        shop_cognition.append(item.text)
    #     列表转文字
    shop_cognition = ','.join(shop_cognition)

    # 当前时间
    creat_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    sql_url = "INSERT  INTO shop_new VALUES(null ,'{}','{}','{}','{}','{}','{}','{}','{}')".format(
        shop_id, shop_name, shop_id_url, shop_score, shop_comment, shop_comment_count, shop_cognition, creat_time)
    sql.insert_comment_info(sql_url)

class meituan_sql():
    def __init__(self):
        '''
        特别提醒，如果第一次运行报错的话，要在终端上面启动下mysql，可能是mysql服务未启动：mysql -uroot -p
        '''
        # 1、连接数据库
        self.conn = connect(host='localhost', port=3306,  user='root', password='wangying',charset='utf8')
        # 获得Cursor对象
        self.cs1 = self.conn.cursor()
        self.creat_database(database_name)
         # 连接taobao数据库
        sql_url = 'use '+database_name
        self.cs1.execute(sql_url)
        # 创建表
        self.creat_comment_list_db()
        self.creat_shop_db()
        print("数据库创建完毕")

    def execute_sql(self,sql):
        self.cs1.execute(sql)
        temp_list = list()
        for temp in self.cs1.fetchall():
            temp_list.append(temp)
            print(temp)

        return temp_list

    def creat_database(self,name):
        sql_url = 'CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci'.format(name)
        self.cs1.execute(sql_url)

    def creat_comment_list_db(self):
        '''创建表格，用于储存淘宝列表信息'''
        self.cs1.execute('''CREATE TABLE  if not EXISTS meituan_comment_new (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '评论id',
          `shop_id` int NOT NULL ,
          `shop_name` varchar(100) NOT NULL ,
          `usr_image` varchar(500) NOT NULL ,
          `usr_name` varchar(255) NOT NULL,
          `usr_level` varchar(500) NOT NULL,
          `deal_name` varchar(255) NOT NULL,
          `comment_time` varchar(128) NOT NULL,
          `comment_detail_link` varchar(255) DEFAULT NULL,
          `comment_score` varchar(128) NOT NULL,
          `comment_detail` varchar(1000) NOT NULL,
          `comment_photo` varchar(2000) NOT NULL,
          `comment_op` varchar(100) DEFAULT NULL,
          `comment_reply` varchar(100) DEFAULT NULL,
          `creatTime` timestamp NOT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;''')


    def creat_shop_db(self):
        '''创建表格，用于储存淘宝列表信息'''
        self.cs1.execute('''CREATE TABLE  if not EXISTS shop_new (
          `id` int NOT NULL AUTO_INCREMENT COMMENT 'shop 自增 id',
          `shop_id` int NOT NULL ,
          `shop_name` varchar(100) NOT NULL ,
          `shop_url` varchar(255) NOT NULL ,
          `shop_score` varchar(255) NOT NULL,
          `shop_comment` varchar(255) NOT NULL,
          `shop_comment_count` varchar(255) NOT NULL,
          `shop_cognition` varchar(255) DEFAULT NULL,
          `creatTime` timestamp NOT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;''')


    def insert_comment_info(self,sql):
        print("插入数据"+sql)
        self.cs1.execute(sql)
        self.conn.commit()

def more_thread_webdriver(item):
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.implicitly_wait(5)

    url = "https://www.meituan.com/feedback/%s/" % item
    driver.get(url)
    time.sleep(0.5)
    driver.find_element_by_xpath('//*[@id="react"]/div/div/div[2]/div[1]/div/div[2]/div[2]/div/span[1]').click()
    time.sleep(0.5)
    soup_meituan_shop(driver.page_source, mt_sql)  # 抓取店铺信息

    i = 1
    try:
        while True:
            soup_meituan_comment(driver.page_source, mt_sql)
            driver.find_element_by_class_name('icon-btn_right').click()
            i = i + 1
            print("该爬取页数" + str(i))
            time.sleep(1)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("报错" * 50)


if __name__ == "__main__":

    mt_sql =meituan_sql()
    shop_list = ['118865503','63319969','195958554','184614863','1479205367','2417425','1069571113','190457441','98514636','5987338']

    for item in shop_list:
        th1 = threading.Thread(target=more_thread_webdriver,
                               args=(item,))
        th1.start()





