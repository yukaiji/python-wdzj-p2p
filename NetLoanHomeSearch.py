from bs4 import BeautifulSoup
import requests
import pymysql
import datetime
import logging
import warnings


# 获取数据库连接
def getMysqlConnect():
    try:
        connect = pymysql.connect(host='localhost', port=3362, user='root',
                                  passwd='123456', db='py_test')
        return connect
    except Exception as err:
        logging.error('数据库连接获取异常:' + str(err))
        return None


# 关闭数据库连接
def closeMysqlConnect(connect):
    connect.close()


class NetLoanHomeSearch:
    # 最终的结果集
    searchInfoList = []
    # 最终的结果集
    pageNum = 1
    # 数据库连接
    db = getMysqlConnect()

    # 请求URL ，爬取页当前页面的HTML
    def requestUrlQueryDom(self, pageNum, status):
        # 定制请求头
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': '',
            'Host': 'www.wdzj.com',
            'Referer': 'https://www.wdzj.com/dangan/search?filter=e1&show=1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/67.0.3396.99 Safari/537.36'
        }

        try:
            # 请求URL
            if status == 'run':
                status = 'e1'
            if status == 'exception':
                status = 'e3-e2'
            url = "https://www.wdzj.com/dangan/search?filter=%s&show=1&sort=3&currentPage=%s" % (status, pageNum)
            response = requests.get(url, headers=headers)
            # 设置字符编码
            response.encoding = 'utf-8'
            # 返回HTML页面
            return response.text
        except Exception as err:
            logging.error('请求页面HTML信息异常, 页码%s, 异常信息:' % pageNum + str(err))
            return None

    # 解析HTML元素，获取需要的信息
    def analysisDom(self, pageNum, status):
        try:
            logging.info("爬虫获取网贷之家页码：%s" % pageNum)
            # 获取页面HTML
            dom = NetLoanHomeSearch.requestUrlQueryDom(self, pageNum, status)
            # HTML解析为BeautifulSoup
            soup = BeautifulSoup(dom)
            # 查找当前页公司信息列表
            itemList = soup.find("ul", class_="terraceList").find_all('li', class_='item')
            for item in itemList:
                # 公司名称
                companyName = item.find('h2').find('a').text
                # 排名 ,这里在100名往后不再有排名，也有可能直接没有那一行的DOM元素，所以做None判断
                if item.find('div', class_='itemTitleTag') is None:
                    rankDom = None
                else:
                    rankDom = item.find('div', class_='itemTitleTag').find('strong')
                if rankDom is None:
                    rank = ''
                else:
                    rank = rankDom.text
                # 内容DIV列表
                textDivs = item.find('a', class_='itemConLeft').select('div[class="itemConBox"]')
                # 公司上线时间
                foundTime = textDivs[3].text.split('：')[1]
                # 公司所在城市
                city = textDivs[2].text.split('：')[1]
                searchInfo = [companyName, rank, foundTime, city, status]
                NetLoanHomeSearch.insertIntoP2PRank(self, searchInfo)
                self.searchInfoList.append(searchInfo)

            # 第一次请求时，设置应该爬取的次数(HTML最大页码)
            if self.pageNum == 1:
                self.pageNum = soup.find('div', class_='pageList').select('a[class="page"]')[0]['currentnum']
                # self.pageNum = 3
        except Exception as err:
            logging.error('爬虫获取网贷之家页码:%s异常:' % pageNum + str(err))

    # 将爬取的数据分页入库
    def insertIntoP2PRank(self, info):
        try:
            # 获取数据库游标
            cursor = self.db.cursor()
            # 获取当前时间
            dt = datetime.datetime.now()
            # 拼接SQL语句
            sql = "INSERT INTO ra_p2p_company_rank (company_name, rank, found_time, city, status, created_time, update_time) VALUES " \
                  "('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (info[0], info[1], info[2], info[3], info[4], dt, dt)
            # 执行sql语句
            cursor.execute(sql)
            # 提交执行结果
            self.db.commit()

        except Exception as err:
            logging.error('插入数据库异常--error:' + str(err))

    # 删除数据
    def deleteP2PRank(self,):
        try:
            # 获取数据库游标d
            cursor = self.db.cursor()
            # 获取当前时间
            dt = datetime.datetime.now()
            # 拼接SQL语句
            sql = "DELETE FROM ra_p2p_company_rank"
            # 执行sql语句
            cursor.execute(sql)
            # 提交执行结果
            self.db.commit()

        except Exception as err:
            logging.error('清除历史数据异常--error:' + str(err))
        logging.error('清除历史数据完成--ok')

    # 入口方法
    def __init__(self):
        if self.db is None:
            return
        logging.info("爬虫获取网贷之家排名开始--wait，当前时间:" + str(datetime.datetime.now()))
        NetLoanHomeSearch.deleteP2PRank(self)
        try:
            params = {'run', 'exception'}
            for param in params:
                # 首次解析URL，并且获取最大页码
                logging.info("获取公司排名，当前运行状态:" + param)
                NetLoanHomeSearch.analysisDom(self, 1, param)
                # 获取所有的正在运营平台信息
                for i in range(2, int(self.pageNum) + 1):
                    NetLoanHomeSearch.analysisDom(self, i, param)
                self.pageNum = 1
        except Exception as err:
            logging.error('爬虫获取网贷之家发生错误--error:' + str(err))
        finally:
            # 关闭数据库链接
            closeMysqlConnect(self.db)
            logging.info('爬虫获取网贷之家排名结束--success，当前时间:' + str(datetime.datetime.now()))


# 忽略一些警告信息，比如BeautifulSoup解析器
warnings.filterwarnings('ignore')
# 设置日志警告级别
logging.basicConfig(level=logging.INFO)
# 开始
obj = NetLoanHomeSearch()
