import scrapy
import json
import datetime
from LandTrade import datesettings
from LandTrade.items import LandtradeItem


def if_keyword_exist(text, keywords):
    if text[keywords[0]].__contains__(keywords[1]):
        return text[keywords[0]][keywords[1]]
    else:
        return None


class LandtradeSpider(scrapy.Spider):

    name = 'landtrade'
    allowed_domains = ['landchina.com']
    list_url = 'https://api.landchina.com/tGdxm/result/list'
    detail_url = 'https://api.landchina.com/tGdxm/result/detail'
    header = {'Content-Type': 'application/json',
              'Accept': 'application/json, text/plain, */*',
              'Sec-Fetch-Mode': 'cors',
              'Origin': 'https://www.landchina.com',
              'Referer': 'https://www.landchina.com/'}
    count = 0

    start_date = datetime.datetime(2005, 1, 1, 0, 0)
    delta = datetime.timedelta(days=1, hours=23, minutes=59, seconds=59)  # 设置搜索间隔为两日
    end = datetime.datetime(2005, 1, 31, 0, 0)

    def parse(self, response, **kwargs):
        pass

    def start_requests(self):

        list_payload = {'pageNum': 1,
                        'pageSize': 10,
                        'startDate': str(self.start_date),
                        'endDate': str(self.end)}

        yield scrapy.Request(url=self.list_url, method='POST', headers=self.header, body=json.dumps(list_payload), meta={'payload': list_payload}, callback=self.parse_list)

    def parse_list(self, response):

        self.logger.info('FROM SPIDER: Starting Date: ' + response.meta['payload']['startDate'])
        self.logger.info('FROM SPIDER: Ending Date: ' + response.meta['payload']['endDate'])

        response_text = json.loads(response.text)
        self.logger.info(response_text)
        list_payload = response.meta['payload']
        page_num = response_text['data']['pageNum']
        last_page = response_text['data']['navigateLastPage']
        for each in response_text['data']['list']:
            guid = each['gdGuid']
            detail_payload = {'gdGuid': guid}
            yield scrapy.Request(url=self.detail_url, method='POST', headers=self.header,
                                 body=json.dumps(detail_payload), meta={'payload': detail_payload},
                                 callback=self.parse_detail)
        if page_num < last_page:
            list_payload['pageNum'] = page_num + 1
            yield scrapy.Request(url=self.list_url, method='POST', headers=self.header, body=json.dumps(list_payload), meta={'payload': list_payload}, callback=self.parse_list)

    def parse_detail(self, response):

        detail_payload = response.meta['payload']
        response_text = json.loads(response.text)
        url = 'https://www.landchina.com/#/landSupplyDetail?id=' + detail_payload['gdGuid'] + '&type=供地结果&path=0'
        self.logger.info('FROM SPIDER: Current URL: ' + url)

        if response_text['msg'] == '操作成功' and response_text['code'] == 200:
            self.count = self.count + 1
            self.logger.info('FROM SPIDER: Current Quantity Of Details: ' + str(self.count))
            self.logger.info('FROM SPIDER: Request Succeed. Msg: ' + response_text['msg'] + ' Code: ' + str(response_text['code']))
            self.logger.info(response_text)
            LandtradeItems = LandtradeItem()
            LandtradeItems['province'] = if_keyword_exist(response_text, ['data', 'province'])
            LandtradeItems['city'] = if_keyword_exist(response_text, ['data', 'city'])
            LandtradeItems['district'] = if_keyword_exist(response_text, ['data', 'area'])
            LandtradeItems['region'] = if_keyword_exist(response_text, ['data', 'xzqFullName'])
            LandtradeItems['id'] = if_keyword_exist(response_text, ['data', 'area'])
            LandtradeItems['project_name'] = if_keyword_exist(response_text, ['data', 'xmMc'])
            LandtradeItems['land_name'] = if_keyword_exist(response_text, ['relate', 'zdBh'])
            LandtradeItems['location'] = if_keyword_exist(response_text, ['data', 'tdZl'])
            LandtradeItems['land_area'] = if_keyword_exist(response_text, ['relate', 'mj'])
            LandtradeItems['total_area'] = if_keyword_exist(response_text, ['relate', 'jzMj'])
            LandtradeItems['source'] = if_keyword_exist(response_text, ['data', 'tdLy'])
            LandtradeItems['land_usage'] = if_keyword_exist(response_text, ['data', 'tdYt'])
            LandtradeItems['supply_mode'] = if_keyword_exist(response_text, ['data', 'gyFs'])
            LandtradeItems['use_term'] = if_keyword_exist(response_text, ['data', 'crNx'])
            LandtradeItems['industry'] = if_keyword_exist(response_text, ['data', 'hyFl'])
            LandtradeItems['land_level'] = if_keyword_exist(response_text, ['data', 'tdJb'])
            LandtradeItems['owner'] = if_keyword_exist(response_text, ['data', 'srr'])
            LandtradeItems['max_plot_ratio'] = if_keyword_exist(response_text, ['data', 'maxRjl'])
            LandtradeItems['min_plot_ratio'] = if_keyword_exist(response_text, ['data', 'minRjl'])
            LandtradeItems['max_green_rate'] = if_keyword_exist(response_text, ['data', 'maxLhl'])
            LandtradeItems['min_green_rate'] = if_keyword_exist(response_text, ['data', 'minLhl'])
            LandtradeItems['max_height'] = if_keyword_exist(response_text, ['data', 'maxJzGd'])
            LandtradeItems['min_height'] = if_keyword_exist(response_text, ['data', 'minJzGd'])
            LandtradeItems['max_density'] = if_keyword_exist(response_text, ['data', 'maxJzMd'])
            LandtradeItems['min_density'] = if_keyword_exist(response_text, ['data', 'minJzMd'])
            delivery_date = if_keyword_exist(response_text, ['data', 'jdSj'])
            LandtradeItems['delivery_date'] = datetime.datetime.fromtimestamp(delivery_date/1000) if delivery_date else None
            commencement_date = if_keyword_exist(response_text, ['data', 'dgSj'])
            LandtradeItems['commencement_date'] = datetime.datetime.fromtimestamp(commencement_date/1000) if commencement_date else None
            completion_date = if_keyword_exist(response_text, ['data', 'jgSj'])
            LandtradeItems['completion_date'] = datetime.datetime.fromtimestamp(completion_date/1000) if completion_date else None
            contract_date = if_keyword_exist(response_text, ['data', 'qdRq'])
            LandtradeItems['contract_date'] = datetime.datetime.fromtimestamp(contract_date/1000) if contract_date else None
            LandtradeItems['bail'] = if_keyword_exist(response_text, ['relate', 'crBzj'])
            LandtradeItems['starting_price'] = if_keyword_exist(response_text, ['relate', 'qsj'])
            LandtradeItems['closing_price'] = if_keyword_exist(response_text, ['relate', 'cjJg'])
            LandtradeItems['url'] = url

            yield LandtradeItems
        else:
            self.logger.info('FROM SPIDER: Request Failed. Msg: ' + response_text['msg'] + ' Code: ' + str(response_text['code']))
            self.logger.info('FROM SPIDER: Retrying')
            yield scrapy.Request(url=self.detail_url, method='POST', headers=self.header, body=json.dumps(detail_payload), meta={'payload': detail_payload}, callback=self.parse_detail)
