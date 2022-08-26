# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from twisted.enterprise import adbapi
import pymysql
import logging
from pymysql import cursors


class LandtradePipeline:

    def __init__(self, dbpool):
        self.store_count = 0
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        adb_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PWD'],
            cursorclass=cursors.Cursor
        )
        dbpool = adbapi.ConnectionPool('pymysql', **adb_params)
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addCallback(self.handle_error)
        self.store_count += 1
        logging.info('FROM PIPELINES: Current Storage Counted: ' + str(self.store_count))
        return item

    def do_insert(self, cursor, item):
        sql = '''INSERT INTO LANDTRADE(PROVINCE, CITY, DISTRICT, REGION, ID, PROJECT_NAME, LAND_NAME, LOCATION, LAND_AREA, TOTAL_AREA, SOURCE, LAND_USAGE, SUPPLY_MODE, USE_TERM, INDUSTRY, LAND_LEVEL, OWNER, MAX_PLOT_RATIO, MIN_PLOT_RATIO, MAX_GREEN_RATE, MIN_GREEN_RATE, MAX_HEIGHT, MIN_HEIGHT, MAX_DENSITY, MIN_DENSITY, DELIVERY_DATE, COMMENCEMENT_DATE, COMPLETION_DATE, CONTRACT_DATE, BAIL, STARTING_PRICE, CLOSING_PRICE, URL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(sql, (item['province'], item['city'], item['district'], item['region'], item['id'], item['project_name'], item['land_name'], item['location'], item['land_area'], item['total_area'], item['source'], item['land_usage'], item['supply_mode'], item['use_term'], item['industry'], item['land_level'], item['owner'], item['max_plot_ratio'], item['min_plot_ratio'], item['max_green_rate'], item['min_green_rate'], item['max_height'], item['min_height'], item['max_density'], item['min_density'], item['delivery_date'], item['commencement_date'], item['completion_date'], item['contract_date'], item['bail'], item['starting_price'], item['closing_price'], item['url']))

    def handle_error(self, failure):
        if failure:
            print(failure)
