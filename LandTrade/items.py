# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LandtradeItem(scrapy.Item):
    province = scrapy.Field()  # 省
    city = scrapy.Field()  # 市
    district = scrapy.Field()  # 区县
    region = scrapy.Field()  # 行政区
    id = scrapy.Field()  # 电子监管号
    project_name = scrapy.Field()  # 项目名称
    land_name = scrapy.Field()  # 地块名称
    location = scrapy.Field()  # 项目位置
    land_area = scrapy.Field()  # 土地面积
    total_area = scrapy.Field()  # 建筑面积
    source = scrapy.Field()  # 土地来源
    land_usage = scrapy.Field()  # 土地用途
    supply_mode = scrapy.Field()  # 供地方式
    use_term = scrapy.Field()  # 使用年限
    industry = scrapy.Field()  # 行业分类
    land_level = scrapy.Field()  # 土地级别
    owner = scrapy.Field()  # 土地使用权人
    max_plot_ratio = scrapy.Field()  # 容积率上限
    min_plot_ratio = scrapy.Field()  # 容积率下限
    max_green_rate = scrapy.Field()  # 绿化率上限
    min_green_rate = scrapy.Field()  # 绿化率下限
    max_height = scrapy.Field()  # 建筑高度上限
    min_height = scrapy.Field()  # 建筑高度下限
    max_density = scrapy.Field()  # 建筑密度上限
    min_density = scrapy.Field()  # 建筑密度下限
    delivery_date = scrapy.Field()  # 约定交地日期
    commencement_date = scrapy.Field()  # 约定开工日期
    completion_date = scrapy.Field()  # 约定竣工日期
    contract_date = scrapy.Field()  # 合同签订日期
    bail = scrapy.Field()  # 保证金
    starting_price = scrapy.Field()  # 起拍价
    closing_price = scrapy.Field()  # 成交价
    url = scrapy.Field()  # 网址


def if_keyword_exist(text, keywords):
    if text[keywords[0]].__contains__(keywords[1]):
        return text[keywords[0]][keywords[1]]
    else:
        return None
