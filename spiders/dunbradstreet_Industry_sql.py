import numpy as np
import multiprocessing
import sys
import traceback
from scrapy.crawler import CrawlerProcess
import scrapy
import logging
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
import json
import os
from module import Category, Industry
from utils import INIT_DB, INSERT

INDUSTRYS = ["Manufacturing Sector", "Membership Organizations", "Mining", "Nonresidential Building Construction", "Wholesale Sector", "Heavy & Civil Engineering Construction",
             "Lodging", "Residential Construction Contractors","Specialty Contractors","Agriculture & Forestry Sector","Professional Services Sector","Rental & Leasing",
             "Retail Sector","Business Services Sector","Education Sector","Electric Utilities","Finance & Insurance Sector","Real Estate","Religious Organizations",
             "Arts, Entertainment & Recreation Sector","Consumer Services","Government","Health Care Sector","Management of Companies & Enterprises","Media",
             "Natural Gas Distribution & Marketing","Nonprofit Institutions","Oil & Gas Exploration & Production","Oil & Gas Field Services","Oil & Gas Well Drilling",
             "Private Households","Restaurants, Bars & Food Services","Transportation Services Sector","Water & Sewer Utilities"]

DB_NAME = "test"

class IndustryPage(scrapy.Spider):

    name = "industry_spider"
    base_url = 'https://www.dnb.com'
    start_urls = ["https://www.dnb.com/business-directory.html", ]
    def parse(self, response):
        print('\n********** Second Status Info **********\n')
        if response.status == 200:
            
            INIT_DB(DB_NAME)
            
            CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
            if not os.path.exists(INDUSTRY_DIR):
                os.mkdir(INDUSTRY_DIR)
            print(f"URL :\t\t{response.url} ... OK\n")
            all_industrys = response.xpath("//div[@class='accordion_list clearfix']").css("div.col-md-12")
            all_industry_names = np.array(all_industrys.css("div.title::text").extract())
            specific_industry_names = np.array(INDUSTRYS)

            IndustryData = []
            CategoryData = []
            for i in range(len(specific_industry_names)):
                industry_datas = []
                industry_mask = all_industry_names == specific_industry_names[i]
                masked_industrys = np.array(all_industrys)[industry_mask]
                for industry in masked_industrys:
                    industry_name = industry.css("div.title::text").extract_first()
                    industry_category_names = industry.css("div.link").css("a::text").extract()
                    industry_category_urls = industry.css("div.link").css("a::attr('href')").extract()
                    IndustryData.append((i+1, industry_name))
                    for idx, category in enumerate(industry_category_names):
                        CategoryData.append((category, industry_category_urls[idx], i+1))
            INSERT(DB_NAME, "Industry", IndustryData)
            INSERT(DB_NAME, "Category", CategoryData)

        print('\n********** Second Status Info **********\n')

def run_dunbrad_spider():
    process = CrawlerProcess(
        {'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'})
    process.crawl(IndustryPage)
    process.start()

if __name__ == "__main__":
    P = multiprocessing.Process(target= run_dunbrad_spider)
    P.start()
    P.join()
        
