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


class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK")
            load_region_info = response.css("h1.title::text")
            load_error = 'Oh no! 500 Error' in load_region_info.extract()
            if load_error:
                # print ("Oh no! 500 Error ! ...")
                pass
            else:
                RegionsDict = self.q.get()
                All_Regions = response.xpath("//div[@class='industry_country_crawl parbase basecomp section']").css("div.container").css("div.col-xs-6")
                for region in All_Regions:
                    Name = region.css("a::text").extract_first().strip()
                    Url = region.css("a::attr('href')").extract_first()
                    RegionsDict[Name] = Url
                self.q.put(RegionsDict)
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(url, q):
    process = CrawlerProcess(
            {
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 3,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED':False
            }
        )
    process.crawl(CategoryPage, start_urls= [url,], q= q)
    process.start()
    
def Hello(url, q):
    print (f"Hello ... {url}")
    return 

specific_industry_names = np.array(["Manufacturing Sector",
                                    "Membership Organizations",
                                    "Mining",
                                    "Nonresidential Building Construction",
                                    "Wholesale Sector",
                                    "Heavy & Civil Engineering Construction",
                                    "Lodging",
                                    "Residential Construction Contractors",
                                    "Specialty Contractors",
                                    "Agriculture & Forestry Sector",
                                    "Professional Services Sector",
                                    "Rental & Leasing",
                                    "Retail Sector",
                                    "Business Services Sector",
                                    "Education Sector",
                                    "Electric Utilities",
                                    "Finance & Insurance Sector",
                                    "Real Estate",
                                    "Religious Organizations",
                                    "Arts, Entertainment & Recreation Sector",
                                    "Consumer Services",
                                    "Government",
                                    "Health Care Sector",
                                    "Management of Companies & Enterprises",
                                    "Media",
                                    "Natural Gas Distribution & Marketing",
                                    "Nonprofit Institutions",
                                    "Oil & Gas Exploration & Production",
                                    "Oil & Gas Field Services",
                                    "Oil & Gas Well Drilling",
                                    "Private Households",
                                    "Restaurants, Bars & Food Services",
                                    "Transportation Services Sector",
                                    "Water & Sewer Utilities"])

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    jobs = []
    queues = []
    all_parses = 0
    parsesed = 0
    num_industry = len(ALL_INDUSTRY)
    # for i in range(num_industry):
    i = 3
    file_name = f"Industry_{i}.json"
    file_path = os.path.join(INDUSTRY_DIR, file_name)
    industry_category_regoins = []
    all_category_parses = 0
    category_parsesed = 0
    with open(file_path, 'r', encoding= 'utf-8') as f:
        industry_datas = json.load(f)
        num_category = len(industry_datas)
        all_category_parses += num_category
        category_parsesed += num_category
        for idx, data in enumerate(industry_datas):
            if not ("Regions" in data) or not (len(data["Regions"]) != 0):
                category = data["Category"]
                url = 'https://www.dnb.com' + data["CategoryURL"]
                # print (f"{i} ... {idx} ... {category} Process ... {url}")
                Q = multiprocessing.Queue()
                Q.put({})
                P = multiprocessing.Process(target= run_dunbrad_spider, args= (url, Q))
                P.start()
                P.join(timeout= 10)
                CategoryRegions = Q.get(timeout= 5)
                print (f"{i} ... {idx} ... {category}\tNumb Datas ... {len(CategoryRegions)}")
                if len(CategoryRegions) != 0:
                    data["Regions"] = CategoryRegions
                else:
                    category_parsesed -= 1
            industry_category_regoins.append(data)
        all_parses += all_category_parses
        parsesed += category_parsesed
    category_parse_rate = category_parsesed / all_category_parses
    print (f"Industry ... {specific_industry_names[i]:50s} ... Regions Parse Rate: {category_parse_rate * 100:.2f} %")
    with open(file_path, 'w', encoding= 'utf-8') as f:
        json.dump(industry_category_regoins, f)
    # all_parses_rate = parsesed / all_parses
    # print (f"Total Regions Parse Rate: {all_parses_rate * 100:.3f} %")