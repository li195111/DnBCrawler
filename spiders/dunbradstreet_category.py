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
    idx = None
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK")
            RegionsDict = {}
            load_region_info = response.css("h1.title::text")
            load_error = 'Oh no! 500 Error' in load_region_info.extract()
            if load_error:
                # print ("Oh no! 500 Error ! ...")
                pass
            else:
                All_Regions = response.xpath("//div[@class='industry_country_crawl parbase basecomp section']").css("div.container").css("div.col-xs-6")
                for region in All_Regions:
                    Name = region.css("a::text").extract_first().strip()
                    Url = region.css("a::attr('href')").extract_first()
                    RegionsDict[Name] = Url
            self.q.put({"idx":self.idx,"data":RegionsDict})
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(industrys, q):
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
    for industry in industrys:
        idx = industry["ID"]
        data = industry["data"]
        url = DNB_BASE + data["CategoryURL"]
        process.crawl(CategoryPage, start_urls= [url,], q= q, idx= idx)
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
    DNB_BASE = 'https://www.dnb.com'
    all_parses = 0
    parsesed = 0
    num_industry = len(ALL_INDUSTRY)
    # for i in range(num_industry):
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 5
    file_name = f"Industry_{i}.json"
    file_path = os.path.join(INDUSTRY_DIR, file_name)
    while True:
        all_category_parses = 0
        category_parsesed = 0
        with open(file_path, 'r', encoding= 'utf-8') as f:
            industry_datas = json.load(f)
            num_category = len(industry_datas)
            all_category_parses += num_category
            NewIndustryDatas = []
            for idx, data in enumerate(industry_datas):
                if not ("Regions" in data) or not (len(data["Regions"]) != 0):
                    NewIndustryDatas.append({"ID":idx,"data":data})
            num_add = len(NewIndustryDatas)
            category_parsesed = num_category - num_add
            print (f"Industry {i}\t... Number to add:\t{num_add}")
            Q = multiprocessing.Queue()
            P = multiprocessing.Process(target= run_dunbrad_spider, args= (NewIndustryDatas, Q))
            P.start()
            P.join(timeout= num_add if num_add > 10 else 10)
            while not Q.empty():
                region_datas = Q.get(timeout= 1)
                idx = region_datas["idx"]
                regions = region_datas["data"]
                if len(regions) != 0:
                    industry_datas[idx]["Regions"] = regions
                else:
                    category_parsesed -= 1
            all_parses += all_category_parses
            parsesed += category_parsesed
        category_parse_rate = category_parsesed / all_category_parses
        print (f"Industry ... {specific_industry_names[i]:50s} ... Regions Parse Rate: {category_parse_rate * 100:.2f} %")
        with open(file_path, 'w', encoding= 'utf-8') as f:
            json.dump(industry_datas, f)
        if category_parse_rate == 1:
            break
    # all_parses_rate = parsesed / all_parses
    # print (f"Total Regions Parse Rate: {all_parses_rate * 100:.3f} %")