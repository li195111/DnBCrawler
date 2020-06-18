import numpy as np
import multiprocessing
import sys
import traceback
from scrapy.crawler import CrawlerProcess, CrawlerRunner
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
    region = ""
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            region_locations = {}
            locations = response.xpath("//div[@id='locationResults']").css("div.data")
            for loc in locations:
                name = loc.css("a::text").extract_first().strip()
                url = loc.css("a::attr('href')").extract_first()
                # numb = loc.css("a").css("span.number-countries::text").extract_first().replace("(","").replace(")","").replace(",","")
                region_locations[name] = url
                # total_numb += int(numb)
            # print (f"Number of Region Locations: {len(region_locations)}")
            self.q.put(region_locations)
        else:
            print(f"URL :\t\t{response.url} ... Error Code: {response.status}\n")
        # print('\n********** Second Status Info **********\n')
        return region_locations

def run_dunbrad_spider(url, Q):
    process = CrawlerProcess(
            {
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                # 'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 1,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED':False
            }
        )
    process.crawl(CategoryPage, start_urls= [url,], q= Q, region= reg)
    process.start()
    # process.stop()
    
def Hello(url, q):
    print (f"Hello ... ")
    return 

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    DNB_BASE = 'https://www.dnb.com'
    num_industry = len(ALL_INDUSTRY)
    # for i in range(num_industry):
    i = 0
    file_name = f"Industry_{i}.json"
    file_path = os.path.join(INDUSTRY_DIR, file_name)
    while True:
        num_log_items = False
        indusrty_category_regions_locations = []
        with open(file_path, 'r', encoding= 'utf-8') as f:
            industry_datas = json.load(f)
            num_category = len(industry_datas)
            for idx, data in enumerate(industry_datas):
                category = data["Category"]
                if ("Regions" in data) and (len(data["Regions"]) != 0):
                    regions = data["Regions"]
                    if not ("Locations" in data):
                        data["Locations"] = {}
                    for reg_idx, reg in enumerate(regions):
                        if not reg in data["Locations"]:
                            url = DNB_BASE + regions[reg]
                            Q = multiprocessing.Queue()
                            P = multiprocessing.Process(target= run_dunbrad_spider, args= (url, Q))
                            P.start()
                            P.join(timeout= 5)
                            while not Q.empty():
                                loc_data = Q.get(timeout= 1)
                                if len(loc_data) > 0:
                                    data["Locations"][reg] = loc_data
                                    num_log_items = True
                                print (reg, loc_data.__len__())
                            if num_log_items:
                                break
                indusrty_category_regions_locations.append(data)
        if num_log_items:
            with open(file_path, 'w', encoding= 'utf-8') as f:
                json.dump(indusrty_category_regions_locations, f)
        else:
            break
