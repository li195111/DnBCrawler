import sys
import traceback
from scrapy.crawler import CrawlerProcess
import scrapy
import logging
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
from multiprocessing import Process, Queue

class PageTest(scrapy.Spider):
    
    name = "detail_spider"
    
    start_urls = [#"https://www.dnb.com/business-directory/industry-analysis.agriculture-forestry-sector.html",
                  "https://www.dnb.com/business-directory/company-profiles.sociedad_aragonesa_de_gestion_agroambiental_sl.4fc1bd68ac6c10d1b313faab942605f1.html"]
    def parse(self, response):
        print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print (f"URL :\t\t{response.url} ... OK\n")
            main = response.xpath("//div[@class='hero_container']")
            pic = main.css("div.col-md-12")
            print (pic.css("h1.title::text").extract())
            print (pic.css("div.tradeName::text").extract())
            print (pic.css("div.street_address_1::text").extract())
            company_regional = pic.css("div.company_regional_address")
            print (company_regional.css("span.company_postal::text").extract(), company_regional.css("span.company_country::text").extract())
            type_role = pic.css("div.type-role")
            print (type_role.css("span.type-role-label::text").extract(), type_role.css("span.type::text").extract(), type_role.css("span.role::text").extract())
            print (pic.css("div.phone::text").extract())
            
        print('\n********** Second Status Info **********\n')

    
def run_detail_spider():
	process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
	process.crawl(PageTest)
	process.start()

if __name__ == "__main__":
    run_detail_spider()