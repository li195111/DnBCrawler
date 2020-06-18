import os

class ContantCrawler:
    
    def __init__(self):
        super(ContantCrawler, self).__init__()
        
        company_name_xpath = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/h4"
        address = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/p[1]"
        address = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/p[2]"
        address = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/p[3]"
        phone = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/div[1]/p[1]"
        phone = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/div[1]/p[1]/a[2]"
        webset = "/html/body/section[2]/div/div/div[1]/div/article/div[1]/div[2]/div[1]/p[2]"
        
        contact = "/html/body/section[2]/div/div/div[1]/div/article/div[3]"
        