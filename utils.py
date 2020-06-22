import os
import sys
import time
import json
import numpy as np 
import sqlite3
import mysql.connector
import pymysql

Industry_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL);'''
Category_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    IndustryID INTEGER NOT NULL);'''
Region_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    CategoryID INTEGER NOT NULL);'''
Location_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    CategoryID INTEGER NOT NULL,
    RegionID INTEGER NOT NULL);'''
Town_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    CategoryID INTEGER NOT NULL,
    LocationID INTEGER NOT NULL);'''
Page_TABLE_SYNTAX = '''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    PageNumb INTEGER NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    CategoryID INTEGER NOT NULL,
    TownID INTEGER NOT NULL);'''
PageCompany_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(1024) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    SalesRevenue VARCHAR(100),
    CategoryID INTEGER NOT NULL,
    TownID INTEGER NOT NULL,
    PageID INTEGER NOT NULL);'''
Company_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(1024) NOT NULL,
    CategoryID INTEGER NOT NULL,
    TownID INTEGER NOT NULL,
    PageCompanyID INTEGER NOT NULL,
    ShortName VARCHAR(100),
    SalesRevenue VARCHAR(100),
    CompanyType VARCHAR(100),
    Website VARCHAR(100),
    Address VARCHAR(2083),
    Phone VARCHAR(100));'''
    
TB_SYNTAXS = [Industry_TABLE_SYNTAX, Category_TABLE_SYNTAX, Region_TABLE_SYNTAX, Location_TABLE_SYNTAX, Town_TABLE_SYNTAX, Page_TABLE_SYNTAX, PageCompany_TABLE_SYNTAX, Company_TABLE_SYNTAX]
TB_NAMES = ["Industry", "Category", "Region", "Location", "Town", "Page", "PageCompany", "Company"]
INSERT_TB_ITEMS = {"Industry":   "ID, Name",
                   "Category":   "Name, URL, IndustryID",
                   "Region":     "Name, URL, CategoryID",
                   "Location":   "Name, URL, CategoryID, RegionID",
                   "Town":       "Name, URL, CategoryID, LocationID",
                   "Page":       "PageNumb, URL, CategoryID, TownID",
                   "PageCompany":"Name, URL, SalesRevenue, CategoryID, TownID, PageID",
                   "Company":    "Name, CategoryID, TownID, PageCompanyID, ShortName, SalesRevenue, CompanyType, Website, Address, Phone"}
GET_ITEMS = {"Industry":    "ID,",
             "Category":    "Name, IndustryID",
             "Region":      "Name, CategoryID",
             "Location":    "Name, CategoryID, RegionID",
             "Town":        "Name, CategoryID, LocationID",
             "Page":        "PageNumb, CategoryID, TownID",
             "PageCompany": "Name, CategoryID, TownID, PageID",
             "Company":     "Name, CategoryID, TownID, PageCompanyID"}
ITEM_IDS = {"Industry":     np.array([0,]),
             "Category":    np.array([0,2]),
             "Region":      np.array([0,2]),
             "Location":    np.array([0,2,3]),
             "Town":        np.array([0,2,3]),
             "Page":        np.array([0,2,3]),
             "PageCompany": np.array([0,3,4,5]),
             "Company":     np.array([0,2,3,4]),}

def CONNECT_DB_CURSOR(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME
    )
    return connect.cursor()

def CREATE_DB(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password
    )
    cursor = connect.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.close()
    connect.close()

def CHECK_DB(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password
    )
    cursor = connect.cursor()
    cursor.execute("SHOW DATABASES")
    CHECK = ((DB_NAME,) in cursor)
    cursor.close()
    connect.close()

def DROP_DB(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password
    )
    cursor = connect.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cursor.close()
    connect.close()

def CREATE_TB(TB_NAME, TB_SYNTAX, DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME
    )
    cursor = connect.cursor()
    cursor.execute(TB_SYNTAX % TB_NAME)
    cursor.close()
    connect.close()

def INIT_DB(DB_NAME= "test"):
    CREATE_DB(DB_NAME)
    for i in range(len(TB_NAMES)):
        CREATE_TB(TB_NAMES[i], TB_SYNTAXS[i], DB_NAME)
    
def INSERT(DB_NAME:str, TB_NAME:str, INSERT_VAL:list, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME
    )
    cursor = connect.cursor()
    ITEMS = INSERT_TB_ITEMS[TB_NAME]
    num_items = len([item for item in ITEMS.split(",") if len(item) != 0])
    if num_items == 1:
        ITEMS = ITEMS[:-1]
        MATCH_ITEMS = "%s"
    else:
        MATCH_ITEMS = ["%s"] * num_items
    SELECT_MATCHES = " AND".join([ITEMS.split(",")[i] + " = " + MATCH_ITEMS[i] for i in range(num_items)])
    INSERT_MATCHES = ",".join(MATCH_ITEMS)
    INSERT_SQL = f'''INSERT INTO {TB_NAME} ({ITEMS}) VALUE ({INSERT_MATCHES})'''
    SELECT_SQL = f'''
    SELECT * FROM 
        (SELECT {INSERT_MATCHES}) AS tmp 
    WHERE EXISTS 
        (SELECT * FROM {TB_NAME} WHERE {SELECT_MATCHES})'''
    INSERT_VAL = [tuple(VAL) for VAL in INSERT_VAL]
    try:
        cursor.executemany(INSERT_SQL, INSERT_VAL)
    except Exception as e:
        print (f"Error :\t{e}")
        print (INSERT_SQL)
        print (INSERT_VAL)
    try:
        connect.commit()
    except Exception as e:
        print (f"Error :\t{e}")
    cursor.close()
    connect.close()

def GetItemID(DB_NAME, TB_NAME, ITEM_VALUES, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME
    )
    cursor = connect.cursor()
    ITEMS = GET_ITEMS[TB_NAME]
    num_items = len([item for item in ITEMS.split(",") if len(item) != 0])
    if num_items == 1:
        ITEMS = ITEMS[:-1]
        MATCH_ITEMS = "%s"
    else:
        MATCH_ITEMS = str(tuple(["%s"] * num_items)).replace("'","")
    SELECT_SQL = f"SELECT ID FROM {TB_NAME} WHERE ({ITEMS}) = {MATCH_ITEMS}"
    SELECT_VAL = tuple(ITEM_VALUES)
    cursor.execute(SELECT_SQL, SELECT_VAL)
    result = cursor.fetchall()
    cursor.close()
    connect.close()
    if len(result) != 1:
        raise result
    return result[0][0]

def SelectItems(DB_NAME:str, TB_NAME:str, SELECT_ITEMS, ITEM_VALUES, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = pymysql.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME,
        port= 3306)
    # connect = mysql.connector.connect(
    #         host= host,
    #         user= user,
    #         password= password,
    #         database= DB_NAME,
    #         port= 3306
    # )
    cursor = connect.cursor()
    num_items = len([item for item in SELECT_ITEMS.split(",") if len(item) != 0])
    if num_items == 1:
        SELECT_ITEMS = SELECT_ITEMS[:-1]
        MATCH_ITEMS = "%s"
        SELECT_MATCHES = SELECT_ITEMS + " = " + MATCH_ITEMS
    else:
        MATCH_ITEMS = ["%s"] * num_items
        SELECT_MATCHES = " AND".join([SELECT_ITEMS.split(",")[i] + " = " + MATCH_ITEMS[i] for i in range(num_items)])
    SELECT_SQL = f"SELECT * FROM {TB_NAME} WHERE {SELECT_MATCHES}"
    cursor.execute(SELECT_SQL, tuple(ITEM_VALUES))
    result = cursor.fetchall()
    cursor.close()
    connect.close()
    return result

def DeleteItem(DB_NAME:str, TB_NAME:str, SELECT_ITEMS, ITEM_VALUES, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password,
        database= DB_NAME
    )
    cursor = connect.cursor()
    num_items = len([item for item in SELECT_ITEMS.split(",") if len(item) != 0])
    if num_items == 1:
        SELECT_ITEMS = SELECT_ITEMS[:-1]
        MATCH_ITEMS = "%s"
        SELECT_MATCHES = SELECT_ITEMS + " = " + MATCH_ITEMS
    else:
        MATCH_ITEMS = ["%s"] * num_items
        SELECT_MATCHES = " AND".join([SELECT_ITEMS.split(",")[i] + " = " + MATCH_ITEMS[i] for i in range(num_items)])
    DELETE_SQL = f"DELETE FROM {TB_NAME} where {SELECT_MATCHES}"
    cursor.execute(DELETE_SQL)
    connect.commit()
    cursor.close()
    connect.close()

def json2sql(mode= None,jsonDir= 'Industrys', DB_NAME= 'test', host= 'localhost', user= 'root', password= 'Aboutx_121'):
    INIT_DB(DB_NAME)
    CUR_PATH = os.path.dirname(os.path.abspath(__file__))
    INDUSTRY_DIR = os.path.join(CUR_PATH, jsonDir)
    INDUSTRY_NAMES = os.listdir(INDUSTRY_DIR)
    INDUSTRY_NAMES.sort()
    INDUSTRY_PATHS = [os.path.join(INDUSTRY_DIR, name) for name in INDUSTRY_NAMES]
    for i in range(len(INDUSTRY_NAMES)):
        file_name = f"Industry_{i}.json"
        file_path = os.path.join(INDUSTRY_DIR, file_name)
        with open(file_path, 'r', encoding= 'utf-8') as f:
            industry_datas = json.load(f)
            IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
            category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
            numCategorys = len(industry_datas)
            for idx, industry_data in enumerate(industry_datas):
                CategoryID = GetItemID(DB_NAME, "Category", [industry_data["Category"], IndustryID])
                region_datas = SelectItems(DB_NAME, "Region", "CategoryID,", [CategoryID,])
                if mode == "region":
                    if "Regions" in industry_data:
                        RegionData = []
                        for reg_name in industry_data["Regions"]:
                            url = industry_data["Regions"][reg_name]
                            reg_data = (reg_name, url, CategoryID)
                            in_region_database = np.sum(np.sum(np.array(region_datas)[:,1:] == reg_data,1) == 3) > 0
                            if not in_region_database:
                                RegionData.append(reg_data)
                        INSERT(DB_NAME, "Region", RegionData)
                        print (f"Industry {i+1:02d} Region ... Number Insert {num_insert:05d} ... {(idx+1) * 100 / numCategorys:.2f} %", end= '\r')
                elif mode == "location":
                    if ("Locations" in industry_data):
                        num_reg_datas = len(industry_data["Locations"])
                        for reg_idx, reg_name in enumerate(industry_data["Locations"]):
                            LocationData = []
                            RegionID = GetItemID(DB_NAME, "Region", [reg_name, CategoryID])
                            num_reg_loc_datas = len(industry_data["Locations"][reg_name])
                            loc_name = np.array(list(industry_data["Locations"][reg_name].keys()))
                            loc_urls = np.array(list(industry_data["Locations"][reg_name].values()))
                            LocationData = np.stack([loc_name,loc_urls,np.full_like(loc_name,CategoryID),np.full_like(loc_name,RegionID)],-1)
                            print (f"Industry {i+1:02d} Location ... Num insert {len(LocationData):05d} ... {(idx+1) * 100 / numCategorys:03.2f} % {(reg_idx+1) * 100 / num_reg_datas:03.2f} %", end= '\r')
                            INSERT(DB_NAME, "Location", LocationData)
                elif mode == "town":
                    if ("Towns" in industry_data):
                        num_reg_datas = len(industry_data["Towns"])
                        for reg_idx, reg_name in enumerate(industry_data["Towns"]):
                            RegionID = GetItemID(DB_NAME, "Region", [reg_name, CategoryID])
                            num_loc_datas = len(industry_data["Towns"][reg_name])
                            TownDatas = []
                            for loc_name in industry_data["Towns"][reg_name]:
                                LocationID = GetItemID(DB_NAME, "Location", [loc_name, CategoryID, RegionID])
                                if LocationID:
                                    town_name = np.array(list(industry_data["Towns"][reg_name][loc_name].keys()))
                                    town_urls = np.array(list(industry_data["Towns"][reg_name][loc_name].values()))
                                    TownData = np.stack([town_name,town_urls,np.full_like(town_name,CategoryID),np.full_like(town_name,LocationID)],-1)
                                    TownDatas.append(TownData)
                            TownDatas = np.concatenate(TownDatas,0)
                            print (f"Industry {i+1:02d} Town ... Num insert {len(TownDatas):05d} ... {(idx+1) * 100 / numCategorys:03.2f} % {(reg_idx+1) * 100 / num_reg_datas:03.2f} %", end= '\r')
                            INSERT(DB_NAME, "Town", TownDatas)
    return
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 0
    # if i == 0:
    #     json2sql('region')
    # elif i == 1:
    #     json2sql('location')
    # elif i == 2:
    #     json2sql('town')
    # DB_NAME= 'test'
    # host= 'localhost'
    # user= 'root'
    # password= 'Aboutx_121'
    # connect = mysql.connector.connect(
    #     host= host,
    #     user= user,
    #     password= password,
    #     database= DB_NAME
    # )
    # cursor = connect.cursor()
    # SQL = "DELETE FROM location where CategoryID >= 152 and CategoryID <= 157"
    # cursor.execute(SQL)
    # connect.commit()
    pass