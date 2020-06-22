import os
import sys
import time
import json
import numpy as np 
import sqlite3
import mysql.connector

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
Company_TABLE_SYNTAX ='''
CREATE TABLE IF NOT EXISTS %s (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    URL VARCHAR(2083) NOT NULL,
    CategoryID INTEGER NOT NULL,
    TownID INTEGER NOT NULL,
    ShortName VARCHAR(100),
    SalesRevenue VARCHAR(100),
    CompanyType VARCHAR(100),
    Website VARCHAR(100),
    Address VARCHAR(100),
    Phone VARCHAR(100));'''
    
TB_SYNTAXS = [Industry_TABLE_SYNTAX, Category_TABLE_SYNTAX, Region_TABLE_SYNTAX, Location_TABLE_SYNTAX, Town_TABLE_SYNTAX, Page_TABLE_SYNTAX, Company_TABLE_SYNTAX]
TB_NAMES = ["Industry", "Category", "Region", "Location", "Town", "Page", "Company"]
INSERT_TB_ITEMS = {"Industry":  "ID, Name",
                   "Category":  "Name, URL, IndustryID",
                   "Region":    "Name, URL, CategoryID",
                   "Location":  "Name, URL, CategoryID, RegionID",
                   "Town":      "Name, URL, CategoryID, LocationID",
                   "Page":      "PageNumb, URL, CategoryID, TownID",
                   "Company":   "Name, URL, CategoryID, TownID, ShortName, SalesRevenue, CompanyType, Website, Address, Phone"}
GET_ITEMS = {"Industry":    "ID,",
             "Category":    "Name, IndustryID",
             "Region":      "Name, CategoryID",
             "Location":    "Name, CategoryID, RegionID",
             "Town":        "Name, CategoryID, LocationID",
             "Page":        "PageNumb, CategoryID, TownID",
             "Company":     "Name, CategoryID, TownID"}
ITEM_IDS = {"Industry": np.array([0,]),
             "Category":np.array([0,2]),
             "Region":  np.array([0,2]),
             "Location":np.array([0,2,3]),
             "Town":    np.array([0,2,3]),
             "Page":    np.array([0,2,3]),
             "Company": np.array([0,2,3]),}

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

def CHECK_DB(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password
    )
    cursor = connect.cursor()
    cursor.execute("SHOW DATABASES")
    return ((DB_NAME,) in cursor)

def DROP_DB(DB_NAME, host= 'localhost', user= 'root', password= 'Aboutx_121'):
    connect = mysql.connector.connect(
        host= host,
        user= user,
        password= password
    )
    cursor = connect.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cursor.close()
    
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
    SELECT_SQL = f"SELECT * FROM {TB_NAME} WHERE {SELECT_MATCHES}"
    INSERT_SQL = f"INSERT INTO {TB_NAME} ({ITEMS}) VALUES ({INSERT_MATCHES})"
    VALUES = []
    for VALUE in INSERT_VAL:
        cursor.execute(SELECT_SQL, VALUE)
        result = cursor.fetchall()
        if len(result) == 0:
            VALUES.append(VALUE)
    # print (f"{TB_NAME} ... Number Insert {len(VALUES)}")
    cursor.executemany(INSERT_SQL, VALUES)
    connect.commit()
    cursor.close()
    return len(VALUES)
    
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
    if len(result) != 1:
        raise ValueError(f"{result}")
    return result[0][0]

def SelectItems(DB_NAME:str, TB_NAME:str, SELECT_ITEMS, ITEM_VALUES, host= 'localhost', user= 'root', password= 'Aboutx_121'):
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
    SELECT_SQL = f"SELECT * FROM {TB_NAME} WHERE {SELECT_MATCHES}"
    cursor.execute(SELECT_SQL, tuple(ITEM_VALUES))
    result = cursor.fetchall()
    cursor.close()
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

def json2sql(i= 0, jsonDir= 'Industrys', DB_NAME= 'test', host= 'localhost', user= 'root', password= 'Aboutx_121'):
    INIT_DB(DB_NAME)
    CUR_PATH = os.path.dirname(os.path.abspath(__file__))
    INDUSTRY_DIR = os.path.join(CUR_PATH, jsonDir)
    INDUSTRY_NAMES = os.listdir(INDUSTRY_DIR)
    INDUSTRY_NAMES.sort()
    INDUSTRY_PATHS = [os.path.join(INDUSTRY_DIR, name) for name in INDUSTRY_NAMES]
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
            # if "Regions" in industry_data:
            #     RegionData = []
            #     for reg_name in industry_data["Regions"]:
            #         url = industry_data["Regions"][reg_name]
            #         reg_data = (reg_name, url, CategoryID)
            #         in_region_database = np.sum(np.sum(np.array(region_datas)[:,1:] == reg_data,1) == 3) > 0
            #         if not in_region_database:
            #             RegionData.append(reg_data)
            #     num_insert = INSERT(DB_NAME, "Region", RegionData)
            #     print (f"Industry {i+1:02d} Region ... Number Insert {num_insert:05d} ... {(idx+1) * 100 / numCategorys:.2f} %", end= '\r')
            if ("Locations" in industry_data):
                num_reg_datas = len(industry_data["Locations"])
                for reg_idx, reg_name in enumerate(industry_data["Locations"]):
                    LocationData = []
                    RegionID = GetItemID(DB_NAME, "Region", [reg_name, CategoryID])
                    location_datas = SelectItems(DB_NAME, "Location", "RegionID,", [RegionID,])
                    num_reg_loc_datas = len(industry_data["Locations"][reg_name])
                    loc_name = np.array(list(industry_data["Locations"][reg_name].keys()))
                    loc_urls = np.array(list(industry_data["Locations"][reg_name].values()))
                    loc_datas = np.stack([loc_name,loc_urls,np.full_like(loc_name,CategoryID),np.full_like(loc_name,RegionID)],-1)
                    if len(location_datas) > 0:
                        for loc_idx, loc_data in enumerate(loc_datas):
                            if not loc_data in np.array(location_datas)[:,1:]:
                                LocationData.append(tuple(loc_data))
                    else:
                        LocationData = loc_datas
                    LocationData = [tuple(loc_data) for loc_data in LocationData]
                    print (f"Industry {i+1:02d} Location ... Num insert {len(LocationData):05d} ... {(idx+1) * 100 / numCategorys:03.2f} % {(reg_idx+1) * 100 / num_reg_datas:03.2f} %", end= '\r')
                    um_insert = INSERT(DB_NAME, "Location", LocationData)
            # if ("Town" in industry_data):
            #     TownData = []
            #     for reg_name in industry_data["Town"]:
            #         for loc_name in industry_data["Town"][reg_name]:
            #             LocationID = GetItemID(DB_NAME, "Location", [loc_name, CategoryID, RegionID])

            #             for town_name in industry_data["Town"][reg_name][loc_name]:
            #                 town_url = industry_data["Town"][reg_name][loc_name][town_name]
            #                 TownData.append((town_name, town_url, CategoryID, LocationID))
            #     INSERT(DB_NAME, "Town", TownData)
    return
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 0
    json2sql(i)
    
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
    # SQL = "DELETE FROM test.location where RegionID = 13391"
    # cursor.execute(SQL)
    # connect.commit()
    pass