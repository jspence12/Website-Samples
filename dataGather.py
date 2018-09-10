"""
Written and designed by Josh Spence.

This script is intended to be run as a cron job every 2 weeks. 
Changes in economic reporting are not frequent enough to justify more frequent searches

Contains all data gather functions for my personal website.
All individual gathers are treated as a subclass of the SqlObject super class.
This superclass was created to hold data from multiple api calls to minimize the 
number of db calls necessary to complete a gather, especially update queries which
bottleneck the speed of this integration.

Each subclass should have a execute() method which calls all the necessary methods 
to gather, parse, then store data using a sqlObject data structure.

"""

from copy import copy
from datetime import date
import json
import mysql.connector
import os
import re
import requests
from requests_html import HTMLSession
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

#Config Variables
db_credentials = {"user":"","password":"","database":""}
eod_credentials = {"user":"","password":""}
fed_key = ""
bls_key = ""


class SqlObject:
    """
    Creates storage object for quickly consolidating results from multiple API calls into a single database row.
    This greatly reduces the number of database calls required to successfully update all records
    """
    __slots__ = ["table","data","dimension_list","metric_list","_db_line","_insert_list","_cnx","_cursor"]

    def __init__(self,table,dimensions,metrics):
        self.table = table                          #default table variable for methods
        self.data = {}                              #storage method to minimize number of db operations
        self.dimension_list = dimensions             #must be in order of nesting of self.data
        self.metric_list = metrics                   #order does not matter
        self._db_line = []                           #holding object for query and pushing into insert list
        self._insert_list = []                       #for bulk inserting new entries
        self._cnx = None                            #for maintaing connections across function scopes as needed
        self._cursor = None                         #for maintaing connections across function scopes as needed

    def execute(self):
        """Abstract function for gathering data from a source and storing it to a database"""
        raise NotImplementedError

    def recurse_parse(self,_dataset=None,_rowIndex=0):
        """Parses sqlObject data attribute recursively and performs INSERT or UPDATE operations as required"""
        if _dataset == None:
            _dataset = self.data
            self._cnx = mysql.connector.connect(**db_credentials)
        for branch in _dataset:
            if isinstance(_dataset[branch],dict):
                self._modify_db_line(branch,_rowIndex)
                self.recurse_parse(_dataset=_dataset[branch],_rowIndex=_rowIndex + 1)
            else:
                self._modify_db_line({},_rowIndex)
                for branch in _dataset:
                    self._db_line[-1][branch] = _dataset[branch]
                for metric in self.metric_list:
                    if metric not in self._db_line[_rowIndex]:
                        self._db_line[-1][metric] = "NULL"
                self._select()
                break
        if _rowIndex == 0: 
            if len(self._insert_list) > 0:
                self._insert()
            self._cnx.close()

    def _modify_db_line(self,branch,_rowIndex):
        """Helper function for recurse_parse(). Extends _db_line if no value present at _rowIndex"""
        try:
            self._db_line[_rowIndex] = branch
        except:
            self._db_line.append(branch)

    def _select(self):
        """Checks if a given _db_line exists in the database. If it does, calls UPDATE operation, else inserts into a list for later bulk INSERT"""
        self._cursor = self._cnx.cursor()
        select_query = "SELECT * FROM %s WHERE %s = \"%s\"" % (self.table,self.dimension_list[0],self._db_line[0])
        for dimension_name,dimension_value in zip(self.dimension_list[1:],self._db_line[1:]):
            if dimension_value == "NULL":
                select_query += " AND %s = %s" % (dimension_name,dimension_value)
            else:
                select_query += " AND %s = \"%s\"" % (dimension_name,dimension_value)
        select_query += ";"
        self._cursor.execute(select_query)
        self._cursor.fetchall()
        if self._cursor.rowcount >= 1:
            self._update()
        else: 
            self._insert_list.append(self._db_line[:])
        self._cursor.close()

    def _update(self):
        """updates a single line in the database"""
        update_query = "UPDATE %s SET " % (self.table)
        for key in self._db_line[-1]:
            if self._db_line[-1][key] == "NULL":
                update_query += "%s = %s," % (key,self._db_line[-1][key])
            else:
                update_query += "%s = \"%s\"," % (key,self._db_line[-1][key])
        update_query = update_query[0:len(update_query)-1] + " WHERE %s = \"%s\"" % (self.dimension_list[0],self._db_line[0])
        for dimension_name,dimension_value in zip(self.dimension_list[1:],self._db_line[1:]):
            update_query += " AND %s = \"%s\"" % (dimension_name,dimension_value)
        update_query += ";"
        self._cursor.execute(update_query)
        self._cnx.commit()

    def _insert(self):
        """Bulk inserts all new lines from a recurse_parse() operation"""
        self._cursor = self._cnx.cursor()
        max_query_size = 65535 // (len(self.dimension_list) + len(self.metric_list))  #largest number of rows allowed in a single insert statement
        insert_list_index = 0
        insert_query_intro = self._constructinsert_query_intro()
        while insert_list_index < len(self._insert_list):
            insert_query = insert_query_intro
            insert_query_size = 0
            while insert_query_size < max_query_size and insert_list_index < len(self._insert_list):
                insert_query += "(\"%s\"" %(self._insert_list[insert_list_index][0])
                for value in self._insert_list[insert_list_index][1:len(self._insert_list[insert_list_index])-1]:
                    insert_query += ", \"%s\"" % (value)
                for key in self.metric_list:
                    if self._insert_list[insert_list_index][-1][key] == "NULL":
                        insert_query += ", %s" % (self._insert_list[insert_list_index][-1][key])
                    else:
                        insert_query += ", \"%s\"" % (self._insert_list[insert_list_index][-1][key])
                insert_query += "), "
                insert_query_size += 1
                insert_list_index += 1
            insert_query = insert_query[0:len(insert_query)-2] + ";"
            print(insert_query)
            self._cursor.execute(insert_query)
            self._cnx.commit()    
        self._cursor.close()

    def _constructinsert_query_intro(self):
        """Helper function for insert() operation"""
        insert_query_intro = "INSERT INTO %s (%s" % (self.table,self.dimension_list[0])
        for dimension in self.dimension_list[1:]:
            insert_query_intro += ", %s" % (dimension)
        for metric in self.metric_list:
            insert_query_intro += ", %s" % (metric)
        insert_query_intro += ") VALUES "
        return insert_query_intro


class FedData(SqlObject):
    """Gathers data from Federal Reserve API and stores into MySQL"""

    __slots__ = ["table","data","dimension_list","metric_list","_db_line","_insert_list","_cnx","_cursor","series_ids","api_key"]

    def __init__(self):
        SqlObject.__init__(self,"economy",['date'],["unemployment","gdp"])
        self.series_ids = {"LRUN64TTUSA156N":"unemployment","GDPA":"gdp"}
        self.api_key = fed_key
    
    def execute(self):
        for seriesId in self.series_ids:
            response = self._fed_get_data(seriesId)
            self._fed_parse(response,self.series_ids[seriesId])
        self.recurse_parse()

    def _fed_get_data(self,series_id,jsonFormat=True):
        """Calls Federal Reserve API and returns response"""
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s" % (series_id, self.api_key)
        if jsonFormat == True:
            url = url + "&file_type=json"
        response = requests.get(url)
        print(response.text)
        return json.loads(response.text)

    def _fed_parse(self,api_response,output_column):
        """parses Fed API data broken out by year"""
        if "error_code" in api_response:
            print(str(api_response["error_code"]) +":"+ api_response["error_message"])
            return
        dataset = []
        for data in api_response["observations"]:
            if data["value"] == ".":
                continue
            if self.data.get(data["date"]) is not None:
                self.data[data["date"]][output_column] = data["value"]
            else:
                self.data[data["date"]] = {output_column:data["value"]}


class BLSData(SqlObject):
    """
    Gathers data from BLS API Industry Productivity data set and stores into MySQL
    BLS API only allows 500 requests per day. Each call allows for 50 data series' worth of data
    with a max date range of 20 years. IndustryProductivity dataset uses 2012 NAICS classification, 
    but expects to migrate to the 2017 NAICS classification system in 2020
    """

    __slots__ = ["table","data","dimension_list","metric_list","_db_line","_insert_list","_cnx","_cursor","measure_codes","_companiesList","_series_id_list"]
    def __init__(self):
        self.measure_codes = {"L020":"laborCostMil","C020":"capitalCostMil","P020":"intermediateCostMil","M020":"combinedCostMil","W010":"employmentIndex","M000":"multifactorProductivity"}
        metrics = []
        for code in self.measure_codes:
            metrics.append(self.measure_codes[code])
        SqlObject.__init__(self,"ipmetrics",["industryCode","date"],metrics)                   
        self._companiesList = []
        self._series_id_list = []

    def execute(self):
        """Abstract function for gathering data from a source and storing it to a database"""
        total_api_calls=0
        for measureCode in self.measure_codes:
            self._construct_series_id_list(measureCode)
        chunked_list = self._chunk_list(self._series_id_list,50)
        for list in chunked_list:
            if total_api_calls < 500:
                data = self._blsGetData(list)
                self._bls_parse_data(data)
                total_api_calls += 1
            else:
               #log api limit reached here
               break
        self.recurse_parse()
        self._consolidate_gaps()

    def _consolidate_gaps(self):
        """Fills gaps in BLS data algebraicly. CombinedCostMil = LaborCostMil + capitalCostMil + intermediateCostMil"""
        self._cnx = mysql.connector.connect(**db_credentials)
        self._cursor = self._cnx.cursor()
        metrics = ["intermediateCostMil","laborCostMil","capitalCostMil","intermediateCostMil","laborCostMil"]
        index = 0
        query = "UPDATE %s SET CombinedCostMil = intermediateCostMil + laborCostMil + capitalCostMil" % self.table
        query += " WHERE intermediateCostMil IS NOT NULL AND laborCostMil IS NOT NULL AND capitalCostMil IS NOT NULL"
        self._cursor.execute(query)
        self._cnx.commit()
        print(query)
        while index < 3:
            query = "UPDATE %s SET %s = combinedCostMil - %s - %s" % (self.table, metrics[index], metrics[index+1], metrics[index+2]) 
            query += " WHERE combinedCostMil IS NOT NULL AND %s IS NOT NULL and %s IS NOT NULL" % (metrics[index+1], metrics[index+2])
            self._cursor.execute(query)
            self._cnx.commit()
            index += 1
            print(query)
        self._cursor.close()
        self._cnx.close()

    def _get_industry_codes(self):
        """Gets list of Industry Codes used to pull data from the BLS"""
        cnx=mysql.connector.connect(**db_credentials,buffered=True)
        cursor = cnx.cursor()
        #Query references internal table 
        query = "SELECT DISTINCT sectorCode, industryCode FROM ipref WHERE naicsCode12 >= 3000 AND naicsCode12 < 4000 AND industryCode IS NOT NULL;"        #app's scope only covers manufacturers
        cursor.execute(query)
        industry_code_list = cursor.fetchall()
        cursor.close()
        return industry_code_list

    def _chunk_list(self, list, maxlength):
        """chunks list into smaller lists of 50 to meet BLS API call limitations"""
        chunked_lists=[]
        current_list = []
        for entry in list:
            if len(current_list) < 50:
                current_list.append(entry)
            else:
                chunked_lists.append(copy(current_list))
                current_list = []
        chunked_lists.append(copy(current_list))
        return chunked_lists

    def  _construct_series_id_list(self,measureCode):
        """Constructs Lists of API signatures for BLS Industry Productivity API Calls"""
        ipuIndustry=self._get_industry_codes()
        for industryCode in ipuIndustry:
            seriesId = "IPU%s%s%s" % (str(industryCode[0]),str(industryCode[1]),measureCode)
            self._series_id_list.append(seriesId)

    def _blsGetData(self,series_id_list):
        """Calls BLS API and returns response"""
        header = {"Content-type":"application/json"}
        payload = json.dumps({
           "seriesid":series_id_list, 
           "startyear":'2006',
           "endyear":'2017',
           "catalog":False, 
           "calculations":False, 
           "annualaverage":False,
           "registrationkey":bls_key
           })
        response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',data=payload,headers=header)
        print(response.text)
        return json.loads(response.text)

    def _bls_parse_data(self,api_response, period="annual"):
        """Parses BLS API response"""
        dataset = []
        for result in api_response["Results"]["series"]:
            if result["data"] == []:
                continue
            series_id = result["seriesID"][4:11]
            measureCode =  result["seriesID"][11:15]
            if self.data.get(series_id) == None:
                self.data[series_id] = {}
            if period == "annual":
                for data in result["data"]:
                    date = "%s-01-01" % (data["year"])
                    if self.data[series_id].get(date) == None:
                        self.data[series_id][date] = {self.measure_codes[measureCode]:data["value"]}
                    else:
                        self.data[series_id][date][self.measure_codes[measureCode]] = data["value"]


class CompaniesData(SqlObject):
    """Gathers data from EODdata and SEC and stores into MySQL"""
    __slots__ = ["table","data","dimension_list","metric_list","_db_line","_insert_list","_cnx","_cursor","exchanges","directory"]
    def __init__(self):
        SqlObject.__init__(self,"companies",["symbol"],["name","sicCode"])
        self.exchanges = ["NYSE","NASDAQ"]
        self.directory = "C:\\Josh\\dataGather\\Data"

    def execute(self):
        """Abstract function for gathering data from a source and storing it to a database"""
        self._get_ticker_symbols()
        self._parse_ticker_symbols()
        self._crawl_sic_codes()
        self.recurse_parse()

    def _get_ticker_symbols(self):
        """
        Downloads files containing lists of ticker symbols on various exchanges and their associated names
        Using Selenium to avoid having to avoid having to forge security token inputs included in the login form on EOD data website.
        
        Chrome webdriver is used to take advantage of the fact that chrome uses a distinct file extention when a file is in the process
        of being downloaded, giving the program a convenient way to check if a file has finished downloading before moving on.
        """
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory" : self.directory,
            "download.prompt_for_download": "false",
            "download.directory_upgrade": "true"
        }
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(chrome_options=options)
        driver.get("http://www.eoddata.com/symbols.aspx")               
        element = driver.find_element_by_id("ctl00_cph1_ls1_txtEmail")
        element.send_keys(eod_credentials["user"])
        element = driver.find_element_by_id("ctl00_cph1_ls1_txtPassword")
        element.send_keys(eod_credentials["password"],Keys.RETURN)
        for exchange in self.exchanges:
            driver.get("http://www.eoddata.com/Data/symbollist.aspx?e=%s" % exchange)
            while os.path.exists(self.directory+"\\%s.txt" % exchange) == False:
                time.sleep(1)
        driver.close()
        return

    def _parse_ticker_symbols(self):
        """Process files gathered by _get_ticker_symbols()"""
        for files in os.listdir(path=self.directory):
            with open(self.directory+"\\"+files) as ticker_list:
                next(ticker_list)                                       #skip header row
                for line in ticker_list:
                    line = line[0:len(line)-1]                          #omit newline charachter
                    pair = line.split(sep="\t")
                    if re.search("^([A-z]{1,6}$)",pair[0]) is None:     #removes assets which are not common stocks from push to db.
                        continue
                    self.data[pair[0]]={"name":pair[1],"lastUpdated":date.today().isoformat(),"sicCode":None}
    
    def _crawl_sic_codes(self):
        """Crawls SEC website for SIC Codes"""
        session = HTMLSession()
        clean_up_list = []
        for symbol in self.data:
            try:
                webpage = session.get("https://www.sec.gov/cgi-bin/browse-edgar?CIK=%s&owner=exclude&action=getcompany" % (symbol))
                code = webpage.html.find(".identInfo>a",first=True).text
                code = code.split(sep=" ")[0]
                if re.search("^\d{4}$",code) != None:                   #Some companies have an SEC page but do not have an SIC code listed.
                    print(symbol)
                    self.data[symbol]["sicCode"] = code
                else:
                    clean_up_list.append(symbol)
            except:
                print(symbol + " Not Found")
                clean_up_list.append(symbol)
        self._clean_up_data(clean_up_list)

    def _clean_up_data(self,clean_up_list):
        for entry in clean_up_list:
            del self.data[entry]

companies = CompaniesData()
companies.execute()
fed = FedData()
fed.execute()
bls = BLSData()
bls.execute()