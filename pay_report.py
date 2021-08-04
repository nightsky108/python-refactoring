#NEW PAYMENTS REPORT SCRAPER 2021

from datetime import date, timedelta, datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver

from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

import csv
import time
import random
import re
import os
from os import listdir
import glob 

import boto3
from botocore.exceptions import ClientError
import pandas as pd

from config import get_config

s3 = boto3.client('s3')

class Browser:
    def __init__(self, account):
        self.days_back = 3
        self.start_date = datetime.today() - timedelta(days=1, hours=8) 
        self.marketplace_id = account['marketplace_id']
        self.sc_name = account['name']
        self.seller_id = account['seller_id']
        self.account_name = account['name']        
        self.data = None
        try:
            os.system("taskkill /im chrome.exe /f")
            os.system("taskkill /im chromedriver.exe /f")
        except Exception as e:
            print(e)
        options = webdriver.ChromeOptions()
        options.add_argument(r'user-data-dir=C:\Users\Administrator\AppData\Local\Google\Chrome\User Data')
        options.add_argument('--profile-directory=Profile 1')
        chromedriver = "./chromedriver.exe"
        self.driver = webdriver.Chrome(executable_path=chromedriver,chrome_options=options)
  

    def sign_in(self):
        try:
            self.driver.get('https://sellercentral.amazon.com/home?')
            # time.sleep(random.randint(1,4))
            #check what page we are looking at 
            #IF SIGNED IN RETURN
            time.sleep(random.uniform(1,4))

            # self.driver.find_element_by_link_text("Select").click()
            # self.driver.find_element_by_id("ap_email").click()
            # self.driver.find_element_by_id("ap_email").clear()
            # self.driver.find_element_by_id("ap_email").send_keys("LOGIN HERE")

        except Exception as e:
            print(e)
            return False
        return True

    def open(self):
        try:
            self.driver.get('_blank')
        except Exception as e:
            print(e)
            return False
        return True

    def close(self):
        try:
            self.driver.quit()
        except Exception as e:
            print(e)
            return False
        return True

    def remove_duplicate_rows(self, start_datetime, end_datetime):

        conn = psycopg2.connect(dbname='dbname', host='host', port='5439', user='user', password='password')
        cur = conn.cursor()
        # Begin your transaction
        cur.execute("begin;")
        sql_delete = '''delete  from \
                        pay_report.date_range \
                        where seller_id = %(seller_id)s \
                        and marketplace_id = %(marketplace_id)s \
                        and report_datetime between %(start_datetime)s and %(end_datetime)s'''
        cur.execute(sql_delete, {"seller_id" : self.seller_id, 
                        "marketplace_id" : self.marketplace_id, 
                        "start_datetime" : start_datetime, 
                        "end_datetime" : end_datetime })
        cur.execute("commit;")
        print("pay report delete execution fine!")
        return None



    def process_report(self):
        path = './DetailSalesTrafficByChildItem/'+self.account_name+'/'
        list_of_files = glob.glob(path+'*csv') # * means all if need specific format then *.csv
        print(list_of_files)
        latest_file = max(list_of_files, key=os.path.getctime)
        print (latest_file)
        key = latest_file.split('\\')[-1]
        print(key)
        with open(latest_file, "r",encoding="utf8") as infile:
            reader = csv.reader(infile)
            print(type(reader))
            for index, row in enumerate(reader): 
                if row[0] == 'date/time':
                    header_index = index
        print('header index: ', header_index )
        csv_input = pd.read_csv(latest_file, header=header_index)
        csv_input['seller_id'] = self.seller_id
        csv_input['marketplace_id'] = self.marketplace_id
        csv_input['sc_name'] = self.sc_name
        csv_input['date/time']=csv_input['date/time'].apply(lambda x:datetime.strptime(str(str(x)[:20]).strip(), "%b %d, %Y %H:%M:%S"))
        # csv_input['date/time'] = datetime.strptime(csv_input['date/time'], '%a %d, %Y %H:%M:%S %p %Z')
        csv_input = csv_input.replace({',': ''}, regex=True)
        print(csv_input)
        min_date = min(csv_input['date/time'])
        max_date = max(csv_input['date/time'])
        self.remove_duplicate_rows(min_date, max_date)
        csv_input.to_csv(latest_file, index=False)
        with open(latest_file, 'rb') as upload_file:
            processed = s3.upload_fileobj(upload_file,"optivations-sc-data", 'payment_reports/'+self.account_name+'/'+key )
            self.remove_file(latest_file)
            return True
        return True

    
    def remove_file(self, file):
        try:
            os.remove(file)
        except Exception as e:
            print(e)
            return False
        return True
    
    def go_to_reports(self):
        time.sleep(8)
        # try:
        #     print('check marketplace')
        #     flag_class = wait(self.driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="sc-mkt-switcher-form"]/div[1]'))).get_attribute('class')
        #     if 'us-amazon-flag' in str(flag_class):
        #         self.marketplace_id = 'ATVPDKIKX0DER'
        #     if 'ca-amazon-flag' in str(flag_class):
        #         self.marketplace_id = 'A2EUQ1WTGCTBG2'
        # except Exception as e:
        #     print(e)
        print(self.marketplace_id)
        print('checking sc name')
        # try:
        #     time.sleep(13)
        #     self.sc_name = self.driver.find_element_by_class_name('sc-mkt-picker-switcher-txt').text
        #     print('sc name')
        #     print(self.sc_name)
        # except Exception as e:
        #     print(e)
        #     return False
        try:
            self.driver.get("https://sellercentral.amazon.com/gp/payments-account/settlement-summary.html/ref=xx_payments_dnav_xx")
            time.sleep(random.randint(1,4))
            self.driver.get("https://sellercentral.amazon.com/gp/payments-account/settlement-summary.html/ref=xx_payments_dnav_xx")
            time.sleep(random.uniform(15,16))
            self.driver.get("https://sellercentral.amazon.com/payments/reports/custom/request?ref_=xx_report_ttab_das")
            
            generate_button = wait(self.driver, 20).until(EC.element_to_be_clickable((By.ID, 'drrGenerateReportButton')))
            generate_button.click()
            time.sleep(4)
            submit_button = wait(self.driver, 20).until(EC.element_to_be_clickable((By.ID, 'drrReportRangeTypeRadioCustom')))
            submit_button.click()
            self.driver.find_element_by_id("drrFromDate").send_keys((self.start_date - timedelta(days=self.days_back)).strftime("%m/%d/%Y"))
            self.driver.find_element_by_id("drrToDate").send_keys(self.start_date.strftime("%m/%d/%Y"))
            actions = ActionChains(self.driver)
            actions.send_keys(Keys.TAB*2)
            actions.send_keys(Keys.RETURN)
            actions.perform()
            time.sleep(random.randint(7,12))
            return True 
        except Exception as e:
            print(e)
            return False
    def wait_for_report_to_refresh(self):
        try:
            print('waiting for report refresh')
            time.sleep(random.randint(2,4))
            while self.driver.find_element_by_link_text("Refresh").click() == None:
                time.sleep(4*self.days_back)
                print('refreshing again')
        except Exception as e:
            print(e)
            return False
        return True

    def download_report_pay(self):
        #TODO Add verification for table data
        time.sleep(random.uniform(12,13))
        # self.scroll(random.uniform(12,15))
        download_url = wait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="downloadButton"]')))
        time.sleep(random.randint(7,12))
        try:
            download_url.click()
            time.sleep(14)
        except Exception as e:
            print(e)
            download_url.click()
            time.sleep(14)
        return True
        #TODO set this sleep dependant on how many dates we are


    def main(self):
        time.sleep(random.uniform(11, 14))
        b.open()
        b.sign_in()
        generate_report = b.go_to_reports()
        print('generate' , generate_report)
        if generate_report == True:
            b.wait_for_report_to_refresh()
        downloaded = b.download_report_pay()
        if downloaded == True:
            b.process_report()            
        b.close()



if __name__ == "__main__":
    try:
        b = Browser(get_config())
        b.main()
    except Exception as e:
        raise
