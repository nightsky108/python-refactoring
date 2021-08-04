from datetime import date, timedelta, datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver

from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC

import time
import random
import re
import os
from os import listdir

import boto3
from botocore.exceptions import ClientError
import pandas as pd

from config import get_config

s3 = boto3.client('s3')

class Browser:
    def __init__(self, account):

        self.days_back = 3
        self.start_date = datetime.now() - timedelta(days=1, hours=8)
        # self.start_date = datetime.strptime('2020-10-24', '%Y-%m-%d')
        self.marketplace_id = account['marketplace_id']
        self.seller_id = account['seller_id']
        self.account_name = account['name']   
        self.bucket_name = account['bucket_name']
        self.sc_name = account['name']
        try:
            os.system("taskkill /im chrome.exe /f")
            os.system("taskkill /im chromedriver.exe /f")
        except Exception as e:
            print(e)
        options = webdriver.ChromeOptions()
        # prefs = {'download.default_directory' : r'C:\Users\Administrator\Desktop\business_scraper\DetailSalesTrafficByChildItem\Truemark'}
        # options.add_experimental_option('prefs', prefs)
        options.add_argument(r'user-data-dir=C:\Users\Administrator\AppData\Local\Google\Chrome\User Data')
        options.add_argument('--profile-directory=Profile 1')
        chromedriver = "./chromedriver.exe"
        try:
            self.driver = webdriver.Chrome(executable_path=chromedriver,chrome_options=options)
        except Exception as e:
            print(e)

    def sign_in(self):
        try:
            self.driver.get('https://sellercentral.amazon.com/home')
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


    def close(self):
        try:
            self.driver.quit()
        except Exception as e:
            print(e)
            return False
        return True

    def download_report(self):
        try:
            time.sleep(random.uniform(10,15))
            self.driver.find_element_by_xpath("//kat-button[@type='button']").click()
            time.sleep(random.uniform(25,30))
            print('report downloaded')
            return True
        except Exception as e:
            print(e)
            return False

    def process_report(self, date_time=None):
        print('processing report')
        path = './DetailSalesTrafficByChildItem/'+self.account_name+'/'
        directory_list = listdir(path)
        print(directory_list)
        time.sleep(10)
        if date_time is None:
            date_time = self.date
        for file in directory_list:
            try:
                file_creation_time = (os.path.getctime(path+file))
                currnt_time = (time.time())
                print(currnt_time - file_creation_time)
                if (currnt_time - file_creation_time) < 600:
                    try:
                        print('new report found')
                        csv_input = pd.read_csv(path+file)
                        csv_input['date'] = date_time.strftime("%Y-%m-%d")
                        csv_input['seller_id'] = self.seller_id
                        csv_input['marketplace_id'] = self.marketplace_id
                        csv_input['sc_name'] = self.sc_name
                        # print(csv_input.columns.to_list())
                        csv_input.columns = ['parent_asin','child_asin','title','sessions','session_pct','page_views','page_views_pct','buy_box_pct','units_ordered','units_ordered_b2b','unit_session_pct','unit_sessino_pct_b2b','ordered_product_sales','ordered_product_sales_b2b','total_order_items','total_order_tiems_b2b','date','seller_id','marketplace_id','sc_name']
                        print(csv_input.columns.to_list())
                        # csv_input = csv_input[['parent_asin','child_asin','title','sessions','session_pct','page_views','page_views_pct','buy_box_pct','units_ordered','units_ordered_b2b','unit_session_pct','unit_sessino_pct_b2b','ordered_product_sales','ordered_product_sales_b2b','total_order_items','total_order_tiems_b2b','date','seller_id','marketplace_id','sc_name']]
                        # print(csv_input.columns.to_list())
                        print('sleeping')
                        csv_input = csv_input.replace({'%': ''}, regex=True)
                        csv_input = csv_input.replace({',': ''}, regex=True)
                        print('before order prod csv')
                        csv_input['ordered_product_sales'] = csv_input['ordered_product_sales'].str.replace('$', '')
                        csv_input['ordered_product_sales_b2b'] = csv_input['ordered_product_sales_b2b'].str.replace('$', '')
                        csv_input['session_pct'] = csv_input['session_pct'].astype(float) 
                        csv_input['page_views_pct'] = csv_input['page_views_pct'].astype(float) 
                        csv_input['buy_box_pct'] = csv_input['buy_box_pct'].astype(float)
                        csv_input['unit_session_pct'] = csv_input['unit_session_pct'].astype(float) 
                        print('before csv')
                        csv_input.to_csv(path+date_time.strftime("%Y-%m-%d")+'.csv', index=False)
                    except Exception as e:
                        print(e)
                        break
                    with open(path+self.date.strftime("%Y-%m-%d")+'.csv', 'rb') as upload_file:
                        filename = str('DetailSalesTrafficByChildItem/')+str(self.account_name)+'/'+str(self.date.strftime('%Y-%m-%d'))+'.csv'
                        processed = s3.upload_fileobj(upload_file,"optivations-sc-data", str(filename))
                        print(filename)
                        self.remove_file(path, self.date.strftime("%Y-%m-%d")+'.csv')
                        self.remove_file(path, file)
                    return True
            except Exception as e:
                print(e)
                break
        return False
    
    def remove_file(self, path, file):
        try:
            os.remove(path+file)
        except Exception as e:
            print(e)
            return False
        return True
    

    def go_to_reports(self, from_date, to_date=date.today()):
        self.date = to_date
        time.sleep(5)
        print('going to report section')
        try:
            self.driver.find_element_by_id("sc-logo-asset").click()
            time.sleep(random.uniform(1,2))
            self.driver.find_element_by_link_text("Business Reports").click()
            time.sleep(random.uniform(1,2))
            # b.driver.find_element_by_xpath("//div[@id='root']/div/div/kat-box/div[8]/kat-link/a/span").click()
            sales_by_sku = wait(b.driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//div[@id='root']/div/div/kat-box/div[10]/kat-link/a/span")))
            sales_by_sku.click()
            time.sleep(random.uniform(5,9))
        except Exception as e:
            print (e)

        try:
            actions = ActionChains(self.driver)
            time.sleep(random.uniform(1,2))
            actions.send_keys(Keys.TAB*3).perform()
            time.sleep(random.uniform(1,2))
            
            actions = ActionChains(self.driver)
            time.sleep(random.uniform(1,2))
            actions.send_keys(from_date.strftime("%m/%d/%Y")).perform()
            time.sleep(random.uniform(1,2))

            actions = ActionChains(self.driver)
            time.sleep(random.uniform(1,2))
            actions.send_keys(Keys.TAB*1).perform()
            time.sleep(random.uniform(4,5))
            url = str(self.driver.current_url)

            url_pre = url.split("toDate")[0]
            url = url_pre + 'toDate=' + to_date.strftime('%Y-%m-%d')
            print(url)
            time.sleep(random.uniform(4,5))
            self.driver.get(url)
            time.sleep(random.uniform(4,5))
            self.driver.get(url)
            time.sleep(random.uniform(4,5))

        except Exception as e:
            print(e)

    def main(self, date_list):
        try:
            b.sign_in()
        except Exception as e:
            print(e)
            return False
        for date_time in date_list:
            b.go_to_reports(datetime.strptime(date_time["from_time"], '%Y-%m-%d'), 
                datetime.strptime(date_time["to_time"], '%Y-%m-%d'))            
            downloaded = b.download_report()
            if downloaded == False:
                downloaded = b.download_report()
            if downloaded == False:
                downloaded = b.download_report()
            if downloaded == True:
                b.process_report()
        b.close()

if __name__ == "__main__":

    try:
        b = Browser(get_config())
        date_list = []
        for d in range(0,b.days_back+1):
            b.date = (b.start_date - timedelta(d))
            date_list.append({"from_time" : b.date.strftime("%Y-%m-%d"), "to_time" : b.date.strftime("%Y-%m-%d")})
        print(date_list)
        b.main(date_list)
    except Exception as e:
        raise
