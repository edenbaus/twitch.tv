#!/home/scott/genv/bin/python
# -*- coding: utf-8 -*- 

# data cleaning, html parsing, and date/time management libraries
import numpy as np
import pandas as pd
import requests
import html5lib
import time
import datetime as dt
import pytz

# Selenium & BeautifulSoup scraping/web browser automation libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# BigQuery Pandas
from pandas.io import gbq


# virtual display libraries for headless scraping
from pyvirtualdisplay import Display
display = Display(visible=0,size=(1280,1024))
display.start()


def scrape_batch():

    def load_more_results(elem):
        elem.send_keys(Keys.PAGE_DOWN)
        time.sleep(3)
        elem.send_keys(Keys.PAGE_DOWN)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source,'html5lib')
        return soup

    def list_views(soup):
        return [entry for entry in soup.findAll('div',{'data-target':'directory-container'})[0].text.split('viewers')[:-1]]

    
    print "Initializing Selenium"
    url = 'https://www.twitch.tv/directory'
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(2)
    print "Setting Local Time as Batch..."
    scrape_time = dt.datetime.strptime(
                        str(dt.datetime.now(pytz.timezone('US/Eastern')) \
                            .replace(second=0, microsecond=0))[:~5],'%Y-%m-%d %H:%M:%S')
    print "Batch: {}".format(str(scrape_time))
    time.sleep(1)
    print 'Initializing Scraping Variables'
    # initialize scraping vars
    soup = BeautifulSoup(driver.page_source,'html5lib')
    elem = driver.find_element_by_partial_link_text("Fortnite")
    time.sleep(1)
    results = list_views(soup)
    last_game = results[~0]
    
    print 'Beginning Scraping loop'
    # scrape data
    count = 1
    R = 0
    while R < len(list_views(load_more_results(elem))):
        print 'Grabbing batch {}: > {} Games'.format(count,R)
        count += 1
        soup = load_more_results(elem)
        results = list_views(soup)
        print results[~0]
        R = len(results)
    print 'Processing Results'
    # sort results
    results_dic={}
    j=0
    for i in range((len(soup.findAll('div',{'class':'tw-mg-t-05'})))):
        if i % (R/10) == 0:
            print "Processing... {}%".format(j)
            j+=10

        results_dic[soup.findAll('div',{'class':'tw-mg-t-05'})[i].h3['title']] = \
            soup.findAll('div',{'class':'tw-mg-t-05'})[i].p['title']   
        
    print "Creating Pandas DataFrame"    
    results_df = pd.Series(results_dic).reset_index().rename(columns={'index':'game',0:'views'})
    results_df['views'] = results_df['views'].map(lambda x: x.replace(',','').split('viewer')[0].strip())
    results_df['views'] = results_df['views'].astype(int)
    results_df = results_df.sort_values(by=['views'],ascending=[0])
    results_df['batch'] = scrape_time
    print "Inserting results to BigQuery"
    gbq.to_gbq(results_df,'twitch.scrape','edenbaus',if_exists='append')
    
def main():
    scrape_batch()
    
if __name__ == "__main__": 
    main()
