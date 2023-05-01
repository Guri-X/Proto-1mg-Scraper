import json
import sys
import pandas as pd
import csv
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time
from selenium.common.exceptions import WebDriverException
from pyvirtualdisplay import Display

display = Display(visible=0, size=(1920, 1080))
display.start()

class OneMGScraper:
    def __init__(self, **kwargs):
        self.urls_file_name = kwargs.get("urls_file_name")
        self.category_url = kwargs.get("category_url")
        self.total_pages = kwargs.get("total_pages")
        self.headers_list = ["Page", "URL"]
        self.website_url = "https://www.1mg.com/"

        self.create_urls_file()

    def get_previous_page_num(self):
        if os.path.exists(self.urls_file_name):
            data_df = pd.read_csv(self.urls_file_name)
            data_df = data_df.tail(1)

            if not data_df.empty:
                page_num = data_df.iloc[0]["Page"]
            else:
                page_num = 0

            return page_num
        else:
            return 0

    def create_urls_file(self):
        if not os.path.exists(self.urls_file_name):
            with open(self.urls_file_name, "w+") as csvfile:
                csv_exists = csv.DictReader(csvfile).fieldnames
                if not csv_exists:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.headers_list)

    def get_urls(self):
        while True:
            try:
                driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"))
                driver.maximize_window()

                start_with = self.get_previous_page_num() + 1

                for page_num in range(start_with, self.total_pages+1):
                    print(f"Scraping URLs for page: {page_num}")
                    driver.get(f"{self.website_url}{self.category_url}?filter=true&pageNumber={page_num}")

                    elems = driver.find_elements(by=By.XPATH, value="//a[@href]")
                    for elem in elems:
                        href_link = elem.get_attribute("href")
                        if "otc" in href_link:
                            map_dict = {
                                "Page": page_num,
                                "Url": href_link
                            }
                            pd.DataFrame(map_dict, index=[0]).to_csv(self.urls_file_name, mode="a", header=False, index=False)

                    time.sleep(2)
                # display.stop()
            except WebDriverException:
                print("WebDriverException Error Occured, Sleeping For 10 Seconds")
                time.sleep(10)

if __name__ == "__main__":
    category_name = sys.argv[1]
    category_url = sys.argv[2]
    total_pages = sys.argv[3]
    urls_file_name = f"{category_name}_urls.csv"

    if total_pages.isdigit():
        total_pages = int(total_pages)
    else:
        print("Total pages must be an integer")
        sys.exit()
    
    oms = OneMGScraper(category_url=category_url, urls_file_name=urls_file_name, total_pages=total_pages) # categories/fitness-supplements-5
    oms.get_urls()