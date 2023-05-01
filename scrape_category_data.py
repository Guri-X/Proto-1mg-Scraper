import requests
import re
import sys
import json
import pandas as pd
import os
import csv
import string
from utils import (soup_html_parser, get_scripttag_json_obj, fetch_pdp_dynamic_reducer, get_recently_bought_data, get_price_box_data, get_variants_data,
                    get_otc_reducer_data, get_otc_reducer_new_data)
from urllib import parse
import time
import numpy as np
import concurrent.futures as cf

class OneMGScraper:
    def __init__(self, **kwargs):
        self.urls_file_name = kwargs.get("urls_file_name")
        self.output_file_name = kwargs.get("output_file_name")
        self.session = requests.Session()
        self.website_url = "https://www.1mg.com"
        self.headers_list = ["Name", "URL", "Brand", "Item Condition", "Manufacturer Name", "Average Rating", "Ratings Count", "Discount Price", "MRP Price", "Purchase Count", "View Count", "Reviews Count", "Other Text", "Variants Count", "Variants List", "Is Scrapped"]
        self.remaining_urls = []
        self.threads = 2

        self.create_urls_file()
        self.get_saved_urls()

    def create_urls_file(self):
        if not os.path.exists(self.output_file_name):
            with open(self.output_file_name, "w+") as csvfile:
                csv_exists = csv.DictReader(csvfile).fieldnames
                if not csv_exists:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.headers_list)

    def get_saved_urls(self):
        out_file = pd.read_csv(self.output_file_name)["URL"].to_list()

        urls_file_df = pd.read_csv(self.urls_file_name)
        print("Total Urls: ", urls_file_df.shape[0])
        urls_file_df = urls_file_df[~urls_file_df["URL"].isin(out_file)]
        print("Data To Be Scraped: ", urls_file_df.shape[0])

        self.remaining_urls = urls_file_df["URL"].to_list()

    def fetch_category_data(self):
        for i,url in enumerate(self.remaining_urls):
            while True:
                try:
                    print(f"Scraping Data For {url}")
                    manufacturer_name = other_text = ""
                    discount_price = mrp_price = purchase_count = view_count = variants_count = reviews_count = 0
                    rating_value = rating_count = 0
                    variants_list = []
                    
                    json_m = get_scripttag_json_obj(self.session, url)
                    # open(f"data_{i}.json", "w").write(json.dumps(json_m))
                    if json_m is not None:
                        shell_reducer = json_m.get("shellReducer")
                        schema = shell_reducer.get("schema")
                        product = schema.get("product")
                        drug = schema.get("drug")

                        if product:
                            name = product.get("name")
                            brand = product.get("brand")
                            item_condition = product.get("itemCondition")
                            manufacturer = product.get("manufacturer")
                            if manufacturer:
                                manufacturer_name = manufacturer.get("name")

                            aggregate_ratings = product.get("aggregateRating")
                            if aggregate_ratings:
                                rating_value = aggregate_ratings.get("ratingValue")
                                rating_count = aggregate_ratings.get("ratingCount")

                        if drug:
                            break

                        pdp_dynamic_reducer = json_m.get("pdpDynamicReducer")
                        if pdp_dynamic_reducer:
                            discount_price, mrp_price, purchase_count, view_count, other_text, variants_count, variants_list = fetch_pdp_dynamic_reducer(pdp_dynamic_reducer, discount_price, mrp_price, purchase_count, view_count, other_text, variants_count, variants_list)
                        
                        otc_reducer_new = json_m.get("otcReducerNew")
                        if otc_reducer_new:
                            otc_static_data = otc_reducer_new.get("staticData")
                            otc_dyanmic_data = otc_reducer_new.get("dynamicData")
                            otc_combined_data = otc_reducer_new.get("combinedData")

                            if otc_dyanmic_data:
                                discount_price, mrp_price = get_price_box_data(otc_dyanmic_data, discount_price, mrp_price)
                                purchase_count, view_count, other_text = get_recently_bought_data(otc_dyanmic_data, purchase_count, view_count, other_text)
                                variants_count, variants_list = get_variants_data(otc_dyanmic_data, variants_count, variants_list)
                                reviews_count = get_otc_reducer_new_data(otc_combined_data, reviews_count)

                            if otc_static_data:
                                discount_price, mrp_price = get_price_box_data(otc_static_data, discount_price, mrp_price)
                                purchase_count, view_count, other_text = get_recently_bought_data(otc_static_data, purchase_count, view_count, other_text)
                                variants_count, variants_list = get_variants_data(otc_static_data, variants_count, variants_list)
                                reviews_count = get_otc_reducer_new_data(otc_combined_data, reviews_count)

                        otc_reducer = json_m.get("otcReducer")
                        if otc_reducer:
                            data = otc_reducer.get("data")
                            if data:
                                purchase_count, view_count, reviews_count, other_text = get_otc_reducer_data(data, purchase_count, view_count, reviews_count, other_text)

                        # drug_page_reducer_v2 = json_m.get("drugPageReducerV2")
                        # if drug_page_reducer_v2:
                        #     static_data = drug_page_reducer_v2.get("staticData")
                        #     dyanmic_data = drug_page_reducer_v2.get("dynamicData")

                        #     if dyanmic_data:
                        #         # discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list = self.get_price_box_data(dyanmic_data, discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list)
                        #         purchase_count, view_count, other_text = get_recently_bought_data(dyanmic_data, purchase_count, view_count, other_text)

                        #     if static_data:
                        #         # discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list = self.get_price_box_data(static_data, discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list)
                        #         purchase_count, view_count, other_text = get_recently_bought_data(static_data, purchase_count, view_count, other_text)

                        # if discount_price == 0:
                        #     discount_price = mrp_price

                        map_dict = {
                            "name": name,
                            "url": url,
                            "brand": brand,
                            "item_condition": item_condition,
                            "manufacturer_name": manufacturer_name,
                            "rating_value": rating_value,
                            "rating_count": rating_count,
                            "discount_price": discount_price,
                            "mrp_price": mrp_price,
                            "purchase_count": purchase_count,
                            "view_count": view_count,
                            "reviews_count": reviews_count,
                            "other_text": other_text,
                            "variants_count": variants_count,
                            "variants_list": json.dumps(variants_list),
                            "is_scraped": "Scraped"
                        }
                    else:
                        map_dict = {
                            "name": "",
                            "url": url,
                            "brand": "",
                            "item_condition": "",
                            "manufacturer_name": "",
                            "rating_value": "",
                            "rating_count": "",
                            "discount_price": "",
                            "mrp_price": "",
                            "purchase_count": "",
                            "view_count": "",
                            "reviews_count": "",
                            "other_text": "",
                            "variants_count": "",
                            "variants_list": "",
                            "is_scraped": "Not Scraped"
                        }
                    pd.DataFrame(map_dict, index=[i]).to_csv(self.output_file_name, mode="a", header=False, index=False)
                    time.sleep(1)
                except requests.exceptions.ConnectionError:
                    print("Connection Error")
                    time.sleep(5)
                break

    # def fetch_data(self):
    #     threads = []
    #     nested_urls_list = np.array_split(self.remaining_urls, self.threads)

    #     with cf.ThreadPoolExecutor(max_workers=self.threads) as executor:
    #         for urls_list in nested_urls_list:
    #             threads.append(executor.submit(self.fetch_category_data, urls_list))
            
    #         for t in threads:
    #             if t.exception():
    #                 print(t.exception())
    #                 print(t.result())

if __name__ == "__main__":
    urls_file_name = "ayurveda_urls.csv"
    output_file_name = "_".join(urls_file_name.split(".")[0].split("_")[:2]) + "_data.csv"
    oms = OneMGScraper(urls_file_name=urls_file_name, output_file_name=output_file_name)
    oms.fetch_category_data()