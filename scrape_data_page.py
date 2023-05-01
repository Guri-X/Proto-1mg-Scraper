from distutils.log import INFO
import json
import sys
from utils import soup_html_parser
import pandas as pd
import csv
import os
import requests
import re
import time
import concurrent.futures as cf
import numpy as np

class OneMGScraper:
    def __init__(self, **kwargs):
        self.urls_file_name = kwargs.get("urls_file_name")
        self.first_file_name = kwargs.get("first_file_name")
        self.second_file_name = kwargs.get("second_file_name")
        self.session = requests.Session()
        self.meds_json_exp = re.compile(r"window.__INITIAL_STATE__ = (\{.*\});")
        self.json_exp = re.compile(r"\{.*\}")
        self.headers_list = ["Name", "URL", "Is Prescription Required", "Salt Composition", "Manufacturer Name", "Discount Price", "MRP Price", "Recent Purchase",
                             "Variants", "Is Sold Out", "Is Discontinued", "Is Banned For Sale", "Is Not For Sale", "Is Scraped"]
        self.remaining_urls = []
        self.threads = 15

        self.create_urls_file()
        self.get_saved_urls()

    def get_saved_urls(self):
        first_out_file = pd.read_csv(self.first_file_name)["URL"].to_list()
        second_out_file = pd.read_csv(self.second_file_name)["URL"].to_list()
        out_file = first_out_file + second_out_file

        urls_file_df = pd.read_csv(self.urls_file_name)
        print(urls_file_df.shape)
        urls_file_df = urls_file_df[~urls_file_df["Url"].isin(out_file)]
        print(urls_file_df.shape)

        self.remaining_urls = urls_file_df["Url"].to_list()

    def create_urls_file(self):
        if not os.path.exists(self.second_file_name):
            with open(self.second_file_name, "w+") as csvfile:
                csv_exists = csv.DictReader(csvfile).fieldnames
                if not csv_exists:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.headers_list)

    def get_json_obj(self, url):
        resp_obj = self.session.get(url)
        resp_obj = resp_obj.content
        soup_obj = soup_html_parser(resp_obj)
        
        scripts_obj = soup_obj.find_all('script')
        script_obj = list(filter(self.meds_json_exp.search, [str(script) for script in scripts_obj]))

        if script_obj:
            script_obj = script_obj[0]
            json_m = self.json_exp.search(script_obj).group()
            json_m = json.loads(json_m)

            return json_m
        else:
            print("No Script Tags Found")
            return None

    def get_fact_box_data(self, data, *args):
        info_text = args[0]
        fact_box = data.get("fact_box")
        if fact_box:
            values_list = fact_box[0].get("values")
            if values_list:
                value_dict = [dict_ for dict_ in values_list if dict_.get("headers") == "Contains"]
                if value_dict:
                    value_dict = value_dict[0]
                    info_text = value_dict.get("info_text")

        return info_text

    def get_recently_bought_data(self, data, *args):
        recent_purchase = args[0]
        sku = data.get("sku")
        if sku:
            summary = sku.get("summary")
            overview = sku.get("overview")
            if summary:
                social_cue_s = summary.get("social_cue")
                if social_cue_s:
                    recent_purchase = social_cue_s.get("display_text").split()[0]

            if overview:
                social_cue_o = overview.get("social_cue")
                if social_cue_o:
                    recent_purchase = social_cue_o.get("display_text").split()[0]

        return recent_purchase

    def get_price_box_data(self, data, *args):
        discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list = args
        price_box = data.get("priceBox")
        if price_box:
            price_list = price_box.get("priceList")
            if price_list:
                price_list = price_list[0]
                discount_dict = price_list.get("offerPrice")
                mrp_dict = price_list.get("mrp")
                tag = price_list.get("tag")
                
                if (discount_dict is not None) and (discount_price == 0):
                    discount_price = float(discount_dict.get("discountedPrice").replace("\u20b9", ""))
                
                if (mrp_dict is not None) and (mrp_price == 0):
                    mrp_price = float(mrp_dict.get("price").replace("\u20b9", ""))

                if tag:
                    label = tag.get("label")
                    if label:
                        if (label == "SOLD OUT") and (is_sold_out == ""):
                            is_sold_out = True

                        if (label == "BANNED FOR SALE") and (is_banned_for_sale == ""):
                            is_banned_for_sale = True

        variants = data.get("variants")
        if variants:
            for variant in variants:
                variant_d = variant.get("variant")
                if variant_d:
                    options = variant_d.get("subHeader")
                    variant_data = [dict_.get("ctaLabel") for dict_ in variant_d.get("variantsData")]

                    variants_dict = {options: variant_data}
                    variants_list.append(variants_dict)

        return discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list

    def fetch_medicine_details(self, urls_list):
        for i,url in enumerate(urls_list):
            while True:
                try:
                    print(f"Scraping Data For {url}")
                    manufacturer_name = info_text = is_sold_out = is_discontinued = is_banned_for_sale = is_not_for_sale = ""
                    discount_price = recent_purchase = mrp_price = 0
                    variants_list = []
                    
                    json_m = self.get_json_obj(url)
                    if json_m is not None:
                        shell_reducer = json_m.get("shellReducer")
                        schema = shell_reducer.get("schema")
                        drug = schema.get("drug")
                        
                        name = drug.get("name")
                        is_prescription_req = drug.get("prescriptionStatus")
                        manufacturer = drug.get("manufacturer")
                        if manufacturer:
                            manufacturer_name = manufacturer.get("legalName")
                        active_ingredient = drug.get("activeIngredient")
                        drug_unit = drug.get("drugUnit")

                        pdp_dynamic_reducer = json_m.get("pdpDynamicReducer")
                        drug_page_reducer = json_m.get("drugPageReducer")
                        drug_page_reducer_v2 = json_m.get("drugPageReducerV2")

                        if pdp_dynamic_reducer:
                            data = pdp_dynamic_reducer.get("data")
                            if data:
                                care_plan_info_v2 = data.get("care_plan_info_v2")
                                if care_plan_info_v2:
                                    price_data = care_plan_info_v2.get("data")
                                    if price_data:
                                        price_data = price_data[0]
                                        discount_dict = price_data.get("discount")
                                        mrp_dict = price_data.get("mrp")
                                        
                                        if (discount_dict is not None) and (discount_price == 0):
                                            discount_price = discount_dict.get("price")
                                        
                                        if (mrp_dict is not None) and (mrp_price == 0):
                                            mrp_price = mrp_dict.get("price")

                                price_key = data.get("price")
                                if (price_key) and (mrp_price == 0):
                                    mrp_price = price_key

                                price_with_symbol = data.get("priceWithSymbol")
                                if (price_with_symbol) and (mrp_price == 0):
                                    mrp_price = float(price_with_symbol.replace("\u20b9", ""))

                                discount_with_symbol = data.get("discountWithSymbol")
                                if (discount_with_symbol) and (discount_price == 0):
                                    discount_val = discount_with_symbol.get("price")
                                    if discount_val:
                                        discount_price = float(discount_val.replace("\u20b9", ""))

                                unavailable_d = data.get("unavailable")
                                if unavailable_d:
                                    label_un = unavailable_d.get("label")
                                    if (label_un == "DISCONTINUED") and (is_discontinued == ""):
                                        is_discontinued = True

                                    if (label_un == "NOT FOR SALE") and (is_not_for_sale == ""):
                                        is_not_for_sale = True

                                out_of_stock_d = data.get("out_of_stock")
                                if out_of_stock_d:
                                    label_d1 = out_of_stock_d.get("label")
                                    if (label_d1 == "SOLD OUT") and (is_sold_out == ""):
                                        is_sold_out = True

                        if drug_page_reducer:
                            data = drug_page_reducer.get("data")
                            if data:
                                info_text = self.get_fact_box_data(data, info_text)
                                recent_purchase = self.get_recently_bought_data(data, recent_purchase)

                        if drug_page_reducer_v2:
                            static_data = drug_page_reducer_v2.get("staticData")
                            dyanmic_data = drug_page_reducer_v2.get("dynamicData")

                            if dyanmic_data:
                                discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list = self.get_price_box_data(dyanmic_data, discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list)
                                recent_purchase = self.get_recently_bought_data(dyanmic_data, recent_purchase)
                                info_text = self.get_fact_box_data(dyanmic_data, info_text)

                            if static_data:
                                discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list = self.get_price_box_data(static_data, discount_price, mrp_price, is_sold_out, is_banned_for_sale, variants_list)
                                recent_purchase = self.get_recently_bought_data(static_data, recent_purchase)
                                info_text = self.get_fact_box_data(static_data, info_text)

                        if (active_ingredient is not None) and (drug_unit is not None) and (info_text == ""):
                            info_text = f"{active_ingredient} ({drug_unit})"

                        if discount_price == 0:
                            discount_price = mrp_price

                        map_dict = {
                            "name": name,
                            "url": url,
                            "is_prescription_req": is_prescription_req,
                            "info_text": info_text,
                            "manufacturer_name": manufacturer_name,
                            "discount_price": discount_price,
                            "mrp_price": mrp_price,
                            "recent_purchase": recent_purchase,
                            "variants": json.dumps(variants_list),
                            "is_sold_out": is_sold_out,
                            "is_discontinued": is_discontinued,
                            "is_banned_for_sale": is_banned_for_sale,
                            "is_not_for_sale": is_not_for_sale,
                            "is_scraped": "Scraped"
                        }
                    else:
                        map_dict = {
                            "name": "",
                            "url": url,
                            "is_prescription_req": "",
                            "info_text": "",
                            "manufacturer_name": "",
                            "discount_price": "",
                            "mrp_price": "",
                            "recent_purchase": "",
                            "variants": "",
                            "is_sold_out": "",
                            "is_discontinued": "",
                            "is_banned_for_sale": "",
                            "is_not_for_sale": "",
                            "is_scraped": "Not Scraped"
                        }
                    pd.DataFrame(map_dict, index=[i]).to_csv(self.second_file_name, mode="a", header=False, index=False)
                except requests.exceptions.ConnectionError:
                    print("Connection Error")
                    time.sleep(5)
                break

    def fetch_data(self):
        threads = []
        nested_urls_list = np.array_split(self.remaining_urls, self.threads)

        with cf.ThreadPoolExecutor(max_workers=self.threads) as executor:
            for urls_list in nested_urls_list:
                threads.append(executor.submit(self.fetch_medicine_details, urls_list))
            
            for t in threads:
                if t.exception():
                    print(t.exception())
                    print(t.result())

    def test(self):
        url = "https://www.1mg.com/otc/rubired-suspension-otc164444"
        json_m = self.get_json_obj(url)
        open("details.json", "w+").write(json.dumps(json_m))

        # drug_page_reducer_v2 = json_m.get("drugPageReducerV2")
        # dyanmic_data = drug_page_reducer_v2.get("dynamicData")
        # if dyanmic_data:
        #     self.get_price_box_data(dyanmic_data)

if __name__ == "__main__":
    urls_file_name = "medicine_urls.csv"
    first_file_name = "medicine_data_1.csv"
    second_file_name = "medicine_data_2.csv"

    oms = OneMGScraper(urls_file_name=urls_file_name, first_file_name=first_file_name, second_file_name=second_file_name)
    # oms.fetch_data()
    oms.test()