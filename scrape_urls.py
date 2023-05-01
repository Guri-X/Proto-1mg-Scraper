import requests
import re
import sys
import json
import pandas as pd
import os
import csv
import string
from utils import soup_html_parser
from urllib import parse

class OneMGScraper:
    def __init__(self, **kwargs):
        self.urls_file_name = kwargs.get("urls_file_name")
        self.session = requests.Session()
        self.meds_json_exp = re.compile(r"window.__INITIAL_STATE__ = (\{.*\});")
        self.json_exp = re.compile(r"\{.*\}")
        self.website_url = "https://www.1mg.com"
        self.headers_list = ["Label", "Page", "Url"]
        self.labels_list = list(string.ascii_lowercase)

        self.create_urls_file()

    def get_previously_fetched_data(self):
        if os.path.exists(self.urls_file_name):
            prev_data = pd.read_csv(self.urls_file_name).tail(1)

    def create_urls_file(self):
        if not os.path.exists(self.urls_file_name):
            with open(self.urls_file_name, "w+") as csvfile:
                csv_exists = csv.DictReader(csvfile).fieldnames
                if not csv_exists:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.headers_list)

    def save_medicine_urls(self, meds_url_data):
        pd.DataFrame(meds_url_data).to_csv(self.urls_file_name, mode="a", header=False, index=False)

    def fetch_medicine_urls(self):
        self.labels_list.remove("a")
        for label in self.labels_list:
            print(f"Scraping '{label.upper()}' Alphabet URLs")
            req_url = f"/drugs-all-medicines?page=1"
            while True:
                page_num = parse.parse_qs(parse.urlparse(req_url).query)['page'][0]
                if not parse.parse_qs(parse.urlparse(req_url).query).get("label"):
                    req_url = req_url + f"&label={label}"
                
                resp_obj = self.session.get(f"{self.website_url}{req_url}")
                resp_obj = resp_obj.content

                soup_obj = soup_html_parser(resp_obj)
                scripts_obj = soup_obj.find_all('script')
                script_obj = list(filter(self.meds_json_exp.search, [str(script) for script in scripts_obj]))

                if not script_obj:
                    print("No Script Tags Found")
                    sys.exit()

                script_obj = script_obj[0]
                json_m = self.json_exp.search(script_obj).group()
                json_m = json.loads(json_m)

                shell_reducer = json_m.get('shellReducer')
                meta_data = shell_reducer.get("meta_data")
                next_ = meta_data.get("next")
                schema = shell_reducer.get('schema').get("schema")
                meds_list = schema.get('itemListElement')
                
                meds_url_data = []
                for meds in meds_list:
                    map_dict = {
                        "label": label,
                        "page": page_num,
                        "url": meds.get('url')
                    }
                    meds_url_data.append(map_dict)
                self.save_medicine_urls(meds_url_data)

                if next_:
                    req_url = next_
                else:
                    break

if __name__ == "__main__":
    urls_file_name = "medicine_urls.csv"
    oms = OneMGScraper(urls_file_name=urls_file_name)
    oms.fetch_medicine_urls()