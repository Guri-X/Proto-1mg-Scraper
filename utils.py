from bs4 import BeautifulSoup
import json
import re

meds_json_exp = re.compile(r"window.__INITIAL_STATE__ = (\{.*\});")
json_exp = re.compile(r"\{.*\}")

def soup_html_parser(html_str):
    return BeautifulSoup(html_str, 'html.parser')

def get_scripttag_json_obj(session, url):
    resp_obj = session.get(url)
    resp_obj = resp_obj.content
    soup_obj = soup_html_parser(resp_obj)
    open("scrape_healthcare_devices.html", "w+").write(str(soup_obj))
    
    scripts_obj = soup_obj.find_all('script')
    script_obj = list(filter(meds_json_exp.search, [str(script) for script in scripts_obj]))

    if script_obj:
        script_obj = script_obj[0]
        json_m = json_exp.search(script_obj).group()
        json_m = json.loads(json_m)

        return json_m
    else:
        print("No Script Tags Found")
        return None

def fetch_pdp_dynamic_reducer(data, *args):
    discount_price, mrp_price, purchase_count, view_count, other_text, variants_count, variants_list = args
    pdp_data = data.get("data")
    if pdp_data:
        care_plan_info_v2 = pdp_data.get("care_plan_info_v2")
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

        price_key = pdp_data.get("price")
        if (price_key) and (mrp_price == 0):
            mrp_price = price_key

        price_with_symbol = pdp_data.get("priceWithSymbol")
        if (price_with_symbol) and (mrp_price == 0):
            mrp_price = float(price_with_symbol.replace("\u20b9", ""))

        discount_with_symbol = pdp_data.get("discountWithSymbol")
        if (discount_with_symbol) and (discount_price == 0):
            discount_val = discount_with_symbol.get("price")
            if discount_val:
                discount_price = float(discount_val.replace("\u20b9", ""))

        social_cue = pdp_data.get("social_cue")
        if social_cue:
            display_text = social_cue.get("display_text").split()
            if "bought" in display_text:
                purchase_count = display_text[0]
            elif "viewed" in display_text:
                view_count = display_text[0]
            else:
                other_text = " ".join(display_text)

        variants = pdp_data.get("variants")
        if variants:
            values = variants.get("values")
            for variant in values:
                display_text = variant.get("display_text")
                variants_list.append(display_text)

        if variants_list:
            variants_count = len(variants_list)

    return discount_price, mrp_price, purchase_count, view_count, other_text, variants_count, variants_list

def get_recently_bought_data(data, *args):
    purchase_count, view_count, other_text = args
    ratings_overview_data = data.get("ratingsOverviewData")

    if ratings_overview_data:
        social_disclaimer = ratings_overview_data.get("socialDisclaimer")
        if social_disclaimer:
            sub_text = social_disclaimer.get("subText").split()
            if ("bought" in sub_text) and (purchase_count == 0):
                purchase_count = sub_text[0]
            elif ("viewed" in sub_text) and (view_count == 0):
                view_count = sub_text[0]
            else:
                other_text = " ".join(sub_text)

    return purchase_count, view_count, other_text

def get_price_box_data(data, *args):
    discount_price, mrp_price = args
    price_box = data.get("priceBox")
    if price_box:
        price_list = price_box.get("priceList")
        if price_list:
            price_list = price_list[0]
            discount_dict = price_list.get("discount")
            mrp_dict = price_list.get("mrp")
            
            if (discount_dict is not None) and (discount_price == 0):
                discount_price = float(discount_dict.get("price").replace("\u20b9", ""))
            
            if (mrp_dict is not None) and (mrp_price == 0):
                mrp_price = float(mrp_dict.get("price").replace("\u20b9", ""))

    return discount_price, mrp_price

def get_variants_data(data, *args):
    variants_count, variants_list = args
    sel_data = data.get("lowEmphasisSingleSelectionData")
    if sel_data:
        sel_data = sel_data[0]
        variants_data = sel_data.get("variantsData")
        if not variants_list:
            for variant in variants_data:
                variants_list.append(variant.get("ctaLabel"))

    if variants_list:
        variants_count = len(variants_list)

    return variants_count, variants_list

def get_otc_reducer_data(data, *args):
    purchase_count, view_count, reviews_count, other_text = args
    social_cue = data.get("social_cue")
    total_reviews = data.get("total_reviews")

    if social_cue:
        otcr_display_text = social_cue.get("display_text").split()
        if ("bought" in otcr_display_text) and (purchase_count == 0):
            purchase_count = otcr_display_text[0]
        elif ("viewed" in otcr_display_text) and (view_count == 0):
            view_count = otcr_display_text[0]
        else:
            other_text = " ".join(otcr_display_text) 

    if total_reviews:
        reviews_count = total_reviews

    return purchase_count, view_count, reviews_count, other_text

def get_otc_reducer_new_data(data, *args):
    reviews_count = args
    ratings_reviews_data = data.get("ratingsAndReviewsData")
    if ratings_reviews_data:
        reviews_count = ratings_reviews_data.get("reviewsCount")

    return reviews_count