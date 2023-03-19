import scrapy
import requests
import re

class thirdscrapy(scrapy.Spider):
    name = "udacity_batch"
    start_urls = [
        "https://www.udacity.com/courses/all"
    ]

    def parse(self, response):
        l = "https://www.udacity.com/data/catalog.json?v=854825c4"
        yield scrapy.Request(url=l, callback=self.json_contents)

    def json_contents(self, response):
        data = response.json()
        links = []
        for i in data:
            if i["type"] == "course":
                links.append("https://www.udacity.com" + i["url"])

        for link in links:
            discounted_price_link = "https://api.udacity.com/api/braavos/prices?anonymous_id=0ea6d6cc-dff8-47b7-8489-63c9e5ef2e59&coupon=BESTDEAL75&currency=INR&node_key=" + link.split("-")[-1]

            actual_price_link = "https://api.udacity.com/api/braavos/prices?anonymous_id=0ea6d6cc-dff8-47b7-8489-63c9e5ef2e59&currency=INR&node_key=" + link.split("-")[-1]

            yield scrapy.Request(discounted_price_link, callback=self.discounted_course_fees, cb_kwargs={"actual_price_link": actual_price_link, "link":link})


    def discounted_course_fees(self, response, actual_price_link, link):
        data2 = response.json()

        try:
            coupoun = data2["results"][0]["coupon"]["id"]
            if coupoun is not None:
                actual_emi = data2["results"][0]["price"]["original_amount_display"].replace("₹","").replace("INR","").replace(",","").strip()
                #print("actual_emi- ", actual_emi)
                discounted_emi = data2["results"][0]["price"]["payable_amount_display"].replace("₹","").replace("INR","").replace(",","").strip()
                #print("discounted_emi- ", discounted_emi)

                actual_upfront_amount = data2["results"][0]["payment_plans"]["upfront_recurring"]["upfront_subtotal_display"].replace("₹","").replace("INR","").replace(",","").strip()
                #print("actual_upfront_amount- ", actual_upfront_amount)
                discounted_actual_upfront_amount = data2["results"][0]["payment_plans"]["upfront_recurring"]["upfront_amount"]["payable_amount_display"].replace("₹","").replace("INR","").replace(",","").strip()
                #print("discounted_actual_upfront_amount- ", discounted_actual_upfront_amount)

                currency = "INR"

            else:
                yield scrapy.Request(actual_price_link, callback=self.actual_course_fees, cb_kwargs={"link": link})

        except:
            actual_emi = ""
            discounted_emi = ""
            actual_upfront_amount = ""
            discounted_actual_upfront_amount = ""
            currency = ""

        yield {
            "link": link,
            "actual_emi": actual_emi,
            "discounted_emi": discounted_emi,
            "actual_upfront_amount": actual_upfront_amount,
            "discounted_actual_upfront_amount": discounted_actual_upfront_amount,
            "Currency": currency
        }

    def actual_course_fees(self, response, link):
        data3 = response.json()

        try:
            actual_emi = data3["results"][0]['payment_plans']['upfront_recurring']["recurring_amount"]["payable_amount_display"].replace("₹","").replace("INR","").replace(",","").strip()
            print("actual_emi- ", actual_emi)

            actual_upfront_amount = data3["results"][0]["payment_plans"]["upfront_recurring"]["upfront_subtotal_display"].replace("₹","").replace("INR","").replace(",","").strip()
            print("actual_upfront_amount- ", actual_upfront_amount)

        except:
            actual_emi = ""
            actual_upfront_amount = ""
            currency = ""

        yield {
            "link": link,
            "actual_emi": actual_emi,
            "actual_upfront_amount": actual_upfront_amount,
            "Currency": currency
        }