import scrapy
import re
import json
import requests
import pandas as pd
from datetime import date, datetime
from google.oauth2 import service_account
import pygsheets
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
import logging

logging.basicConfig(
    filename='log.txt',
    format='%(levelname)s: %(message)s',
    level=logging.DEBUG,
)


class whizlabs_market_discount(scrapy.Spider):
    name = "whizlabs_market_discount"

    PartnerName = 'Whizlabs'

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': False
    }

    def errback_httpbin(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(f'HttpError on {response.url}')

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error(f'DNSLookupError on {request.url}')

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error(f'TimeoutError on {request.url}')

    df_coupons = pd.DataFrame(
        columns=['Partner Name', 'Link', 'Discount Percentage', 'Discount Price', 'Discount Price Currency',
                 'Coupon Code',
                 'Learn Type', 'Applicability', 'Discount Type', 'Course Specific', 'Course Link', 'Instruction Type','User Type',
                 'Category', 'Card', 'Validity', 'Price Condition', 'Price Limit', 'Condition', 'Description'])

    def start_requests(self):
        self.logger.info(f'--- {self.PartnerName} Scraper - [{self.name}]---')
        return [scrapy.FormRequest("https://www.grabon.in/whizlabs-coupons/", callback=self.grabon_whizlabs, errback = self.errback_httpbin)]


    def uploadData(self):
        credentials = {
          "type": "service_account",
          "project_id": "crawler-373907",
          "private_key_id": "0a1b9f1ba8710a24ec38a38c9734746395d51134",
          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCsTp9rrfpeNMPB\ncNdeC49ON/6Bb+TsU1QtcfMa/KB5iOhc8DwoVY4bU1qsIzJuophy8n4wbahKuH80\nBVrAe0HpsNNmwe+GCPHdfmwkxgNzaJb9D3Cl0G3g7LlJV4L5l6TVR0bga/dLeUi5\nEGEO/+SzODKG5N+Ubcc/bfoedksvoVGgjz5iiv1FuRVqzwuLIBR/Z6y8zbOu0nRf\nf7E56uNQ8TuuQIkRWMBqVy3wMxZ52Rr/KsH+A3Clie0foSaxpfHwBF3lMex2s9KQ\nYGfHWa8Fr4i+1kRVNwrLRGfQaR8O+Woh3BgiDxLEOpuJ319nD66SFd6+3/tk7UBc\ngGRvUkP5AgMBAAECgf9RYnwUytgMJwqkaOf7Q6UsYhuT3NtZGa1oyamfKt5L3p6M\nnJZ4IUY9zIvHQoNPk6eYo3DMRFR49oEee75MVpY2CuR5ufWobrvOzSfHNr/qgubO\nIGd/P4imnyk6ZbCMs1OSaVgmKC4QjU4MG0W8uqELBZWCoW1ObjZDf26OyiY5GEYl\n0YTpTGVYRbuxW2zcbcuSc/36h7fTHxSBEddcYVPvFGZqJX0Jgu7i7A/4kepzWZnd\nsgVmy4dFkehvmT9SB1ZX+yz0FfynX5ASiPQagM9ybudLh44HeOK4T8N7SvJBuOIJ\nAaldKuP5kpCjJiY5UzXyKS/ZbA31R0Qz8KbZajUCgYEA8IWAS72ZStYsEb7F0zwI\nCSudFGgU2KYUnbvGzW1VTs9wjGCD/BjnycdnEs3qClUEPZg7aF69AM9X+MC6ihN/\niur9l7LILKPfAXOGHAwULPnomtEr8R7C7BEjZ75WoHASq1GmU49OAq0BxkNqKmE/\ngmQiMJjEyDJp1+5WzplAph0CgYEAt2VQ7qEYkSPb48/WKH9kSLkQ2tAtDzY/XP1i\nadtJ2Eu19x9p1XKH9lLKz6v5MBeQ1HnkHwTCQzwj0AN/Ax5cDl6CDtkdezbPaxMe\nV8nUmUOK6Cb4ctiJHn8i+c3Z1jOgwoCyndtGnGdmRILfhFUVpd8u2x3vpiFmhv4L\njAye/o0CgYBZWK5M9HRySVb7jIt+KWmuxmXGg9dil2dHJM731qp+6S4c52mAB1xr\nJ0iRwq4zAlvd6aP/5Fl/aIZ8YFOrIQ0a9KoZZQ9ZDdK0fk6OMqp3/qm6gQM6wbuU\n59ToH9ucI6W7wEvx9GT9N3lWRgq1DYUDEeFlfgpzd867qMzUWDecvQKBgG/lHzYZ\nkFWt3VHn0zCuWU+Nqtz2uydW394qs0sHAs03lHSM9BPJZ29BIIEI2mcfWbxqCmdq\njeRb4zXdjDco5N3Xh97rVXOCA5e++HpzqIVCkpQGgvv/Zdn3lC16DXkF0wYZY+Gw\nkiqHY/xVJW8mQqkLvRjv8PIZ2uZuRjlS0gdZAoGAAaidr1ZQWJ1Y3eGqXMW46v+Y\nBBweh9/vDJl232doXNk2HMom7FGh3yYwFpjkuYP7RyHrJsus0ZcbZYDnBu5eMIwp\ncRP2iHajbRwja0YtWTjkghlodwIqdwxNwydPRc+HhJL669mcEda/ZRaKQ0uvT2pF\nEEI9IqfCzulv2gZa3jw=\n-----END PRIVATE KEY-----\n",
          "client_email": "mohitbhalke@crawler-373907.iam.gserviceaccount.com",
          "client_id": "102063114728923850130",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mohitbhalke%40crawler-373907.iam.gserviceaccount.com"
        }

        SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
        service_account_info = json.loads(json.dumps(credentials))
        my_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        gc = pygsheets.authorize(custom_credentials=my_credentials)

        sh = gc.open("Scrapped Partner Data - Market Discount")
        try:
            worksheet = sh.worksheet_by_title(self.PartnerName)
            worksheet.clear('*')
            worksheet.set_dataframe(self.df_coupons, (1, 1))
        except:
            worksheet = sh.add_worksheet(self.PartnerName)
            worksheet.set_dataframe(self.df_coupons, (1, 1))


    def grabon_whizlabs(self, response):
        data = response.xpath('.//div[@class="gc-box banko"]')

        for coupon in data:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Instruction Type': '',
                'User Type': '',  # New User/ All Users
                'Category': '',
                'Card': '',
                'Validity': '',  # Expiry Date
                'Price Condition': '',
                'Price Limit': '',
                'Condition': '',
                'Description': ''
            }
            availableOffers['Partner Name'] = self.PartnerName

            title = coupon.xpath('.//div[@class="gcbr go-cpn-show go-cpy"]//p/text()').extract_first()
            description = re.sub('<[^>]+>', '', coupon.xpath('.//div[@class="gcb-det"]').extract_first()).strip()

            if re.search(r'plus an extra [0-9]+% off| get an extra [0-9]+% off', description, re.IGNORECASE):
                temp = re.search(r'plus an extra [0-9]+% off| get an extra [0-9]+% off', description,
                                 re.IGNORECASE).group()
                availableOffers["Discount Percentage"] = re.search(r'[0-9]+%', temp, re.IGNORECASE).group()
            else:
                discount_per = coupon.xpath('.//div[@class="bank"]//span/text()').extract_first()
                if re.search(r'\d\d\d%|\d\d%|\d%|\$[0-9]+]', discount_per, re.IGNORECASE):
                    availableOffers["Discount Percentage"] = discount_per

            # spec_char = re.findall(r'Rs| ₹', title)
            # if spec_char != []:
            #     cour_title_split = title.split(spec_char[0])
            #     availableOffers["Discount Price"] = cour_title_split[1].strip()

            if availableOffers["Discount Percentage"] == "" and availableOffers["Discount Price"] == "":
                print("================>  Breaking")
                continue

            title_split1 = re.findall(r'Rs| ₹', title)
            title_split2 = title.lower().split("off on")

            if title_split1 != []:
                cour_title_split = title.split(title_split1[0])
                availableOffers["Course Specific"] = cour_title_split[0].replace("@", "").replace("- Purchase Course At", "").strip()

            elif len(title_split2) >= 2:
                print(title_split2)
                if re.search(r'\bnanodegrees', title_split2[1], re.IGNORECASE):
                    availableOffers["Category"] = title_split2[
                        1]  # .lower().replace("nanodegrees", "courses").replace("in udacity", "").replace("at udacity", "").replace("get a 7", "").strip()
                elif re.search(r'\bnanodegree', title_split2[1], re.IGNORECASE):
                    availableOffers["Course Specific"] = title_split2[
                        1]  # .lower().replace("nanodegrees","courses").replace("in udacity", "").replace("at udacity", "").replace("get a 7", "").strip()
                elif re.search(r'\bcourses', title_split2[1], re.IGNORECASE):
                    availableOffers["Category"] = title_split2[1]
                elif re.search(r'\bcourse', title_split2[1], re.IGNORECASE):
                    availableOffers["Course Specific"] = title_split2[1]

            if availableOffers["Discount Price"] != "":
                if re.search(r'rs|₹', title, re.IGNORECASE):
                    availableOffers["Discount Price Currency"] = "INR"
                if re.search(r'\$', title, re.IGNORECASE):
                    availableOffers["Discount Price Currency"] = "USD"

            availableOffers["Link"] = "https://www.grabon.in/coupon-codes/" + \
                                      re.search(r'cpn_[0-9]+', coupon.extract()).group().split("_")[1]

            availableOffers["Coupon Code"] = coupon.xpath('.//span[@class="visible-lg"]/text()').extract_first()
            if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                availableOffers["Coupon Code"] = "NOT REQUIRED"

            if availableOffers["Course Specific"] == "" and availableOffers["Category"] == "" and re.search(
                    r'all courses|all course|all certification|all certifications|sitewide', title, re.IGNORECASE):
                availableOffers["Applicability"] = "Sitewide"
            elif availableOffers["Course Specific"] == "" and availableOffers["Category"] == "" and re.search(
                    r'all courses|all course|all certification|all certifications|sitewide', description,
                    re.IGNORECASE):
                availableOffers["Applicability"] = "Sitewide"
            elif availableOffers["Category"] != "":
                availableOffers["Applicability"] = "Category"
            elif availableOffers["Course Specific"] != "":
                availableOffers["Applicability"] = "Course Specific"
            elif re.search(r'self paced|self-paced|self pace|self-pace', title, re.IGNORECASE):
                availableOffers["Applicability"] = "Learn Type"
            elif re.search(r'self paced|self-paced|self pace|self-pace', description, re.IGNORECASE):
                availableOffers["Applicability"] = "Learn Type"
            else:
                continue

            if availableOffers["Applicability"] != "Sitewide":
                if re.search(
                        r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',
                        title, re.IGNORECASE):
                    availableOffers['Learn Type'] = "Certification"
                    availableOffers['Instruction Type'] = "Self paced"
                elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                    availableOffers['Learn Type'] = "Master"
                    availableOffers['Instruction Type'] = "Instructor"

            if re.search(r'upto', title, re.IGNORECASE):
                availableOffers["Discount Type"] = "Upto"
            elif re.search(r'\d\d\d%|\d\d%|\d%', availableOffers["Discount Percentage"], re.IGNORECASE):
                availableOffers["Discount Type"] = "Flat"

            availableOffers["Description"] = description

            self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

        yield scrapy.Request('https://www.couponzguru.com/whizlabs-coupons/', self.couponzguru_whizlabs, errback = self.errback_httpbin)

        # yield self.df_coupons.to_csv("Grabon_whizlabs.csv", sep=",")


    def couponzguru_whizlabs(self, response):
        data2 = response.xpath('//div[@class="coupon-list"]')

        for coupon in data2:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Instruction Type': '',
                'User Type': '',  # New User/ All Users
                'Category': '',
                'Skills': '',
                'Card': '',
                'Validity': '',  # Expiry Date
                'Price Condition': '',
                'Price Limit': '',
                'Condition': '',
                'Description': ''
            }
            availableOffers['Partner Name'] = self.PartnerName

            title = coupon.xpath('.//h3//a/text()').extract_first()
            description = coupon.xpath('.//p[1]/text()').extract_first()

            discount_per = re.findall("\d\d\d%|\d\d%", title)
            if discount_per != []:
                availableOffers["Discount Percentage"] = discount_per[0]

            if availableOffers["Discount Percentage"] == "":
                print("================>  Breaking")
                continue

            title_split2 = title.lower().split("off on")

            if len(title_split2) >= 2:
                print(title_split2)
                if re.search(r'\bcourses', title_split2[1].replace("certification", ""), re.IGNORECASE):
                    availableOffers["Category"] = title_split2[1].lower().replace("all courses", "").replace("(across site)", "").replace("premium subscription plans", "").replace("- practice exams","").replace("practice exams","").strip()
                elif re.search(r'\bcourse', title_split2[1].replace("certification", ""), re.IGNORECASE):
                    availableOffers["Course Specific"] = title_split2[1].lower().replace("all courses", "").replace("(across site)", "").replace("premium subscription plans", "").replace("- practice exams","").replace("practice exams","").strip()
                elif re.search(r'\bcertifications', title_split2[1], re.IGNORECASE):
                    availableOffers["Category"] = title_split2[1].lower().replace("all courses", "").replace(
                        "(across site)", "").replace("premium subscription plans", "").replace("- practice exams",
                                                                                               "").replace(
                        "practice exams", "").strip()
                else:
                    availableOffers["Course Specific"] = title_split2[1].lower().replace("all courses", "").replace(
                        "(across site)", "").replace("premium subscription plans", "").replace("- practice exams",
                                                                                               "").replace(
                        "practice exams", "").strip()

            coupouns = coupon.xpath('.//span[@class=" btn clicktoreveal-code img-reponsive"]/text()').extract()
            availableOffers["Coupon Code"] = [i.strip() for i in coupouns][0]
            if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "Deal Activated" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                availableOffers["Coupon Code"] = "NOT REQUIRED"

            raw_link = coupon.xpath('.//div//a/@onclick').extract_first()
            try:
                availableOffers["Link"] = raw_link.split(";")[1].split()[2].replace("'", "").replace(",", "").replace(")", "")
            except:
                availableOffers["Link"] = coupon.xpath('.//a/@href').extract_first()

            if availableOffers["Course Specific"] == "" and availableOffers["Category"] == "" and re.search(
                    r'all courses|all course|all certification|all certifications|sitewide', title, re.IGNORECASE):
                availableOffers["Applicability"] = "Sitewide"
            elif availableOffers["Course Specific"] == "" and availableOffers["Category"] == "" and re.search(
                    r'all courses|all course|all certification|all certifications|sitewide', description,
                    re.IGNORECASE):
                availableOffers["Applicability"] = "Sitewide"
            elif availableOffers["Category"] != "":
                availableOffers["Applicability"] = "Category"
            elif availableOffers["Course Specific"] != "":
                availableOffers["Applicability"] = "Course Specific"
            elif re.search(r'self paced|self-paced|self pace|self-pace', title, re.IGNORECASE):
                availableOffers["Applicability"] = "Learn Type"
            elif re.search(r'self paced|self-paced|self pace|self-pace', description, re.IGNORECASE):
                availableOffers["Applicability"] = "Learn Type"
            else:
                continue

            if availableOffers["Applicability"] != "Sitewide":
                if re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',
                        title, re.IGNORECASE):
                    availableOffers['Learn Type'] = "Certification"
                    availableOffers['Instruction Type'] = "Self paced"
                elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                    availableOffers['Learn Type'] = "Master"
                    availableOffers['Instruction Type'] = "Instructor"
                else:
                    availableOffers['Learn Type'] = "Certification"
                    availableOffers['Instruction Type'] = "Self paced"


            if re.search(r'any single course|single course|two courses|three courses|four courses', title,
                         re.IGNORECASE):
                availableOffers["Condition"] = re.search(
                    r'any single course|single course|two courses|three courses|four courses', title,
                    re.IGNORECASE).group()
            elif re.search(r'any single course|single course|two courses|three courses|four courses', description,
                           re.IGNORECASE):
                availableOffers["Condition"] = re.search(
                    r'any single course|single course|two courses|three courses|four courses', description,
                    re.IGNORECASE).group()

            if re.search(r'upto', title, re.IGNORECASE):
                availableOffers["Discount Type"] = "Upto"
            elif re.search(r'\d\d\d%|\d\d%|\d%', availableOffers["Discount Percentage"], re.IGNORECASE):
                availableOffers["Discount Type"] = "Flat"

            availableOffers["Description"] = description

            self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

        yield scrapy.Request('https://whizlabs.knoji.com/promo-codes/', self.knoji_whizlabs, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0'}, errback = self.errback_httpbin)

        # yield self.df_coupons.to_csv("coupounz_whizlabs.csv", sep=",")

    def knoji_whizlabs(self, response):

        couponTable = response.xpath(
            "//div[@class='module filter-promo']/h2[contains(text(),'Whizlabs Promo Codes: Complete Timetable')]/..")
        validCoupons = couponTable.xpath(
            "./div/div[@class='tablerow']/div/div[contains(text(),'Yes')]/../../div/span[@class='popu']/../following-sibling::div[contains(text(),'Off')]/..")

        hiddenCoupons = couponTable.xpath(
            "./div/div[@class='tablerow--hidden']/div/div[contains(text(),'Yes')]/../../div/span[@class='popu']/../following-sibling::div[contains(text(),'Off')]/..")

        if hiddenCoupons:
            validCoupons = validCoupons + hiddenCoupons

        for coupons in validCoupons:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Instruction Type': '',
                'User Type': '',  # New User/ All Users
                'Category': '',
                'Skills': '',
                'Card': '',
                'Validity': '',  # Expiry Date
                'Price Condition': '',
                'Price Limit': '',
                'Condition': '',
                'Description': ''
            }
            availableOffers['Partner Name'] = self.PartnerName

            desc = ''
            for text in coupons.xpath("./div/text()").getall():
                if re.search("(?i)((off.+)|(free))", text):
                    desc = text
                    break

            if desc == '':
                continue

            if re.search("(?i) Off (\()?(select|selected|eligible|discounted) (item|product|categories|course|certification)(s?)",desc):
                continue

            availableOffers['Coupon Code'] = coupons.xpath("./div/span[@class='popu']/text()").get()
            if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                availableOffers["Coupon Code"] = "NOT REQUIRED"

            availableOffers['Description'] = desc

            availableOffers['Link'] = coupons.xpath("./div/span[@class='popu']/@data-tab").get()

            if re.search("(?i)\d{1,}\% off", desc):
                availableOffers['Discount Percentage'] = re.search("\d{1,}%", desc)[0]
            elif re.search("(?i)(\$|\₹|(Rs)|\£)[ .]?\d{1,} off", desc):
                availableOffers['Discount Price'] = re.search("(\$|\₹|(Rs)|\£)[ .]?\d{1,}", desc)[0]
                if re.search("\$", desc):
                    availableOffers['Discount Price Currency'] = 'USD'
                elif re.search("(\₹|(Rs)[ .]?\d{1,}", desc):
                    availableOffers['Discount Price Currency'] = 'INR'
                elif re.search("\£", desc):
                    availableOffers['Discount Price Currency'] = 'EURO'

            if re.search("(?i) Off (select|selected|eligible|discounted) (item|product|categories|course|certification)(s?)$",desc):
                continue

            if re.search("((store|site)[ \-]?(wide))|(.*off (\(?)(course)(s?)(\)?)$)", desc, re.IGNORECASE):
                availableOffers['Applicability'] = 'Sitewide'

            elif re.search("(?i)(entire|any|all|next|first|your|total).*(order|course|purchase)(s?)", desc):
                availableOffers['Applicability'] = 'Sitewide'

            elif re.search("(?i)(sale(s?)|promo code|discount code)", desc):
                if availableOffers['Validity']:
                    availableOffers['Applicability'] = 'Sitewide'

            elif re.search("(?i)((off (\()?(certification|certificate)(s?)(\))?$)|(.* ((or more)|\d) (certification|certificate)(s?)))",desc):
                availableOffers['Applicability'] = 'Learn Type'
                availableOffers['Learn Type'] = 'Certification'

            elif re.search("(?i)reactivate|reactivation", desc):
                availableOffers['Applicability'] = 'Subscription'
                availableOffers['Condition'] = 'Subscription Reactivation'

            elif re.search("(?i)(membership|subscription)", desc):
                availableOffers['Applicability'] = 'Subscription'

            elif re.search("(?i)((Master\'s)|(Post Grad))", desc):
                availableOffers['Applicability'] = "Learn Type"
                availableOffers['Learn Type'] = 'Masters'

            elif re.search(
                    "(?i)((off (\()?(certification|certificate)(s?)(\))?$)|(.* ((or more)|\d) (certification|certificate)(s?)))",
                    desc):
                availableOffers['Applicability'] = 'Learn Type'
                availableOffers['Learn Type'] = 'Certification'

            elif re.search("(?i)(membership|subscription)\W", desc):
                availableOffers['Applicability'] = 'Subscription'

            elif re.search("(?i)Training", desc):
                if re.search("(?i) Off \(Training certified\)", desc):
                    availableOffers['Applicability'] = 'Instruction Type'
                elif re.search("(?i)(certified|certification)", desc):
                    availableOffers['Applicability'] = 'Course Specific'
                    availableOffers['Course Specific'] = re.sub("\)$", "", re.sub("(?i)off (\()?(select)?", "",
                                                                                  re.search(
                                                                                      "(?i)off.*(Training|certification|course)?",
                                                                                      desc)[0]))
                else:
                    availableOffers['Applicability'] = 'Instruction Type'
                availableOffers['Learn Type'] = 'Certification'

            elif re.search("(?i)(certifications|courses|Programs)", desc):
                availableOffers['Applicability'] = 'Category'
                availableOffers['Category'] = re.sub("\)$", "", re.sub("(?i)off (\()?(select)?", "", re.search(
                    "(?i)off.*(Trainings|certifications|courses)?", desc)[0]))
                availableOffers['Learn Type'] = 'Certification'

            elif re.search("(?i)(certification|course|Program|certified)", desc):
                availableOffers['Applicability'] = 'Course Specific'
                availableOffers['Course Specific'] = re.sub("\)$", "", re.sub("(?i)off (\()?(select)?", "", re.search(
                    "(?i)off.*(Training|certification|certified|course)?", desc)[0]))
                availableOffers['Learn Type'] = 'Certification'
            else:
                continue

            if re.search("(?i)(Live|Instructor)?.*Training", desc):
                availableOffers['Instruction Type'] = 'Instructor Paced'
            elif re.search("(?i)self paced", desc):
                availableOffers['Instruction Type'] = 'Self Paced'

            if re.search("(?i)Up To", desc):
                availableOffers['Discount Type'] = 'Up To'
            else:
                availableOffers['Discount Type'] = 'Flat'

            if re.search("(?i)all user(s?)", desc):
                availableOffers['User Type'] = 'All Users'
            elif re.search("(?i)new user(s?)", desc):
                availableOffers['User Type'] = 'New Users'

            if re.search("(?i)\W(One|Two|Three|\d) or more (orders|purchases|courses|certifications)?", desc):
                availableOffers['Condition'] = \
                re.search("(?i)(One|Two|Three|\d) or more (orders|purchases|courses|certifications)?", desc)[0]

            if re.search("(?i)(one|first|\d)(st)? year", desc):
                availableOffers['Condition'] = re.search("(?i)(one|first|\d)(st)? year", desc)[0]

            if re.search("(?i)(minimum|order)(s?).*(\$|\₹|(Rs)|\£)[ .]?\d{1,}", desc):
                temp = re.search("(?i)(minimum|order)(s?).*(\$|\₹|(Rs)|\£)[ .]?\d{1,}", desc).group()
                availableOffers['Price Limit'] = re.search("(\$|\₹|(Rs)|\£)[ .]?\d{1,}(\+?)", desc).group()#.replace("$","").replace("₹","")

                # if re.search("(\$|\₹|(Rs)|\£)", temp).group() == "$":
                #     availableOffers['Discount Price Currency'] = "USD"
                # elif re.search("(\$|\₹|(Rs)|\£)", temp).group() == "₹":
                #     availableOffers['Discount Price Currency'] = "INR"

            self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

        # self.df_coupons.drop_duplicates(
        #     subset=['Discount Percentage', 'Coupon Code', 'Learn Type', 'Applicability', 'Discount Type',
        #             'Course Specific'], inplace=True, ignore_index=True)
        self.df_coupons = self.df_coupons.sort_values(by=['Discount Percentage'], ascending=False, ignore_index=True)
        # yield self.df_coupons.to_csv("knoji_whizlabs.csv", sep=",")
        # yield self.df_coupons.to_csv("market_discount_whizlabs.csv", sep=",")
        self.uploadData()