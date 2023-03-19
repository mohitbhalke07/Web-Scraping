import scrapy
import re
import json
import requests
import pandas as pd
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

class edx_market_discount(scrapy.Spider):
    name = "edx_market_discount"
    PartnerName = 'edX'

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
        columns=['Partner Name', 'Link', 'Discount Percentage', 'Discount Price', 'Discount Price Currency', 'Coupon Code',
                 'Learn Type', 'Applicability', 'Discount Type', 'Course Specific', 'Course Link', 'Instruction Type','User Type',
                 'Category', 'Card', 'Validity', 'Price Condition', 'Price Limit', 'Condition', 'Description'])

    def start_requests(self):
        self.logger.info(f'--- {self.PartnerName} Scraper - [{self.name}]---')
        return [scrapy.FormRequest("https://www.grabon.in/edx-coupons/", callback=self.grabon_edx, errback = self.errback_httpbin)]


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


    def grabon_edx(self, response):
        data = response.xpath('.//div[@class="gc-box banko"]')

        for coupon in data:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Instruction Type': '',
                'User Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Category': '',
                'Card': '',
                'Validity': '',
                'Price Condition': '',
                'Price Limit': '',
                'Condition': '',
                'Description': ''
            }

            availableOffers['Partner Name'] = self.PartnerName
            title = coupon.xpath('.//div[@class="gcbr go-cpn-show go-cpy"]//p/text()').extract_first()
            description = re.sub('<[^>]+>', '', coupon.xpath('.//div[@class="gcb-det"]').extract_first()).strip()

            if re.search(r'plus an extra [0-9]+% off| get an extra [0-9]+% off', description, re.IGNORECASE):
                temp = re.search(r'plus an extra [0-9]+% off| get an extra [0-9]+% off', description, re.IGNORECASE).group()
                availableOffers["Discount Percentage"] = re.search(r'[0-9]+%', temp, re.IGNORECASE).group()
            else:
                discount_per = coupon.xpath('.//div[@class="bank"]//span/text()').extract_first()
                if re.search(r'\d\d\d%|\d\d%|\d%|\$[0-9]+]', discount_per, re.IGNORECASE):
                    availableOffers["Discount Percentage"] = discount_per


            if availableOffers["Discount Percentage"] == "":
                print("================>  Breaking")
                continue

            desc_cousp = re.findall(r'courses on', description.lower())
            if desc_cousp != []:
                coursp = description.lower().split(desc_cousp[0])[1].split(".")[0].replace("etc", "").strip().split(",")
                # coursp = coursp.remove("")
                # print("=============================================================================>>>>>",type(coursp))
                availableOffers["Applicability"] = "Categoty"
                availableOffers["Learn Type"] = "Certifications"

                for i in range(len(coursp)):
                    if coursp[i] != "":
                        availableOffers["Category"] = coursp[i]

                        availableOffers["Link"] = "https://www.grabon.in/coupon-codes/" + re.search(r'cpn_[0-9]+',coupon.extract()).group().split("_")[1]

                        availableOffers["Coupon Code"] = coupon.xpath('.//span[@class="visible-lg"]/text()').extract_first()

                        if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                            availableOffers["Coupon Code"] = "NOT REQUIRED"

                        if re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates', title, re.IGNORECASE):
                            availableOffers['Learn Type'] = "Certification"
                            availableOffers['Instruction Type'] = "Self paced"
                        elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                            availableOffers['Learn Type'] = "Master"
                            availableOffers['Instruction Type'] = "Instructor"


                        # if re.search(r'all courses|all course|all certification|all certifications', title, re.IGNORECASE):
                        #     availableOffers["Applicability"] = "Sitewide"
                        # elif re.search(r'all courses|all course|all certification|all certifications', description, re.IGNORECASE):
                        #     availableOffers["Applicability"] = "Sitewide"
                        # elif availableOffers["Category"] != "":
                        #     availableOffers["Applicability"] = "Category"
                        # elif availableOffers["Course Specific"] != "":
                        #     availableOffers["Applicability"] = "Course Specific"
                        # elif re.search(r'self paced|self-paced|self pace|self-pace', title, re.IGNORECASE):
                        #     availableOffers["Applicability"] = "Learn Type"
                        # elif re.search(r'self paced|self-paced|self pace|self-pace', description, re.IGNORECASE):
                        #     availableOffers["Applicability"] = "Learn Type"
                        # else:
                        #     availableOffers["Applicability"] = "Sitewide"

                        if re.search(r'upto', title, re.IGNORECASE):
                            availableOffers["Discount Type"] = "Upto"
                        elif re.search(r'\d\d\d%|\d\d%|\d%', availableOffers["Discount Percentage"], re.IGNORECASE):
                            availableOffers["Discount Type"] = "Flat"

                        availableOffers["Description"] = description

                        self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

            else:
                availableOffers["Link"] = "https://www.grabon.in/coupon-codes/" + re.search(r'cpn_[0-9]+', coupon.extract()).group().split("_")[1]

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
                    if re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',title, re.IGNORECASE):
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

                self.df_coupons = self.df_coupons.append(availableOffers,  ignore_index=True)

        yield scrapy.Request('https://www.couponzguru.com/edx-coupons/', self.couponzguru_edx, errback = self.errback_httpbin)

        # yield self.df_coupons.to_csv("Grabon_edx.csv", sep=",")

    def couponzguru_edx(self, response):
        data2 = response.xpath('//div[@class="coupon-list"]')

        for coupon in data2:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Instruction Type': '',
                'User Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Category': '',
                'Card': '',
                'Validity': '',
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


            desc_cousp = re.findall(r'select from|on courses – ', description.lower())
            if desc_cousp != []:
                coursp = description.lower().split(desc_cousp[0])[1].split(".")[0].replace("& more courses", "").replace("& much more", "").replace("& more shown on the landing page", "").replace("across site","").replace("programs","").replace("online courses","").strip().split(",")
                print(coursp)

                availableOffers["Applicability"] = "Category"
                availableOffers["Learn Type"] = "Certifications"

                for i in range(len(coursp)):
                    availableOffers["Category"] = coursp[i]

                    # availableOffers["Link"] = coupon.xpath('.//a/@href').extract_first()
                    raw_link = coupon.xpath('.//div//a/@onclick').extract_first()
                    try:
                        availableOffers["Link"] = raw_link.split(";")[1].split()[2].replace("'", "").replace(",","").replace(")", "")
                    except:
                        availableOffers["Link"] = coupon.xpath('.//a/@href').extract_first()

                    code_raw = coupon.xpath('.//div//a/@onclick').extract_first()
                    try:
                        availableOffers["Coupon Code"] = code_raw.split(";")[1].split()[1].replace("'", "").replace(",", "")
                    except:
                        pass

                    if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                        availableOffers["Coupon Code"] = "NOT REQUIRED"

                    if re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Certification"
                        availableOffers['Instruction Type'] = "Self paced"
                    elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Master"
                        availableOffers['Instruction Type'] = "Instructor"

                    if re.search(r'any single course|single course|two courses|three courses|four courses', title, re.IGNORECASE):
                        availableOffers["Condition"] = re.search(r'any single course|single course|two courses|three courses|four courses', title, re.IGNORECASE).group()
                    elif re.search(r'any single course|single course|two courses|three courses|four courses', description, re.IGNORECASE):
                        availableOffers["Condition"] = re.search(r'any single course|single course|two courses|three courses|four courses', description, re.IGNORECASE).group()

                    if re.search(r'upto', title, re.IGNORECASE):
                        availableOffers["Discount Type"] = "Upto"
                    elif re.search(r'\d\d\d%|\d\d%|\d%', availableOffers["Discount Percentage"], re.IGNORECASE):
                        availableOffers["Discount Type"] = "Flat"

                    availableOffers["Description"] = description

                    self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

            else:
                availableOffers["Link"] = coupon.xpath('.//a/@href').extract_first()

                code_raw = coupon.xpath('.//div//a/@onclick').extract_first()
                try:
                    availableOffers["Coupon Code"] = code_raw.split(";")[1].split()[1].replace("'", "").replace(",", "")
                except:
                    pass

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
                    if re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Certification"
                        availableOffers['Instruction Type'] = "Self paced"
                    elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Master"
                        availableOffers['Instruction Type'] = "Instructor"


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

        yield scrapy.Request('https://discountcodes.trustedreviews.com/edx.org', self.trustedreviews_edx, errback = self.errback_httpbin)

        # yield self.df_coupons.to_csv("Grabon_edx.csv", sep=",")

    def trustedreviews_edx(self, response):
        data3 = response.xpath('//div[@class="promotion-list editable "]//div[@class="promotion-discount-card promotion-discount-card--condensed editable shadow w-100 bg-white"]')

        for coupon in data3:
            availableOffers = {
                'Link': '',
                'Discount Percentage': '',
                'Discount Price': '',
                'Discount Price Currency': '',
                'Coupon Code': '',
                'Learn Type': '',
                'Instruction Type': '',
                'User Type': '',
                'Applicability': '',
                'Discount Type': '',
                'Course Specific': '',
                'Course Link': '',
                'Category': '',
                'Card': '',
                'Validity': '',
                'Price Condition': '',
                'Price Limit': '',
                'Condition': '',
                'Description': ''
            }
            availableOffers['Partner Name'] = self.PartnerName

            title = coupon.xpath('.//h3[@class="promotion-discount-card__info__title"]/text()').extract_first()
            description = coupon.xpath('.//div[@class="promotion-discount-card__info__description"]/text()').extract_first().strip()

            discount_per = re.findall("\d\d\d%|\d\d%", title)
            if discount_per != []:
                availableOffers["Discount Percentage"] = discount_per[0]

            if availableOffers["Discount Percentage"] == "":
                print("================>  Breaking")
                continue

            desc_cousp = re.findall(r'select from|on courses – |course in', description.lower())
            if desc_cousp != []:
                coursp = description.lower().split(desc_cousp[0])[1].split("?")[0].replace("or the", "").replace("& more shown on the landing page", "").strip().split(",")
                print(coursp)

                availableOffers["Applicability"] = "Category"
                availableOffers["Learn Type"] = "Certifications"

                for i in range(len(coursp)):

                    try:
                        availableOffers["Coupon Code"] = coupon.xpath(
                            './/span[@class="btn-peel__secret"]/text()').extract_first()
                    except:
                        pass

                    if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                        availableOffers["Coupon Code"] = "NOT REQUIRED"

                    availableOffers["Category"] = coursp[i]

                    availableOffers["Link"] = "https://discountcodes.trustedreviews.com/edx.org#c" + re.search(r'data-promotion-id="[0-9]+"', coupon.extract()).group().split('"')[1]

                    if re.search(r'any single course|single course|two courses|three courses|four courses', title,re.IGNORECASE):
                        availableOffers["Condition"] = re.search(r'any single course|single course|two courses|three courses|four courses', title,re.IGNORECASE).group()
                    elif re.search(r'any single course|single course|two courses|three courses|four courses', description,re.IGNORECASE):
                        availableOffers["Condition"] = re.search(r'any single course|single course|two courses|three courses|four courses', description,re.IGNORECASE).group()

                    if re.search(r'upto', title, re.IGNORECASE):
                        availableOffers["Discount Type"] = "Upto"
                    elif re.search(r'\d\d\d%|\d\d%|\d%', availableOffers["Discount Percentage"], re.IGNORECASE):
                        availableOffers["Discount Type"] = "Flat"

                    # if re.search(r'all courses|all course|all certification|all certifications', title, re.IGNORECASE):
                    #     availableOffers["Applicability"] = "Sitewide"
                    # elif re.search(r'all courses|all course|all certification|all certifications', description,re.IGNORECASE):
                    #     availableOffers["Applicability"] = "Sitewide"
                    # elif availableOffers["Category"] != "":
                    #     availableOffers["Applicability"] = "Category"
                    # elif availableOffers["Course Specific"] != "":
                    #     availableOffers["Applicability"] = "Course Specific"
                    # elif re.search(r'self paced|self-paced|self pace|self-pace', title, re.IGNORECASE):
                    #     availableOffers["Applicability"] = "Learn Type"
                    # elif re.search(r'self paced|self-paced|self pace|self-pace', description, re.IGNORECASE):
                    #     availableOffers["Applicability"] = "Learn Type"

                    elif re.search(r'self paced|self-paced|self pace|self-pace|courses|course|certificates|certifications|certification|certificates',title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Certification"
                        availableOffers['Instruction Type'] = "Self paced"
                    elif re.search(r'instructor-led|instructor', title, re.IGNORECASE):
                        availableOffers['Learn Type'] = "Master"
                        availableOffers['Instruction Type'] = "Instructor"

                    availableOffers["Description"] = description

                    self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

            else:
                try:
                    availableOffers["Coupon Code"] = coupon.xpath('.//span[@class="btn-peel__secret"]/text()').extract_first()
                except:
                    pass

                if availableOffers["Coupon Code"] == "" or availableOffers["Coupon Code"] == "ACTIVATE OFFER":
                    availableOffers["Coupon Code"] = "NOT REQUIRED"

                availableOffers["Link"] = "https://discountcodes.trustedreviews.com/edx.org#c" + re.search(r'data-promotion-id="[0-9]+"', coupon.extract()).group().split('"')[1]

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


                availableOffers["Description"] = description

                self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

        # self.df_coupons.drop_duplicates(
        #     subset=['Discount Percentage', 'Coupon Code', 'Learn Type', 'Applicability', 'Discount Type',
        #             'Course Specific'], inplace=True, ignore_index=True)
        # self.df_coupons.dropna(subset=['Discount Percentage'])
        self.df_coupons = self.df_coupons.sort_values(by=['Discount Percentage'], ascending=False, ignore_index=True)
        # yield self.df_coupons.to_csv("market_discount_edx.csv", sep=",")
        self.uploadData()