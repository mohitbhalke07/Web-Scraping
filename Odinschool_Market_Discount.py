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


class odinschool_market_discount(scrapy.Spider):
    name = "odinschool_market_discount"

    PartnerName = 'OdinSchool'

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

    df_coupons = pd.DataFrame(
        columns=['Partner Name', 'Link', 'Discount Percentage', 'Discount Price', 'Discount Price Currency',
                 'Coupon Code',
                 'Learn Type', 'Applicability', 'Discount Type', 'Course Specific', 'Course Link', 'Instruction Type','User Type',
                 'Category', 'Card', 'Validity', 'Price Condition', 'Price Limit', 'Condition', 'Description'])

    def start_requests(self):
        self.logger.info(f'--- {self.PartnerName} Scraper - [{self.name}]---')
        return [scrapy.FormRequest("https://www.odinschool.com/", callback=self.mainfunc, errback = self.errback_httpbin)]

    def mainfunc(self, response):
        availableOffers = {
            'Partner Name': "",
            'Link': '',
            'Discount Percentage': 0,
            'Discount Price': '',
            'Discount Price Currency': '',
            'Coupon Code': '',
            'Learn Type': '',
            'Applicability': "Sitewide",
            'Discount Type': '',
            'Course Specific': '',
            'Course Link': '',
            'Instruction Type': '',
            'User Type': '',
            'Category': '',
            'Card': '',
            'Validity': '',
            'Price Condition': '',
            'Price Limit': '',
            'Condition': '',
            'Description': "Sitewide"
        }
        availableOffers['Partner Name'] = self.PartnerName

        self.df_coupons = self.df_coupons.append(availableOffers, ignore_index=True)

        # yield self.df_coupons.to_csv("market_discount_odinschool.csv", sep=",")
        self.uploadData()