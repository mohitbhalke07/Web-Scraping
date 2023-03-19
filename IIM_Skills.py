import scrapy
import requests
import re
import datetime
import dateutil.parser as parser
from datetime import datetime

class iimskilscrapy(scrapy.Spider):
    name = "iimskill"
    start_urls = [
        "https://iimskills.com/all-courses/"
    ]

    def parse(self, response):
        links = response.xpath('//div[@class="vc_toggle_content"]//a/@href').extract()

        titles = response.xpath('//div[@class="vc_toggle_title"]//h4/text()').extract()


        for i in range(len(links)):
            yield scrapy.Request(links[i], callback=self.parser_contents, cb_kwargs={"title":titles[i]})

    def parser_contents(self, response, title):

        price = response.xpath('//header[@class="vc_cta3-content-header"]//h2/text()').extract_first().split()[1]

        if response.xpath('//header[@class="vc_cta3-content-header"]//h2/text()').extract_first().split()[0] == '₹':
            currency = "INR"
        else:
            currency = "USD"

        batches_dates = response.xpath('//div[@class="key-icon-box icon-default icon-left cont-center   "]//h4[@class="service-heading"]/text() | //div[@class="vc_row wpb_row vc_row-fluid vc_custom_1553702154880"]//h4[@class="service-heading"]/text()').extract()

        batches_times = response.xpath('//div[@class="key-icon-box icon-default icon-left cont-center   "]//p/text() | //div[@class="vc_row wpb_row vc_row-fluid vc_custom_1553702154880"]//p/text()').extract()

        # Getting Weekend and Weekdays batch dates
        try:
            weekday_start_date = re.findall(r'\d{1,2} (?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{2,4}',batches_dates[0].replace("th","").replace("nd",""))[0]
            weekday_start_date = parser.parse(weekday_start_date).isoformat()

        except:
            weekday_start_date = ""

        weekend_batch = ",".join(re.findall("Saturday|Sunday", "".join(batches_dates)))

        try:
            weekend_start_date = ",".join(re.findall(r"\d{1,2} (?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{2,4}", "".join(batches_dates[1:]).replace("th","").replace("nd","").replace("st","")))
            weekend_start_date = parser.parse(weekend_start_date).isoformat()
        except:
            weekend_start_date = ""

        # Getting Weekend and Weekdays batch timing
        try:
            try:
                wk1 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+",batches_times[0])[0].split("-")[0].strip(), '%I:%M %p')
                wk2 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+",batches_times[0])[0].split("-")[1].strip(), '%I:%M %p')
                weekday_timing = f'{wk1.isoformat()} - {wk2.isoformat()}'
            except:
                wk1 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+",batches_times[0])[0].split("-")[0].strip(), '%I %p')
                wk2 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+",batches_times[0])[0].split("-")[1].strip(), '%I:%M %p')
                weekday_timing = f'{wk1.isoformat()} - {wk2.isoformat()}'

            try:
                wkd1 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+", batches_times[1])[0].split("-")[0].strip(), '%I:%M %p')
                wkd2 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+", batches_times[1])[0].split("-")[1].strip(), '%I:%M %p')
                weekend_timing = f'{wkd1.isoformat()} - {wkd2.isoformat()}'
            except:
                wkd1 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+", batches_times[1])[0].split("-")[0].strip(), '%I %p')
                wkd2 = datetime.strptime(re.findall("\d+:\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+:\d+ \w+|\d+ \w+ - \d+ \w+", batches_times[1])[0].split("-")[1].strip(), '%I:%M %p')
                weekend_timing = f'{wkd1.isoformat()} - {wkd2.isoformat()}'


        except:
            weekday_timing = ""
            weekend_timing = ""

        # What you Learn
        what_u_learn = response.xpath('//div[@class="vc_column-inner vc_custom_1585592427442"]//p/text() | //div[@class="vc_column-inner vc_custom_1617795147451"]//ul//li/span/text() | //div[@class="vc_column-inner vc_custom_1614421290120"]//ul//li/span/text() | //div[@class="vc_column-inner vc_custom_1585592427442"]//ul//li/span/text() | //div[@class="vc_column-inner vc_custom_1622198128856"]//ul//li/span/text()').extract()

        reviewer_name = [i.replace("\n","").replace(",","").strip() for i in response.xpath('//div[@class="tm-prof3"]//p/text()').extract()]
        reviewer_name = "|".join(list(filter(lambda x: x != '', reviewer_name)))

        reviews = [i.replace("\n","").replace(",","").strip() for i in response.xpath('//div[@class="tm-profile3"]//p/text()').extract()]
        reviews = "|".join(list(filter(lambda x: x != '', reviews)))

        modules = response.xpath('//div[@class="wpb_column vc_column_container vc_col-sm-6"]//p[@class="vc_custom_heading"]/text()').extract()
        sub_modules = []

        for i in response.css("div.vc_column-inner.vc_custom_1636468838184").css("div.vc_toggle_content"):
            if i.css("ul").css("li").css("span::text").extract() is not None:
                sub_modules.append(i.css("ul").css("li").css("span::text").extract())
        sub_modules = list(filter(None, sub_modules))

        for i in response.css("div.vc_column-inner").css("div.vc_toggle_content"):
            if i.css("ul").css("li::text").extract() is not None:
                sub_modules.append(i.css("ul").css("li::text").extract())
        sub_modules = list(filter(None, sub_modules))
        for i in response.css("div.vc_column-inner").css("div.vc_toggle_content"):
            if i.css("ul").css("li").css("span::text").extract() is not None:
                sub_modules.append(i.css("ul").css("li").css("span::text").extract())
        sub_modules = list(filter(None, sub_modules))

        for i in response.css("div.vc_row wpb_row vc_row-fluid.vc_custom_1613917780797.vc_row-has-fill").css("div.vc_toggle_content"):
            if i.css("ul").css("li::text").extract() is not None:
                sub_modules.append(i.css("ul").css("li::text").extract())
        sub_modules = list(filter(None, sub_modules))
        for i in response.css("div.vc_row wpb_row vc_row-fluid.vc_custom_1613917780797.vc_row-has-fill").css("div.vc_toggle_content"):
            if i.css("ul").css("li").css("span::text").extract() is not None:
                sub_modules.append(i.css("ul").css("li").css("span::text").extract())
        sub_modules = list(filter(None, sub_modules))

        # for i in response.css("div.wpb_column.vc_column_container.vc_col-sm-12").css("div.vc_toggle_content"):
        #     if i.css("ul").css("li").css("span::text").extract() is not None:
        #         sub_modules.append(i.css("ul").css("li").css("span::text").extract())
        # sub_modules = list(filter(None, sub_modules)

        sub_modules = list(filter(None, sub_modules))

        modlist = []
        modulenum = 1

        if sub_modules != []:
            for i in range(len(modules)):
                # modlist.append('<?xml version="1.0"?><mainmodule>')
                module = f'<module{modulenum}><heading>{modules[i]}</heading><subheading>'
                modlist.append(module)
                # print(module)
                try:
                    submodnum = 1
                    for j in sub_modules[i]:
                        if j is not None:
                            submodule = f"<item{submodnum}>{j}</item{submodnum}>"
                            modlist.append(submodule)
                            submodnum += 1
                        else:
                            continue
                except:
                    pass

                modlist.append(f'</subheading></module{modulenum}>')
                modulenum += 1
            modlist.insert(0, '<?xml version="1.0"?><mainmodule>')
            modlist.append(f'</mainmodule>')
        else:
            for i in range(len(modules)):
                module = f"<p><strong>Module {modulenum}: {modules[i]}</strong>"
                modlist.append(module)
                modulenum += 1
                modlist.append('</p>')

        contents = "".join(modlist)

        yield {
            "link": response,
            "title": title,
            "price": price,
            "currency": currency,
            "weekday_start_date": weekday_start_date,
            "weekday_timing": weekday_timing,
            "weekend_batch": weekend_batch,
            "weekend_start_date": weekend_start_date,
            "weekend_timing": weekend_timing,
            "what_u_learn": what_u_learn,
            "reviewer_name": reviewer_name,
            "reviews": reviews,
            "contents": contents
        }
