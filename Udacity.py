import scrapy
import requests
import re
from ..items import UdacityItem
import dateutil.parser as parser


class thirdscrapy(scrapy.Spider):
    name = "udacity"
    start_urls = [
        "https://www.udacity.com/courses/all"
    ]

    def parse(self, response):
        l = "https://www.udacity.com/data/catalog.json?v=854825c4"
        yield scrapy.Request(url =l, callback=self.parser_contents)

    def parser_contents(self, response):
        data = response.json()
        links = []
        for i in data:
            links.append("https://www.udacity.com" + i["url"])
        links = links[80:-8]

        # for link in links:
        #     yield scrapy.Request(url=link, callback=self.parser_contents1)

        for link in links:
            li = "https://api.udacity.com/api/braavos/prices?anonymous_id=0ea6d6cc-dff8-47b7-8489-63c9e5ef2e59&currency=INR&node_key=" + link[-5:]
            yield scrapy.Request(li, callback=self.course_fees, cb_kwargs={"link":link})

    def course_fees(self, response, link):
        link = link
        try:
            data = response.json()

            display_price = data['results'][0]['payment_plans']['upfront_recurring']['upfront_amount']['original_amount_display']
            emi = data['results'][0]['payment_plans']['upfront_recurring']['recurring_amount']['original_amount_display']
        except:
            display_price = ""
            emi = ""

        yield scrapy.Request(link, callback=self.parser_contents1, cb_kwargs={"display_price": display_price, "emi": emi})

    def parser_contents1(self, response, display_price, emi):
        items = UdacityItem()

        if display_price is "":
            display_price = response.xpath('//div[@class="_price-card_pricingTemplate__Am1WB"]//ins/text() | //div[@class="course-overview_col__1NXeV"]//h5/text()').get()
        else:
            display_price = [i for i in re.findall(r'\d+',display_price)]
            display_price = "".join(display_price)

        if display_price == "Free" or display_price == "":
            currency = ""
        else:
            currency = "INR"


        emi = [i for i in re.findall(r'\d+',emi)]
        try:
            emi = "".join(emi)
        except:
            pass


        title = response.xpath('//header[@class="_brand-refresh_textContentContainer__2w9r5"]/h1/text() | //h1[@class="course-hero_courseTitle__1Djr9"]/text()').get()

        main_desc = []
        if response.xpath('//p[@class="_brand-refresh_summary__oft3N"]/text()').extract() is not None:
            main_desc.append(response.xpath('//p[@class="_brand-refresh_summary__oft3N"]/text()').extract())
        if response.css("div.course-overview_courseSummary__3lmAE").css("p::text").extract() is not None:
            main_desc.append(response.css("div.course-overview_courseSummary__3lmAE").css("p::text").extract())
        if response.css("div.course-overview_courseSummary__3lmAE").css("div::text").extract() is not None:
            main_desc.append(response.css("div.course-overview_courseSummary__3lmAE").css("div::text").extract())
        try:
            main_desc = list(filter(None, main_desc))[0]
            main_desc = f"<p>{main_desc[0]}</p>"
        except:
            pass
        try:
            total_duration = [i for i in re.findall(r'-?\d+\.?\d*', response.xpath('//div[@class="degree-info-columns_container__e0M6Q"]//h5/text()').get())]
        except:
            total_duration = ""

        try:
            total_duration_unit = [i for i in re.findall(r'[a-z]+', response.xpath('//div[@class="degree-info-columns_container__e0M6Q"]//h5/text()').get().lower())]
        except:
            total_duration_unit = ""


        try:
            course_start_date = response.css("div.degree-info-columns_container__e0M6Q").css("h5::text").extract()[1]
            date = parser.parse(course_start_date)
            course_start_date = date.isoformat()
        except:
            course_start_date = None


        prerequisite = []
        try:
            if response.css("div.degree-info-columns_container__e0M6Q").css("h5::text").extract() is not None:
                prerequisite.append(
                    response.css("div.degree-info-columns_container__e0M6Q").css("h5::text").extract()[2])
        except:
            pass
        if response.css("section.course-requirements_container__3BFzM.contain").css("p::text").extract() is not None:
            prerequisite.append(
                response.css("section.course-requirements_container__3BFzM.contain").css("p::text").extract())
        try:
            prerequisite = list(filter(None, prerequisite))[0]
        except:
            pass

        what_u_learn = response.xpath('//div[@class="related-nd-path_intro__NLAsZ"]//p/text() | //div[@class="degree-syllabus_overview__36RFP"]//p/text()').get()

        instructor = []
        if response.css("h3.course-instructors_instructorName__13-ML.h5::text").extract() is not None:
            instructor.append(response.css("h3.course-instructors_instructorName__13-ML.h5::text").extract())
        if response.css("article._brand-refresh_cardContent__3jDyV").css("h3::text").extract() is not None:
            instructor.append(response.css("article._brand-refresh_cardContent__3jDyV").css("h3::text").extract())
        try:
            instructor = list(filter(None, instructor))[0]
            instructor = "| ".join(instructor)
        except:
            pass

        inst_desig = response.css("article._brand-refresh_cardContent__3jDyV").css("b::text").extract()
        inst_desig = "| ".join(inst_desig)

        inst_bio = []  # response.css("article._brand-refresh_cardContent__3jDyV").css("p::text").extract()
        if response.css("article._brand-refresh_cardContent__3jDyV").css("p::text").extract() is not None:
            inst_bio.append(response.css("article._brand-refresh_cardContent__3jDyV").css("p::text").extract())
        try:
            inst_bio = inst_bio[0]
            inst_bio = "| ".join(inst_bio)
        except:
            pass

        # inst_img = []
        # if response.css("article._brand-refresh_cardContent__3jDyV").css("picture").css("img::attr(src)").extract() is not None:
        #     inst_img.append(response.css("article._brand-refresh_cardContent__3jDyV").css("picture").css("img::attr(src)").extract())
        # if response.css("div.course-instructors_instructorsList__3luFq").css("img::attr(src)").extract() is not None:
        #     inst_img.append(response.css("div.course-instructors_instructorsList__3luFq").css("img::attr(src)").extract())
        # try:
        #     inst_img = list(filter(None, inst_img))[0]
        #     inst_img = "| ".join(inst_img)
        # except:
        #     pass

        modules = []
        if response.css("div.degree-syllabus_programOverviewLayout__vOx8b").css("h5::text").extract() is not None:
            modules.append(response.css("div.degree-syllabus_programOverviewLayout__vOx8b").css("h5::text").extract())
        if response.css("div.course-syllabus_lessonUpper__3JA3U").css("h2::text").extract() is not None:
            modules.append(response.css("div.course-syllabus_lessonUpper__3JA3U").css("h2::text").extract())
        try:
            modules = list(filter(None, modules))[0]
        except:
            pass

        sub_modules = []
        for i in response.css("ul.course-syllabus_points__1wR0B"):
            if i.css("li::text").extract() is not None:
                sub_modules.append(i.css("li::text").extract())
        for i in response.css("ul.degree-syllabus_projects__2yt-W"):
            if i.css("li::text").extract() is not None:
                sub_modules.append(i.css("li::text").extract())
        for i in response.css("ul.degree-syllabus_projects__2yt-W"):
            if i.css("li").css("span::text").extract() is not None:
                sub_modules.append(i.css("li").css("span::text").extract())
        sub_modules = list(filter(None, sub_modules))

        modlist = []
        modulenum = 1
        for i in range(len(modules)):
            # modlist.append('<?xml version="1.0"?><mainmodule>')
            module = f'<module{modulenum}><heading>{modules[i]}</heading><subheading>'
            modlist.append(module)
            # print(module)
            try:
                submodnum = 1
                for j in sub_modules:
                    submodule = f"<item{submodnum}>{j[i]}</item{submodnum}>"
                    modlist.append(submodule)
                    submodnum += 1
            except:
                pass

            modlist.append(f'</subheading></module{modulenum}>')
            modulenum += 1
        modlist.insert(0, '<?xml version="1.0"?><mainmodule>')
        modlist.append(f'</mainmodule>')

        contents = "".join(modlist)


        yield {
            "response": response,
            "title": title,
            "main_desc": main_desc,
            "prerequisite": prerequisite,
            "what_u_learn": what_u_learn,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "inst_bio": inst_bio,
            "total_duration": total_duration,
            "total_duration_unit":total_duration_unit,
            "course_start_date": course_start_date,
            "contents": contents,
            "display_price": display_price,
            "emi": emi,
            "currency": currency
        }





