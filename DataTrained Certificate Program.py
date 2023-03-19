import scrapy
from ..items import  DatatrainedItem

class secondscrappy(scrapy.Spider):
    name = "scrap2"
    start_urls = [
        "https://www.datatrained.com/"
    ]

    def parse(self, response):
        for link in response.css("div#data-science-certificate-program").css("a::attr(href)"):
            yield response.follow(link.get(), callback=self.parse_content)
        for link in response.css("div#data-engineering-certificate-program").css("a::attr(href)"):
            yield response.follow(link.get(), callback=self.parse_content)



    def parse_content(self, response):
        items = DatatrainedItem()
        links = [i.attrib["href"] for i in response.css("div#data-science-certificate-program").css("a")]

        title = response.css("div.free-course-cont").css("h1::text").extract_first()
        short_desc = response.css("div.free-course-cont").css("p").extract()
        main_desc = response.css("div.col-lg-12").css("p::text").extract_first()
        learn_type = response.css("div.box").css("h3::text")[0].extract()
        cover_img = response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")

        instructor = []
        for i in response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if i.css("h3") is not None:
                instructor.append(i.css("h3::text").extract_first())
            if i.css("h5") is not None:
                instructor.append(i.css("h5::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 "):
           if j.css("h5") is not None:
                instructor.append(i.css("h5::text").extract_first())
        filtered_list1 = list(filter(None, instructor))
        instructor = "| ".join(filtered_list1)
        #instructor = [i.css("h3::text").extract_first() for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]

        inst_desig = []
        for i in response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 "):
            if j.css("h6") is not None:
                inst_desig.append(i.css("h6::text").extract_first())
        filtered_list2 = list(filter(None, inst_desig))
        inst_desig = "| ".join(filtered_list2)
        #inst_desig = [i.css("span::text").extract_first() for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]

        inst_bio = [i.css("p::text").extract_first() for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]
        inst_bio = "| ".join(inst_bio)
        inst_img = [j.attrib["src"] for j in [i.css("img") for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]]
        inst_img = "| ".join(inst_img)
        total_vid_cont = response.css("ul.ondemand").css("li::text")[0].extract()
        display_price = response.css("div.details-pg").css("li.time-pg").css("span::text").extract_first()
        lang = response.css("div.bottom-content-2").css("li::text").extract_first()
        prerequisite = [j.extract() for j in response.css("div.offset-lg-1.col-lg-6.col-md-6.flex").css("li::text")]
        prerequisite = "| ".join(prerequisite)

        what_u_learn = []
        for i in response.css("div.what-you-learn"):
            if i.css("p") is not None:
                what_u_learn.append(i.css("p::text").extract_first())
            if i.css("li") is not None:
                what_u_learn.append(i.css("li::text").extract())
        filtered_list3 = list(filter(None, what_u_learn))
        what_u_learn = filtered_list3
        #what_u_learn = response.css("div.what-you-learn").css("p::text").extract_first()

        reviewer_names = [i.css("span::text").extract_first() for i in response.css("div.review-box")]
        reviewer_names = "| ".join(reviewer_names)
        reviewer_photos = [i.css("img").attrib["src"] for i in response.css("div.review-box")]
        reviewer_photos = "| ".join(reviewer_photos)
        reviews = [i.css("p::text").extract_first() for i in response.css("div.review-box")]
        reviews = "| ".join(reviews)
        ratings = [i.css("li::text").extract_first() for i in response.css("div.review-box")]
        ratings = "| ".join(ratings)
        try:
            faq_questions = [j.extract() for j in [i.css("h3::text") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_questions = "| ".join(faq_questions)
            faq_answers = [j.css("p::text").extract_first() for j in [i.css("div.card") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_answers = "|".join(faq_answers)
        except:
            faq_questions = None
            faq_answers = None

        items["title"] = title
        items["short_desc"] = short_desc
        items["main_desc"] = main_desc
        items["learn_type"] = learn_type
        items["cover_img"] = cover_img
        items["instructor"] = instructor
        items["inst_desig"] = inst_desig
        items["inst_bio"] = inst_bio
        items["inst_img"] = inst_img
        items["total_vid_cont"] = total_vid_cont
        items["display_price"] = display_price
        items["lang"] = lang
        items["prerequisite"] = prerequisite
        items["what_u_learn"] = what_u_learn
        items["reviewer_names"] = reviewer_names
        items["reviewer_photos"] = reviewer_photos
        items["reviews"] = reviews
        items["ratings"] = ratings
        items["faq_questions"] = faq_questions
        items["faq_answers"] = faq_answers

        yield items


