import scrapy
from ..items import ImaginxpItem
import requests
import re
from price_parser import Price

class secondscrapy(scrapy.Spider):
    name = "scrapimaginxp"
    start_urls = [
        "https://imaginxp.com/"
    ]

    def parse(self, response):
        for link in response.css("div.certificationcourses-ddmenu").css("div.column1").css("a::attr(href)")[:4]:
            #print(response.urljoin(link.get()))
            yield scrapy.Request(response.urljoin(link.get()), callback=self.parser_contents)
        for link in response.css("div.certificationmenucontainer").css("div.column2").css("ul.submenu").css("a::attr(href)")[:3:2]:
            #print(response.urljoin(link.get()))
            yield response.follow(response.urljoin(link.get()), callback=self.parser_contents)
        #certificate = response.css("div.certificationcourses-ddmenu").css("div.column1").css("a::attr(href)")[:4]
        #executive_prog = response.css("div.certificationmenucontainer").css("div.column2").css("ul.submenu").css("a::attr(href)")[:3:2]

    def parser_contents(self, response):
        items = ImaginxpItem()

        title = response.css("div.heroBannerLeft").css("h3::text").extract_first()

        short_desc = response.css("div.heroBannerLeft").css("p::text").extract()

        main_desc = response.css("div.aboutDesc").css("p::text").extract_first()
        main_desc = f'<p>{main_desc}</p>'

        display_price = response.css("div.priceBlock").css("ins::text").extract_first()
        display_price = Price.fromstring(display_price)
        display_price = display_price.amount_float

        currency = "INR"

        emi_start = response.css("div.emiStartBlock::text").extract_first()
        emi_start = Price.fromstring(emi_start)
        emi_start = emi_start.amount_float

        reviewer_names = [i.replace(" | ",",") for  i in response.css("div.info::text").extract()]
        reviewer_names = "| ".join(reviewer_names)

        reviews = response.css("div.testimonailsWrapper").css("p::text").extract()
        reviews = "| ".join(reviews)

        reviewer_photos = response.css("div.thumb").css("img::attr(src)").extract()[1::2]
        reviewer_photos = "| ".join(reviewer_photos)

        faq_questions = response.css("div.courseFaqContainer").css("h4.accordion::text").extract()
        faq_questions = "| ".join(faq_questions)

        faq_answers = response.css("div.courseFaqContainer").css("p::text").extract()
        faq_answers = "| ".join(faq_answers)

        what_u_learn = response.css("div.tabcontent").css("ul").css("li::text").extract()
        what_u_learn = [' '.join(item.split()) for item in what_u_learn]
        what_u_learn = list(filter(None, what_u_learn))
        what_u_learn = "| ".join(what_u_learn)

        modules = []
        for i in response.css("ul.curriculum-sections"):
            if i.css("h4.section-header::text").extract() is not None:
                modules.append(i.css("h4.section-header::text").extract())
        modules = [' '.join(item.split()) for item in modules[0]]
        modules = list(filter(None, modules))

        sub_modules = []
        for i in response.css("ul.section-content"):
            if i.css("span.lesson-title.course-item-title.button-load-item::text").extract() is not None:
                sub_modules.append(i.css("span.lesson-title.course-item-title.button-load-item::text").extract())
        # if response.css("span.lesson-title.course-item-title.button-load-item::text").extract() is not None:
        #     sub_modules.append(response.css("span.lesson-title.course-item-title.button-load-item::text").extract())



        #sub_modules = [i.strip() for i in response.xpath('//div[@class="tabcontent"]//span[@class="lesson-title course-item-title button-load-item"]/text()').extract()]

        #sub_modules = list(filter(None, sub_modules))
        #sub_modules = response.css("span.lesson-title.course-item-title.button-load-item::text").extract()
        #sub_modules = [' '.join(item.split()) for item in sub_modules]
        #sub_modules = list(filter(None, sub_modules))

        modlist = []
        modulenum = 1
        for i in range(len(modules)):
            # modlist.append('<?xml version="1.0"?><mainmodule>')
            module = f'<module{modulenum}><heading>{modules[i]}</heading><subheading>'
            modlist.append(module)
            # print(module)
            try:
                submodnum = 1
                for j in sub_modules[i]:
                    print(j)
                    submodule = f"<item{submodnum}>{j.strip()}</item{submodnum}>"
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
            "short_desc": short_desc,
            "what_u_learn": what_u_learn,
            "reviewer_names": reviewer_names,
            "reviews": reviews,
            "reviewer_photos": reviewer_photos,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers,
            # "modules": modules,
            # "sub_modules": sub_modules,
            "contents": contents,
            "display_price": display_price,
            "emi_start": emi_start,
            "currency": currency
        }




