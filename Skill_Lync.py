import scrapy
import requests
import re
import dateutil.parser as parser
from bs4 import BeautifulSoup
import requests

class skill_lyncscrapy(scrapy.Spider):
    name = "skill_lync"
    start_urls = [
        "https://skill-lync.com/all-courses"
    ]

    def parse(self, response):
        links1 = response.css("div.css-i6b4pj").css("a::attr(href)").extract()[::2][:-1]
        links2 = response.css("div.css-i6b4pj").css("a::attr(href)").extract()
        del links2[:3:2]
        links2 = links2[:-1]

        for link in links1:
            x = "https://skill-lync.com" + link
            yield scrapy.Request(x, callback=self.parser_contents1, cb_kwargs={"url":x})
        for link in links2:
            y = "https://skill-lync.com" + link
            yield scrapy.Request(y, callback=self.parser_contents2, cb_kwargs={"url":y})

    def parser_contents1(self, response, url):
        domains = [i.lower().replace(" ","-") for i in response.xpath('//div[@class="css-1kidr44"]//button/text()').extract()[:5]]
        domains_links = []
        for i in domains:
            domains_links.append("https://skill-lync.com/mechanical-engineering-courses/top-job-leading-courses/" + i)

        for j in domains_links:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
                "Accept-Encoding": "gzip, deflate",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                "Connection": "close", "Upgrade-Insecure-Requests": "1"}
            response = requests.get(j, headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")

            for link in [i.find("a", href=True)["href"] for i in soup.find_all("div", class_="css-wede0r")]:
                li = "https://skill-lync.com" + link
                yield scrapy.Request(li, callback=self.main_contents, cb_kwargs={"jslink":link})



    def parser_contents2(self, response, url):
        domains = [i.lower().replace(" ","-") for i in response.xpath('//div[@class="css-1kidr44"]//button/text()').extract()[:5]]
        domains_links = []
        for i in domains:
            domains_links.append("https://skill-lync.com/electrical-engineering-courses/courses-by-domain/" + i)

        for j in domains_links:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
                "Accept-Encoding": "gzip, deflate",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                "Connection": "close", "Upgrade-Insecure-Requests": "1"}
            response = requests.get(j, headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")

            for link in [i.find("a", href=True)["href"] for i in soup.find_all("div", class_="css-wede0r")]:
                li = "https://skill-lync.com" + link
                yield scrapy.Request(li, callback=self.main_contents, cb_kwargs={"jslink":link})

    def main_contents(self, response, jslink):
        #print(response)

        title = response.xpath('//h1[@class="chakra-text css-ayk7hb"]/text()').extract_first()

        short_desc = response.xpath('//*[@id="__next"]/div[7]/div[2]/div[1]/div[1]/div/p/a/text()').extract_first()

        cover_img = response.css("div.css-ljkl4a").css("span").css("img::attr(src)").extract()[1]

        course_duration = re.findall(r'-?\d+\.?\d*',response.xpath('//p[@class="chakra-text Course_boldTagWithGreenColor__UTGw9 css-mm3bi8"]//b/text()').extract_first())
        course_duration_units = re.findall(r'[a-z]+',response.xpath('//p[@class="chakra-text Course_boldTagWithGreenColor__UTGw9 css-mm3bi8"]//b/text()').extract_first())

        try:
            delivery_method = response.xpath('//p[@class="chakra-text Course_boldTagWithGreenColor__UTGw9 css-mm3bi8"]/text()').extract_first().split("|")[1]
        except:
            delivery_method = ""

        what_u_learn = [i.rstrip().replace(",","") for i in response.xpath('//div[@class="Course_htmlBox__LKuxT"]//li/text() | //div[@class="Course_seoFooterSection__tSWE4"]//ul/li//span/text() | //div[@class="Course_seoFooterSection__tSWE4"]//ul/li/text()').extract()]
        what_u_learn = "| ".join(what_u_learn)

        faq_questions = [i.rstrip() for i in response.xpath('//div[@class="Course_seoFooterSection__tSWE4"]//ol//li//strong/text() | //div[@class="Course_seoFooterSection__tSWE4"]//p//strong/text() | //div[@class="Course_seoFooterSection__tSWE4"]//p//strong//span/text()').extract()]
        faq_questions = list(filter(None, faq_questions))

        modules = response.xpath('//p[@class="chakra-text css-1nng0n0"]/text() | //div[@class="chakra-stack css-awxh4g"]//p[@class="chakra-text css-43hrvt"]/text()').extract()
        modules = [re.sub(r'Week ', '', i) for i in modules]
        modules = [re.sub(r'Module -', '', i) for i in modules]
        modules = [re.sub(r'Module ', '', i) for i in modules]
        modules = [re.sub(r'[0-9]', '', i) for i in modules]
        modules = [i.replace("-  ","") for i in modules]
        modules = [i.replace("-", "") for i in modules]
        modules = [i.replace("- ", "") for i in modules]

        sub_modules = response.xpath('//div[@class="Course_htmlBox__LKuxT css-wefj69"]//ul//li/text() | //div[@class="chakra-stack css-58ov0d"]//ul//li/span/text() | //div[@class="chakra-stack css-58ov0d"]//ul//li/text() | //div[@class="chakra-stack css-58ov0d"]//div[@class="Course_htmlBox__LKuxT"]//ul//li//span/text()').extract()
        sub_modules = [re.sub('In this week the following topics are covered', '',i) for i in sub_modules]
        sub_modules = [re.sub('In this week, the following topics are covered', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this week. the following topics are covered', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this session, we will learn about', '', i) for i in sub_modules]
        sub_modules = [re.sub("In this week's session, we will learn about:", '', i) for i in sub_modules]
        sub_modules = [re.sub('The topics that will be covered in this module are', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this session, we will:', '', i) for i in sub_modules]
        sub_modules = [re.sub('The following topics are discussed this week.', '', i) for i in sub_modules]
        sub_modules = [re.sub('The following topics are covered this week:', '', i) for i in sub_modules]
        sub_modules = [re.sub('The students will be introduced to', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this module, the students will understand,', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this module, the following topics are covered.,', '', i) for i in sub_modules]
        sub_modules = [re.sub('In this session, you will learn,', '', i) for i in sub_modules]
        sub_modules = [re.sub('The students will be introduced to', '', i) for i in sub_modules]
        sub_modules = [i.replace("\xa0", "") for i in sub_modules]
        sub_modules = [i.strip() for i in sub_modules]
        sub_modules = list(filter(None, sub_modules))

        TAG_RE = re.compile(r'<[^>]+>')
        def remove_tags(text):
            return TAG_RE.sub('', text)

        faq_question = [i.replace("\xa0","") for i in response.xpath('//div[@class="Course_seoFooterSection__tSWE4"]//ol//li//strong/text() | //div[@class="Course_seoFooterSection__tSWE4"]//ol//li//strong//span/text() | //div[@class="Course_seoFooterSection__tSWE4"]//p//strong/text()').extract()]
        faq_question = [re.sub(r'[0-9]', '', i) for i in faq_question]
        faq_question = list(filter(None, faq_question))
        faq_question = "| ".join(faq_question)

        faq_answers = [remove_tags(i).replace("\xa0","") for i in response.xpath('//div[@class="Course_seoFooterSection__tSWE4"]//h2[2]/following-sibling::p//span/text() | //ol//following-sibling::p | //*[@class="Course_seoFooterSection__tSWE4"]//h2[contains(text(),"FAQs")]/..//h2[contains(text(),"FAQs")]//following-sibling::p/text() | //*[@class="Course_seoFooterSection__tSWE4"]//span[contains(text(),"FAQs")]/..//following-sibling::p//span/text() | //div[@class="Course_seoFooterSection__tSWE4"]//ol//following-sibling::li/text()').extract()]
        faq_answers = list(filter(None, faq_answers))
        faq_answers = "| ".join(faq_answers)

        # //ol//following-sibling::p
        # //div[@class="Course_seoFooterSection__tSWE4"]//h2[3]/following-sibling::p
        # //*[@class='Course_seoFooterSection__tSWE4']//strong[contains(text(),'FAQs')]/..//following-sibling::p
        # //*[@class='Course_seoFooterSection__tSWE4']//h2[contains(text(),'FAQs')]/..//h2[contains(text(),'FAQs')]//following-sibling::p (strong not)
        # //*[@class='Course_seoFooterSection__tSWE4']//h2[contains(text(),'FAQs')]/..//h2[contains(text(),'FAQs')]//following-sibling::p/strong
        # //*[@class='Course_seoFooterSection__tSWE4']//h2[contains(text(),'FAQs')]/..//h2[contains(text(),'FAQs')]//following-sibling::p/text

        json_link = f'https://skill-lync.com/_next/data/prod-build-154{jslink}.json'

        yield scrapy.Request(json_link, callback=self.json_contents, cb_kwargs={"link": response, "title": title,
            "short_desc": short_desc,
            "cover_img": cover_img,
            "course_duration": course_duration,
            "course_duration_units": course_duration_units,
            "delivery_method": delivery_method,
            "what_u_learn": what_u_learn,
            "modules": modules,
            "sub_modules": sub_modules,
            "faq_question": faq_question,
            "faq_answers": faq_answers
            })


    def json_contents(self, response, link, title, short_desc, what_u_learn, cover_img, course_duration, course_duration_units, delivery_method, modules, sub_modules, faq_question, faq_answers):

        data = response.json()
        TAG_RE = re.compile(r'<[^>]+>')

        def remove_tags(text):
            return TAG_RE.sub('', text)

        reviewer_name = [i["StudentName"] for i in data["pageProps"]["courseData"]["RatingsReviewsSection"]["List"]]
        reviewer_name = "| ".join(reviewer_name)

        reviews = [i["Description"] for i in data["pageProps"]["courseData"]["RatingsReviewsSection"]["List"]]
        reviews = "| ".join(reviews)

        reviewer_photo = [i["StudentPhoto"] for i in data["pageProps"]["courseData"]["RatingsReviewsSection"]["List"]]
        reviewer_photo = "| ".join(reviewer_photo)

        display_price = data["pageProps"]["courseData"]["FlexiblePricingSection"]["Plans"]["INR"][0]["Price"]["Amount"]
        currency = "INR"

        if sub_modules is None:
            sub_modules = []
            try:
                for i in range(len(data["pageProps"]["courseData"]["SyllabusWithTrack"]["TrackList"])):
                    for j in range(len(data["pageProps"]["courseData"]["SyllabusWithTrack"]["TrackList"][i]["CourseList"])):
                        for k in range(len(data["pageProps"]["courseData"]["SyllabusWithTrack"]["TrackList"][i]["CourseList"][j]["SyllabusList"])):
                            sub_modules.append(data["pageProps"]["courseData"]["SyllabusWithTrack"]["TrackList"][i]["CourseList"][j]["SyllabusList"][k]["Title"])
            except:
                try:
                    for i in range(1,len(data["pageProps"]["courseData"]["SyllabusSection"]["CourseList"])):
                        for j in range(len(data["pageProps"]["courseData"]["SyllabusSection"]["CourseList"][i]["SyllabusList"])):
                                sub_modules.append(data["pageProps"]["courseData"]["SyllabusSection"]["CourseList"][i]["SyllabusList"][j]["Title"])
                except:
                    for i in range(len(data["pageProps"]["courseData"]["SyllabusSection"]["SyllabusList"])):
                            sub_modules.append(remove_tags(data["pageProps"]["courseData"]["SyllabusSection"]["SyllabusList"][i]["Description"]).strip().split(";")[0])

        sub_modules = [sub_modules[i:i + 5] for i in range(0, len(sub_modules), 5)]

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
        else:
            for i in range(len(modules)):
                module = f"<p><strong>Module {modulenum}: {modules[i]}</strong>"
                modlist.append(module)
                modulenum += 1
                modlist.append('</p>')

        contents = "".join(modlist)

        try:
            main_desc = remove_tags(data["pageProps"]["courseData"]["CourseOverviewSection"]["Description"])
            main_desc = f'<p>{main_desc.strip()}</p>'
        except:
            main_desc = f'<p>{short_desc}</p>'

        try:
            syllabus_pdf = data["pageProps"]["courseData"]["DownloadSyllabusSection"]["LeadCaptureForm"]["Button"]["ExternalLink"]
        except:
            syllabus_pdf = ""

        yield {
            "link": link,
            "title": title,
            "cover_img": cover_img,
            "main_desc": main_desc,
            "short_desc": short_desc,
            "syllabus_pdf": syllabus_pdf,
            "course_duration": course_duration,
            "course_duration_units": course_duration_units,
            "delivery_method": delivery_method,
            "what_u_learn": what_u_learn,
            "faq_question": faq_question,
            "faq_answers": faq_answers,
            "contents": contents,
            "reviewer_name": reviewer_name,
            "reviews": reviews,
            "reviewer_photo": reviewer_photo,
            "display_price": display_price,
            "currency": currency
        }
