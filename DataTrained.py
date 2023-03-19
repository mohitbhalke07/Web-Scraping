import scrapy
from ..items import DatatrainedItem
import dateutil.parser as parser
import re

class firstscrappy(scrapy.Spider):
    name = "scrap"
    start_urls = [
        "https://www.datatrained.com/"
    ]

    def parse(self, response):
        for link in response.xpath('//div[@class="ft-course"]').css("a::attr(href)")[:16]:
            yield response.follow(link.get(), callback=self.parse_content1)
        for link in response.xpath('//div[@class="ft-course"]').css("a::attr(href)")[16:]:
            yield response.follow(link.get(), callback=self.parse_content)


    def parse_content(self, response):
        items = DatatrainedItem()

        title = []
        if response.css("div.free-course-cont").css("h1::text").extract_first() is not None:
            title.append(response.css("div.free-course-cont").css("h1::text").extract_first())
        if response.xpath('//div[@class="course-heading-banner"]//span/text()').get() is not None:
            title.append(response.xpath('//div[@class="course-heading-banner"]//span/text()').get())
        title = title[0]

        main_desc = response.css("div.col-lg-12").css("p::text").extract_first()
        main_desc = f'<p>{main_desc}</p>'

        cover_img = "https://www.datatrained.com/" + response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")[1:-1]

        short_desc = []
        if response.xpath('//div[@class="free-course-cont"]//p/text()').get() is not None:
            short_desc.append(response.xpath('//div[@class="free-course-cont"]//p/text()').get())
        if response.xpath('//div[@class="place-heading col-flex"]//p/text()').get() is not None:
            short_desc.append(response.xpath('//div[@class="place-heading col-flex"]//p/text()').get())
        if response.xpath('//div[@class="curriculum"]//p/text()').get() is not None:
            short_desc.append(response.xpath('//div[@class="curriculum"]//p/text()').get())
        if response.css("div.what-you-learn").css("p::text").extract_first() is not None:
            short_desc.append(response.css("div.what-you-learn").css("p::text").extract_first())
        if response.css("div.what-you-learn").css("li::text").extract() is not None:
            short_desc.append(response.css("div.what-you-learn").css("li::text").extract())
        try:
            short_desc = short_desc[0]
        except:
            pass

        try:
            learn_type = response.css("div.box").css("h3::text")[0].extract()
            if learn_type != "100% online":
                learn_type = "100% online"
        except:
            learn_type = "100% online"


        total_duration = [i for i in re.findall(r'-?\d+\.?\d*',response.css("ul.ondemand").css("li::text")[0].extract()[:11])]
        total_video_content_unit = "Hours"

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

        inst_bio = [i.css("p::text").extract_first() for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]
        inst_bio = "| ".join(inst_bio)
        inst_img = [j.attrib["src"] for j in [i.css("img") for i in response.css("div.col-lg-4.col-md-6.col-sm-12")][:4]]
        inst_img = "| ".join(inst_img)

        display_price = response.css("div.details-pg").css("li.time-pg").css("span::text").extract_first()
        if display_price is not None:
            currency = "INR"
        else:
            currency = ""

        lang = response.css("div.bottom-content-2").css("li::text").extract_first()
        if lang != "English":
            lang = "English"
        prerequisite = [j.extract() for j in response.css("div.offset-lg-1.col-lg-6.col-md-6.flex").css("li::text")]
        prerequisite = "| ".join(prerequisite)

        what_u_learn = []
        if response.css("div.what-you-learn").css("p::text").extract_first() is not None:
            what_u_learn.append(response.css("div.what-you-learn").css("p::text").extract_first())
        if response.css("div.what-you-learn").css("li::text").extract() is not None:
            what_u_learn.append(response.css("div.what-you-learn").css("li::text").extract())

        filtered_list3 = list(filter(None, what_u_learn))
        what_u_learn = filtered_list3
        try:
            what_u_learn = what_u_learn[0]
            #what_u_learn = "| ".join(what_u_learn)
        except:
            pass

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


        yield {
            "title": title,
            "short_desc": short_desc,
            "main_desc": main_desc,
            "learn_type": learn_type,
            "cover_img": cover_img,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "inst_img": inst_img,
            "inst_bio": inst_bio,
            "lang": lang,
            "prerequisite": prerequisite,
            "what_u_learn": what_u_learn,
            "contents": "",
            "total_duration": total_duration,
            "total_video_content_unit": total_video_content_unit,
            "display_price": display_price,
            "currency": currency,
            "ratings": ratings,
            "reviewer_names": reviewer_names,
            "reviewer_photos": reviewer_photos,
            "reviews": reviews,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }


    def parse_content1(self, response):
        title = []
        for i in response.css("div.cont-box"):
            if i.css("h2") is not None:
                title.append(i.css("h2::text").extract_first())
        for j in response.css("div.cont-box"):
            if j.css("h4") is not None:
                title.append(j.css("h4::text").extract_first())
        filtered_list = list(filter(None, title))
        title = filtered_list

        main_desc = response.css("div.place-heading.top-heading").css("p").extract()

        short_desc = response.xpath('//div[@class="course-heading-banner"]//p/text()').get()

        try:
            cover_video = response.css("div.program-vedio").css("iframe").attrib["src"]
        except:
            pass
        cover_img = response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")[1:-1]

        total_duration = [i for i in re.findall(r'-?\d+\.?\d*', response.css("div.cont-box").css("li::text")[4].extract()[:10])]
        if total_duration is not None:
            total_video_content_unit = "Hours"
        else:
            total_video_content_unit = ""


        course_start_date = "07-10-2022"

        try:
            date = parser.parse(course_start_date)
            course_start_date = date.isoformat()
        except:
            pass
        try:
            course_duration = [i for i in re.findall(r'-?\d+\.?\d*', response.css("div.banner-detail").css("p::text")[1].extract().strip())]
            if course_duration is not None:
                course_duration_unit = "Months"
            else:
                course_duration_unit = ""
        except:
            course_duration = ""
            course_duration_unit = ""

        display_price = response.css("div.cont-box").css("li.time-pg").css("span::text").extract_first()[1:]
        currency = "INR"

        instructor = []
        for i in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6.col-12"):
           if i.css("h5") is not None:
               instructor.append(i.css("h5::text").extract_first())
           if i.css("h3") is not None:
               instructor.append(i.css("h3::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
           if j.css("h5") is not None:
               instructor.append(j.css("h5::text").extract_first())
        filtered_list1 = list(filter(None, instructor))
        instructor = filtered_list1[:6]
        instructor = "| ".join(instructor)

        inst_desig = []
        for i in response.css("div.col-lg-4.col-md-6.col-12"):
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
            elif i.css("h6") is not None:
                inst_desig.append(i.css("h6::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("h6") is not None:
                inst_desig.append(j.css("h6::text").extract_first())
        filtered_list2 = list(filter(None, inst_desig))
        inst_desig = filtered_list2[1:7]
        inst_desig = "| ".join(inst_desig)

        inst_bio = []
        for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12"):
            if i.css("p") is not None:
                inst_bio.append(i.css("p::text").extract_first().strip())
        for j in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("p") is not None:
                inst_bio.append(j.css("p::text").extract_first().strip())
        inst_bio = "| ".join(inst_bio)

        inst_img = []
        for i in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12")]:
            try:
                if i.attrib["src"] is not None:
                    inst_img.append(i.attrib["src"])
            except:
                pass
        for j in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ")]:
            try:
                if j.attrib["src"] is not None:
                    inst_img.append(j.attrib["src"])
            except:
                pass
        inst_img = "| ".join(inst_img)
        try:
            avg_sal_hike = response.css("div.col-lg-4.col-md-12.flex").css("img").attrib["alt"]
        except:
            avg_sal_hike = None
        try:
            high_sal = response.css("div.col-lg-4.col-md-12.flex").css("img")[1].attrib["alt"]
        except:
            high_sal = None

        reviewer_names = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("span.name") is not None:
                reviewer_names.append(i.css("span.name::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3") or response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if j.css("h5") is not None:
                reviewer_names.append(j.css("h5::text").extract_first())
        filtered_list3 = list(filter(None, reviewer_names))
        reviewer_names = "| ".join(filtered_list3)

        reviewer_photos = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("img") is not None:
                reviewer_photos.append(i.css("img").attrib["src"])
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("img") is not None:
                reviewer_photos.append(j.css("img").attrib["src"])
        reviewer_photos = "| ".join(reviewer_photos)

        reviews = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("p") is not None:
                reviews.append(i.css("p::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("p") is not None:
                reviews.append(j.css("p::text").extract_first())
        reviews = "| ".join(reviews)

        try:
            faq_questions = [j.extract().strip().replace("\r\n", "").replace("                            ", "").replace("                           ", "") for j in [i.css("h3::text") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_questions = "|".join(faq_questions)
            faq_answers = [j.css("p::text").extract_first().strip().replace("\r\n","").replace("                                 ","") for j in [i.css("div.card") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_answers = "|".join(faq_answers)
        except:
            faq_questions = None
            faq_answers = None

        modules = [j.extract() for j in response.css("div.accordion.md-accordion").css("h5.mb-0::text")]
        sub_modules = [i.css("span::text").extract() for i in response.css("div.col-lg-12")][:-10]
        sub_modules = sub_modules[3:]
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
        #print(contents)


        yield {
            "title": title,
            "short_desc": short_desc,
            "main_desc": main_desc,
            "cover_img": cover_img,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "inst_img": inst_img,
            "inst_bio": inst_bio,
            "contents": contents,
            "cover_video": cover_video,
            "display_price": display_price,
            "currency": currency,
            "total_duration": total_duration,
            "total_video_content_unit": total_video_content_unit,
            "course_start_date": course_start_date,
            "course_duration": course_duration,
            "course_duration_unit":course_duration_unit,
            "ratings": 4.0,
            "reviewer_names": reviewer_names,
            "reviewer_photos": reviewer_photos,
            "reviews": reviews,
            "avg_sal_hike": avg_sal_hike,
            "high_sal": high_sal,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }








    def parser_contents1(self, response):
        items = DatatrainedItem()

        title = []
        for i in response.css("div.cont-box"):
            if i.css("h2") is not None:
                title.append(i.css("h2::text").extract_first())
        for j in response.css("div.cont-box"):
            if j.css("h4") is not None:
                title.append(j.css("h4::text").extract_first())
        filtered_list = list(filter(None, title))
        title = filtered_list

        main_desc = response.css("div.place-heading.top-heading").css("p").extract()
        short_desc = response.xpath('//div[@class="course-heading-banner"]//p/text()').get()

        try:
            cover_video = response.css("div.program-vedio").css("iframe").attrib["src"]
        except:
            pass
        cover_img = response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")[1:-1]

        content_module = []
        for i in response.css("div.syllabus-newbox"):
            if i.css("h4") is not None:
                content_module.append(i.css("h4::text").extract_first())
        for j in response.css("div.syllabus-newbox"):
            if j.css("span") is not None:
                content_module.append(j.css("span.module-heading::text").extract_first())
        for k in response.css("div.card-body"):
            if k.css("span") is not None:
                content_module.append(k.css("span::text").extract_first())
        filtered_list1_0 = list(filter(None, content_module))
        content_module = "| ".join(filtered_list1_0)

        total_duration = "".join([i for i in response.css("div.cont-box").css("li::text")[4].extract()[:10] if i!="+"])

        course_start_date = response.css("div.banner-detail").css("p::text")[2].extract().strip()
        try:
            date = parser.parse(course_start_date)
            course_start_date = date.isoformat()
        except:
            pass
        course_duration = response.css("div.banner-detail").css("p::text")[1].extract().strip()
        display_price = response.css("div.cont-box").css("li.time-pg").css("span::text").extract_first()

        instructor = []
        for i in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6.col-12"):
           if i.css("h5") is not None:
               instructor.append(i.css("h5::text").extract_first())
           if i.css("h3") is not None:
               instructor.append(i.css("h3::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
           if j.css("h5") is not None:
               instructor.append(j.css("h5::text").extract_first())
        filtered_list1 = list(filter(None, instructor))
        instructor = filtered_list1[:6]
        instructor = "| ".join(instructor)

        inst_desig = []
        for i in response.css("div.col-lg-4.col-md-6.col-12"):
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
            elif i.css("h6") is not None:
                inst_desig.append(i.css("h6::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("h6") is not None:
                inst_desig.append(j.css("h6::text").extract_first())
        filtered_list2 = list(filter(None, inst_desig))
        inst_desig = filtered_list2[1:7]
        inst_desig = "| ".join(inst_desig)

        inst_bio = []
        for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12"):
            if i.css("p") is not None:
                inst_bio.append(i.css("p::text").extract_first().strip())
        for j in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("p") is not None:
                inst_bio.append(j.css("p::text").extract_first().strip())
        inst_bio = "| ".join(inst_bio)

        inst_img = []
        for i in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12")]:
            try:
                if i.attrib["src"] is not None:
                    inst_img.append(i.attrib["src"])
            except:
                pass
        for j in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ")]:
            try:
                if j.attrib["src"] is not None:
                    inst_img.append(j.attrib["src"])
            except:
                pass
        inst_img = "| ".join(inst_img)
        try:
            avg_sal_hike = response.css("div.col-lg-4.col-md-12.flex").css("img").attrib["alt"]
        except:
            avg_sal_hike = None
        try:
            high_sal = response.css("div.col-lg-4.col-md-12.flex").css("img")[1].attrib["alt"]
        except:
            high_sal = None

        reviewer_names = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("span.name") is not None:
                reviewer_names.append(i.css("span.name::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3") or response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if j.css("h5") is not None:
                reviewer_names.append(j.css("h5::text").extract_first())
        filtered_list3 = list(filter(None, reviewer_names))
        reviewer_names = "| ".join(filtered_list3)

        reviewer_photos = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("img") is not None:
                reviewer_photos.append(i.css("img").attrib["src"])
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("img") is not None:
                reviewer_photos.append(j.css("img").attrib["src"])
        reviewer_photos = "| ".join(reviewer_photos)

        reviews = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("p") is not None:
                reviews.append(i.css("p::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("p") is not None:
                reviews.append(j.css("p::text").extract_first())
        reviews = "| ".join(reviews)

        try:
            faq_questions = [j.extract().strip().replace("\r\n", "").replace("                            ", "").replace("                           ", "") for j in [i.css("h3::text") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_questions = "|".join(faq_questions)
            faq_answers = [j.css("p::text").extract_first().strip().replace("\r\n","").replace("                                 ","") for j in [i.css("div.card") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_answers = "|".join(faq_answers)
        except:
            faq_questions = None
            faq_answers = None

        modules = [j.extract() for j in response.css("div.accordion.md-accordion").css("h5.mb-0::text")]
        sub_modules = [i.css("span::text").extract() for i in response.css("div.col-lg-12")][:-10]
        sub_modules = sub_modules[3:]
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
        #print(contents)


        yield {
            "title": title,
            "short_desc": short_desc,
            "main_desc": main_desc,
            "cover_img": cover_img,
            "contents": contents,
            "cover_video": cover_video,
            "display_price": display_price,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "content_module": content_module,
            "total_duration": total_duration,
            "course_start_date": course_start_date,
            "course_duration": course_duration,
            "inst_img": inst_img,
            "inst_bio": inst_bio,
            "ratings": 4.0,
            "reviewer_names": reviewer_names,
            "reviewer_photos": reviewer_photos,
            "reviews": reviews,
            "avg_sal_hike": avg_sal_hike,
            "high_sal": high_sal,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }

    def parser_contents2(self, response):
        items = DatatrainedItem()

        title = []
        for i in response.css("div.cont-box"):
            if i.css("h2") is not None:
                title.append(i.css("h2::text").extract_first())
        for j in response.css("div.cont-box"):
            if j.css("h4") is not None:
                title.append(j.css("h4::text").extract_first())
        filtered_list = list(filter(None, title))
        title = filtered_list

        main_desc = response.css("div.place-heading.top-heading").css("p").extract()
        short_desc = response.xpath('//div[@class="course-heading-banner"]//p/text()').get()

        cover_video = response.css("div.program-vedio").css("iframe").attrib["src"]
        cover_img = response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")[1:-1]

        content_module = []
        for i in response.css("div.syllabus-newbox"):
            if i.css("h4") is not None:
                content_module.append(i.css("h4::text").extract_first())
        for j in response.css("div.syllabus-newbox"):
            if j.css("span") is not None:
                content_module.append(j.css("span.module-heading::text").extract_first())
        for k in response.css("div.card-body"):
            if k.css("span") is not None:
                content_module.append(k.css("span::text").extract_first())
        filtered_list1_0 = list(filter(None, content_module))
        content_module = "| ".join(filtered_list1_0)
        total_duration = "".join([i for i in response.css("div.cont-box").css("li::text")[4].extract()[:10] if i != "+"])

        course_start_date = response.css("div.banner-detail").css("p::text")[3].extract().strip()
        try:
            date = parser.parse(course_start_date)
            course_start_date = date.isoformat()
        except:
            pass
        course_duration = response.css("div.banner-detail").css("p::text")[2].extract().strip()
        display_price = response.css("div.cont-box").css("li.time-pg").css("span::text").extract_first()
        instructor = []
        for i in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6.col-12"):
            if i.css("h5") is not None:
                instructor.append(i.css("h5::text").extract_first())
            if i.css("h3") is not None:
                instructor.append(i.css("h3::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("h5") is not None:
                instructor.append(j.css("h5::text").extract_first())
        for k in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6"):
            if k.css("h3") is not None:
                instructor.append(k.css("h3::text").extract_first())
        filtered_list1 = list(filter(None, instructor))
        instructor = filtered_list1[:5]
        instructor = "| ".join(instructor)

        inst_desig = []
        for i in response.css("div.col-lg-4.col-md-6.col-12"):
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
            elif i.css("h6") is not None:
                inst_desig.append(i.css("h6::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("h6") is not None:
                inst_desig.append(j.css("h6::text").extract_first())
        for k in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6"):
            if k.css("span") is not None:
                inst_desig.append(k.css("span::text").extract_first())
        filtered_list2 = list(filter(None, inst_desig))
        inst_desig = filtered_list2[1:7]
        inst_desig = "| ".join(inst_desig)

        inst_bio = []
        for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12"):
            if i.css("p") is not None:
                inst_bio.append(i.css("p::text").extract_first().strip())
        for j in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css(
                "section.instructors").css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("p") is not None:
                inst_bio.append(j.css("p::text").extract_first().strip())
        inst_bio = "| ".join(inst_bio)

        inst_img = []
        for i in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12")]:
            try:
                if i.attrib["src"] is not None:
                    inst_img.append(i.attrib["src"])
            except:
                pass
        for j in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ")]:
            try:
                if j.attrib["src"] is not None:
                    inst_img.append(j.attrib["src"])
            except:
                pass
        inst_img = "| ".join(inst_img)
        try:
            avg_sal_hike = response.css("div.col-lg-4.col-md-12.flex").css("img").attrib["alt"]
        except:
            avg_sal_hike = None
        try:
            high_sal = response.css("div.col-lg-4.col-md-12.flex").css("img")[1].attrib["alt"]
        except:
            high_sal = None

        reviewer_names = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3") or response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if i.css("span.name") is not None:
                reviewer_names.append(i.css("span.name::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3") or response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if j.css("h5") is not None:
                reviewer_names.append(j.css("h5::text").extract_first())
        for k in response.css("div.col-lg-6.col-md-6.col-12.my-3"):
            if k.css("span") is not None:
                reviewer_names.append(k.css("span.name::text").extract_first())

        filtered_list3 = list(filter(None, reviewer_names))
        reviewer_names = "| ".join(filtered_list3)

        reviewer_photos = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("img") is not None:
                reviewer_photos.append(i.css("img").attrib["src"])
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("img") is not None:
                reviewer_photos.append(j.css("img").attrib["src"])
        for k in response.css("div.col-lg-6.col-md-6.col-12.my-3"):
            if k.css("img") is not None:
                reviewer_photos.append(k.css("img").attrib["src"])
        reviewer_photos = "| ".join(reviewer_photos)

        reviews = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("p") is not None:
                reviews.append(i.css("p::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("p") is not None:
                reviews.append(j.css("p::text").extract_first())
        for k in response.css("div.col-lg-6.col-md-6.col-12.my-3"):
            if k.css("p") is not None:
                reviews.append(k.css("p::text").extract_first())
        reviews = "| ".join(reviews)
        try:
            faq_questions = [j.extract().strip().replace("\r\n", "").replace("                            ", "").replace("                           ", "") for j in [i.css("h3::text") or i.css("h5::text") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_questions = "|".join(faq_questions)
            faq_answers = [j.css("p::text").extract_first().strip().replace("\r\n","").replace("                                 ","") for j in [i.css("div.card") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_answers = "|".join(faq_answers)
        except:
            faq_questions = None
            faq_answers = None

        module = []
        for i in response.css("div.accordion.md-accordion").css("h4.mb-0::text").extract():
            if i is not None:
                module.append(i.replace("                           ","").replace("\n",""))
        for j in response.css("div.accordion.md-accordion").css("h5.mb-0::text").extract():
            if j is not None:
                module.append(j.replace("                           ","").replace("\n",""))
        modules = []
        for ele in module:
            if ele.strip():
                modules.append(ele)
        modules = modules[:3]

        sub_modules = [i for i in response.css("div.accordion.md-accordion").css("li::text").extract()][:-20]
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
            "title": title,
            "short_desc": short_desc,
            "main_desc": main_desc,
            "cover_img": cover_img,
            "contents": contents,
            "cover_video": cover_video,
            "display_price": display_price,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "content_module": content_module,
            "total_duration": total_duration,
            "course_start_date": course_start_date,
            "course_duration": course_duration,
            "inst_img": inst_img,
            "inst_bio": inst_bio,
            "ratings": 4.0,
            "reviewer_names": reviewer_names,
            "reviewer_photos": reviewer_photos,
            "reviews": reviews,
            "avg_sal_hike": avg_sal_hike,
            "high_sal": high_sal,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }

    def parser_contents3(self, response):
        items = DatatrainedItem()

        title = []
        for i in response.css("div.cont-box"):
            if i.css("h2") is not None:
                title.append(i.css("h2::text").extract_first())
        for j in response.css("div.cont-box"):
            if j.css("h4") is not None:
                title.append(j.css("h4::text").extract_first())
        filtered_list = list(filter(None, title))
        title = filtered_list

        main_desc = response.css("div.place-heading.top-heading").css("p").extract()
        short_desc = response.xpath('//div[@class="course-heading-banner"]//p/text()').get()

        try:
            cover_video = response.css("div.program-vedio").css("iframe").attrib["src"]
        except:
            pass
        cover_img = response.css("div.pg-program-vedio").attrib["style"].split()[1].replace("url", "").replace(";", "")[1:-1]

        content_module = []
        for i in response.css("div.syllabus-newbox"):
            if i.css("h4") is not None:
                content_module.append(i.css("h4::text").extract_first())
        for j in response.css("div.syllabus-newbox"):
            if j.css("span") is not None:
                content_module.append(j.css("span.module-heading::text").extract_first())
        for k in response.css("div.card-body"):
            if k.css("span") is not None:
                content_module.append(k.css("span::text").extract_first())
        filtered_list1_0 = list(filter(None, content_module))
        content_module = "| ".join(filtered_list1_0)

        total_duration = "".join([i for i in response.css("div.cont-box").css("li::text")[4].extract()[:10] if i != "+"])

        course_start_date = response.css("div.banner-detail").css("p::text")[3].extract().strip()
        try:
            date = parser.parse(course_start_date)
            course_start_date = date.isoformat()
        except:
            pass

        course_duration = response.css("div.banner-detail").css("p::text")[2].extract().strip()
        display_price = response.css("div.cont-box").css("li.time-pg").css("span::text").extract_first()

        instructor = []
        for i in response.css("div.row.justify-content-center").css("div.col-lg-4.col-md-6.col-12"):
           if i.css("h5") is not None:
               instructor.append(i.css("h5::text").extract_first())
           if i.css("h3") is not None:
               instructor.append(i.css("h3::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
           if j.css("h5") is not None:
               instructor.append(j.css("h5::text").extract_first())
        filtered_list1 = list(filter(None, instructor))
        instructor = filtered_list1[:5]
        instructor = "| ".join(instructor)

        inst_desig = []
        for i in response.css("div.col-lg-4.col-md-6.col-12"):
            if i.css("span") is not None:
                inst_desig.append(i.css("span::text").extract_first())
            elif i.css("h6") is not None:
                inst_desig.append(i.css("h6::text").extract_first())
        for j in response.css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("h6") is not None:
                inst_desig.append(j.css("h6::text").extract_first())
        filtered_list2 = list(filter(None, inst_desig))
        inst_desig = filtered_list2[1:7]
        inst_desig = "| ".join(inst_desig)

        inst_bio = []
        for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12"):
            if i.css("p") is not None:
                inst_bio.append(i.css("p::text").extract_first().strip())
        for j in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ") or response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12"):
            if j.css("p") is not None:
                inst_bio.append(j.css("p::text").extract_first().strip())
        inst_bio = "| ".join(inst_bio)

        inst_img = []
        for i in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-12")]:
            try:
                if i.attrib["src"] is not None:
                    inst_img.append(i.attrib["src"])
            except:
                pass
        for j in [i.css("img") for i in response.css("section.instructors").css("div.col-lg-4.col-md-6.col-sm-12 ")]:
            try:
                if j.attrib["src"] is not None:
                    inst_img.append(j.attrib["src"])
            except:
                pass
        inst_img = "| ".join(inst_img)

        try:
            avg_sal_hike = response.css("div.col-lg-4.col-md-12.flex").css("img").attrib["alt"]
        except:
            avg_sal_hike = None
        try:
            high_sal = response.css("div.col-lg-4.col-md-12.flex").css("img")[1].attrib["alt"]
        except:
            high_sal = None

        reviewer_names = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("span.name") is not None:
                reviewer_names.append(i.css("span.name::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3") or response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if j.css("h5") is not None:
                reviewer_names.append(j.css("h5::text").extract_first())
        filtered_list3 = list(filter(None, reviewer_names))
        reviewer_names = "| ".join(filtered_list3)

        reviewer_photos = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("img") is not None:
                reviewer_photos.append(i.css("img").attrib["src"])
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("img") is not None:
                reviewer_photos.append(j.css("img").attrib["src"])
        reviewer_photos = "| ".join(reviewer_photos)

        reviews = []
        for i in response.css("div.col-xl-4.col-lg-6.col-md-6.col-12.my-3"):
            if i.css("p") is not None:
                reviews.append(i.css("p::text").extract_first())
        for j in response.css("div.col-xl-4.col-lg-6.col-md-6.mt-3.mb-3"):
            if j.css("p") is not None:
                reviews.append(j.css("p::text").extract_first())
        reviews = "| ".join(reviews)

        try:
            faq_questions = [j.extract().strip().replace("\r\n", "").replace("                            ", "").replace("                           ", "") for j in [i.css("h3::text") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_questions = "|".join(faq_questions)
            faq_answers = [j.css("p::text").extract_first().strip().replace("\r\n","").replace("                                 ","") for j in [i.css("div.card") for i in response.css("div.col-lg-10.col-sm-12")][0]]
            faq_answers = "|".join(faq_answers)
        except:
            faq_questions = None
            faq_answers = None

        module = []
        for i in response.css("div.accordion.md-accordion").css("h5.mb-0::text"):
            if i is not None:
                module.append(i.extract())
        for j in response.css("div.accordion.md-accordion").css("h4.mb-0::text"):
            if j is not None:
                module.append(j.extract().strip())
        modules = []
        for ele in module:
            if ele.strip():
                modules.append(ele)
        modules = modules[:-3]

        sub_modules = [i.css("li::text").extract() for i in response.css("div.col-lg-12")][:-10]
        sub_modules = sub_modules[3:]
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
        #print(contents)

        yield {
            "title": title,
            "short_desc": short_desc,
            "main_desc": main_desc,
            "cover_img": cover_img,
            "contents": contents,
            "cover_video": cover_video,
            "display_price": display_price,
            "instructor": instructor,
            "inst_desig": inst_desig,
            "content_module": content_module,
            "total_duration": total_duration,
            "course_start_date": course_start_date,
            "course_duration": course_duration,
            "inst_img": inst_img,
            "inst_bio": inst_bio,
            "ratings": 4.0,
            "reviewer_names": reviewer_names,
            "reviewer_photos": reviewer_photos,
            "reviews": reviews,
            "avg_sal_hike": avg_sal_hike,
            "high_sal": high_sal,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }