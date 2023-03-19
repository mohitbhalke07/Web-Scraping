import scrapy
from ..items import ImarticusItem
import requests
from bs4 import BeautifulSoup
import json
import re


class secondscrapy(scrapy.Spider):
    name = "scrapimarticus"
    start_urls = [
        "https://imarticus.org/"
    ]

    def parse(self, response):
        for link in response.xpath('//ul[@class="ulwidth8"]//li//a/@href').getall()[:-5]:
            #print(response.urljoin(link.get()))
            yield scrapy.Request("https://imarticus.org"+link, callback=self.parser_contents1)

        # for link in response.css("div.allprogrammargin").css("a.course_link::attr(href)"):
        #     #print(response.urljoin(link.get()))
        #     yield scrapy.Request(response.urljoin(link.get()), callback=self.parser_contents1)
        #
        # for link in response.css("div.allprogrammargin2").css("a.course_link::attr(href)"):
        #     #print(response.urljoin(link.get()))
        #     yield response.follow(response.urljoin(link.get()), callback=self.parser_contents1)


    def parser_contents2(self, response,title, main_desc, teaching_mode, emi_start, instructor, inst_bio, reviewer_names, reviews, contents,
                         display_price, faq_questions, faq_answers):
        items = ImarticusItem()

        modules = []
        if response.css("div.curriculamBoxInnerHidden").css("div.BasicContentTitle::text").extract() is not None:
            modules.append(response.css("div.curriculamBoxInnerHidden").css("div.BasicContentTitle::text").extract())
        if response.css("div.curriculamBoxInnerHidden").css("h3.BasicContentTitle::text").extract() is not None:
            modules.append(response.css("div.curriculamBoxInnerHidden").css("h3.BasicContentTitle::text").extract())
        if response.css("h1.fsd_accHeader1__S4XBI::text").extract() is not None:
            modules.append(response.css("h1.fsd_accHeader1__S4XBI::text").extract())
        modules = list(filter(None, modules))
        try:
            modules = modules[0]
        except:
            pass

        sub_modules = []
        for i in response.css("div.curriculamBoxInnerHidden"):
            if i.css("li.BasicContentList::text").extract() is not None:
                sub_modules.append(i.css("li.BasicContentList::text").extract())
        for j in response.css("div.accordion-collapse.collapse.show"):
            if j.css("div.fsd_rightacc__rxyEk").css("p::text").extract() is not None:
                sub_modules.append(j.css("div.fsd_rightacc__rxyEk").css("p::text").extract())
        for k in response.css("div.accordion-collapse.collapse.show"):
            if k.css("li::text").extract() is not None:
                sub_modules.append(k.css("li::text").extract())
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
                        submodule = f"<item{submodnum}>{j}</item{submodnum}>"
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




        yield {
            #"response": response,
            "title": title,
            "main_desc": main_desc,
            "teaching_mode": teaching_mode,
            "emi_start": emi_start,
            "instructor": instructor,
            "inst_bio": inst_bio,
            "reviewer_names": reviewer_names,
            "reviews": reviews,
            "contents": contents,
            "display_price": display_price,
            "faq_questions": faq_questions,
            "faq_answers": faq_answers
        }



    def parser_contents1(self, response):
        items = ImarticusItem()

        title = []
        if response.css("h1.cibopH.dscHeader::text").extract_first() is not None:
            title.append(response.css("h1.cibopH.dscHeader::text").extract_first())
        if response.css("h1.dscDsp.dscHeader::text").extract_first() is not None:
            title.append(response.css("h1.dscDsp.dscHeader::text").extract_first())
        if response.css("div.dspbannerHead.fancy").css("span::text").extract_first() is not None:
            title.append(response.css("div.dspbannerHead.fancy").css("span::text").extract_first())
        if response.css("h1.pgB_F_sec_para__X9930::text").extract() is not None:
            title.append(response.css("h1.pgB_F_sec_para__X9930::text").extract())
        if response.css("h1.common_css_iim_indore_iit_roorkee_sec_para__VgR4k::text").extract() is not None:
            title.append(response.css("h1.common_css_iim_indore_iit_roorkee_sec_para__VgR4k::text").extract())
        if response.css("div.fintechbannerHead").css("span::text").extract_first() is not None:
            title.append(response.css("div.fintechbannerHead").css("span::text").extract_first())
        if response.css("h2.mbafintechbannerHead.m-0::text").extract_first() is not None:
            title.append(response.css("h2.mbafintechbannerHead.m-0::text").extract_first())
        if response.css("h2.mbafintechbannerHead.m-0::text").extract_first() is not None:
            title.append(response.css("h2.mbafintechbannerHead.m-0::text").extract_first())
        if response.css("div.iitscbibannerHead.fancy").css("span::text").extract_first() is not None:
            title.append(response.css("div.iitscbibannerHead.fancy").css("span::text").extract_first())
        if response.css("h2.iitscbibannerHeadcyber.fancy::text").extract_first() is not None:
            title.append(response.css("h2.iitscbibannerHeadcyber.fancy::text").extract_first())
        if response.css("h1.iitscbibannerHead.fancy::text").extract_first() is not None:
            title.append(response.css("h1.iitscbibannerHead.fancy::text").extract_first())
        if response.css("h1.iit_roorkee_cybersecurity_sec_para__1VD3P::text").extract_first() is not None:
            title.append(response.css("h1.iit_roorkee_cybersecurity_sec_para__1VD3P::text").extract_first())
        if response.css(
                "h1.financial-services-capital-markets-management-program-iim-indore_sec_para__2fiK8::text").extract() is not None:
            title.append(response.css(
                "h1.financial-services-capital-markets-management-program-iim-indore_sec_para__2fiK8::text").extract())
        if response.css("h1.iit_roorkee_dm_sec_para__NkWAO::text").extract() is not None:
            title.append(" ".join(response.css("h1.iit_roorkee_dm_sec_para__NkWAO::text").extract()[:2]))
        if response.xpath("//*[@class='fsd_heading__zpAzm']//h1/text()").get() is not None:
            title.append(response.xpath("//*[@class='fsd_heading__zpAzm']//h1/text()").extract())
        if response.xpath('//h1[@class="bimtech_heading_main_mob__bDIgb"]/text()').extract() is not None:
            title.append(response.xpath('//h1[@class="bimtech_heading_main_mob__bDIgb"]/text()').extract())
        if response.xpath('//h1[@class="iimlucknowsecondheader m-0 p-0"]/text()').extract() is not None:
            title.append(response.xpath('//h1[@class="iimlucknowsecondheader m-0 p-0"]/b/text()').extract())
        if response.xpath(
                '//div[@class="iit_roorkee_cybersecurity_courseNameMobile__Ofj10"]//h1[@class="iit_roorkee_cybersecurity_sec_para__GRXq3"]/text() | //div[@class="fsd_heading__Ct3fm"]//h1/text() | //h1[@class="financial-services-capital-markets-management-program-iim-indore_sec_para__VinpY"]/text() | //h1[@class="pgB_F_sec_para__nvMi3"]/text() | //h1[@class="common_css_iim_indore_iit_roorkee_sec_para__NH6wb"]/text() | //div[@class="klu_heading__p6aoy"]//h1/text() | //div[@class="iit_roorkee_dm_courseNameMobile__Rc6Yh"]//h1[@class="iit_roorkee_dm_sec_para__0fF8w"]/text()') is not None:
            title.append(response.xpath(
                '//div[@class="iit_roorkee_cybersecurity_courseNameMobile__Ofj10"]//h1[@class="iit_roorkee_cybersecurity_sec_para__GRXq3"]/text() | //div[@class="fsd_heading__Ct3fm"]//h1/text() | //h1[@class="financial-services-capital-markets-management-program-iim-indore_sec_para__VinpY"]/text() | //h1[@class="pgB_F_sec_para__nvMi3"]/text() | //h1[@class="common_css_iim_indore_iit_roorkee_sec_para__NH6wb"]/text() | //div[@class="klu_heading__p6aoy"]//h1/text() | //div[@class="iit_roorkee_dm_courseNameMobile__Rc6Yh"]//h1[@class="iit_roorkee_dm_sec_para__0fF8w"]/text()').extract())

        title = list(filter(None, title))
        try:
            title = title[0]
            # title = " ".join(title)
        except:
            pass


        main_desc = []
        for i in response.css("div.col-12.text-center"):
            if i.css("span::text") is not None:
                main_desc.append(i.css("span::text").extract_first())
        for j in response.css("div.col-xl-12.col-lg-12.col-md-12"):
            if j.css("span::text") is not None:
                main_desc.append(j.css("span::text").extract_first())
            if j.css("p::text") is not None:
                main_desc.append(j.css("p::text").extract_first())
        for k in response.css("div.common_css_iim_indore_iit_roorkee_aboutcontainerdiv__y9WoG"):
            if k.css("p.common_css_iim_indore_iit_roorkee_aboutpara__KaX_0::text").extract_first() is not None:
                main_desc.append(k.css("p.common_css_iim_indore_iit_roorkee_aboutpara__KaX_0::text").extract_first())
        for l in response.css("div.fsd_para__4M097"):
            if l.css("p::text").extract() is not None:
                main_desc.append(l.css("p::text").extract_first())
        if response.css("p.pgB_F_whyChoosePostGraduate__Ll06U.pgB_F_desc_margin__ejMRU::text").extract_first() is not None:
            main_desc.append(response.css("p.pgB_F_whyChoosePostGraduate__Ll06U.pgB_F_desc_margin__ejMRU::text").extract_first()[2])
        if response.css("p.common_css_iim_indore_iit_roorkee_aboutpara__KaX_0::text").extract_first() is not None:
            main_desc.append(response.css("p.common_css_iim_indore_iit_roorkee_aboutpara__KaX_0::text").extract_first())
        if response.xpath('//div[@class="fsd_para__QhNin"]//p/text() | //div[@class="klu_enrolpara__QRvvf"]//p/text() | //p[@class="bimtech_section2_normal_text__0LTwG"]/text() | //p[@class="pgB_F_whyChoosePostGraduate__QPZOu pgB_F_desc_margin__48Io_"]/text() | //p[@class="common_css_iim_indore_iit_roorkee_aboutpara__3opl_"]/text()').extract_first() is not None:
            main_desc.append(response.xpath('//div[@class="fsd_para__QhNin"]//p/text() | //div[@class="klu_enrolpara__QRvvf"]//p/text() | //p[@class="bimtech_section2_normal_text__0LTwG"]/text() | //p[@class="pgB_F_whyChoosePostGraduate__QPZOu pgB_F_desc_margin__48Io_"]/text() | //p[@class="common_css_iim_indore_iit_roorkee_aboutpara__3opl_"]/text()').extract_first())

        main_desc = list(filter(None, main_desc))
        try:
            main_desc = f"<p>{main_desc[0]}</p>"
        except:
            pass


        display_price = []

        if response.css("span.feesAmount1::text").extract_first() is not None:
            display_price.append(response.css("span.feesAmount1::text").extract_first())
        if response.css("div.csHeader.pt-5.pb-5.text-center").css("p.mabProgramAmount::text").extract_first() is not None:
            display_price.append(response.css("p.fmabProgramAmount::text").extract_first())
        if response.css("div.programFeeCertificateLeft").css("p.programFeePara2::text").extract_first() is not None:
            display_price.append(response.css("div.programFeeCertificateLeft").css("p.programFeePara2::text").extract_first())
        if response.css("div.fsd_amount__5ije0").css("p::text").extract_first() is not None:
            display_price.append(response.css("div.fsd_amount__5ije0").css("p::text").extract_first())
        if response.xpath('//p[@class="mabProgramAmount"]//text()').extract_first() is not None:
            display_price.append(response.xpath('//p[@class="mabProgramAmount"]//text()').extract())
        if response.xpath('//h5[@class="awaHead"]//text()').extract_first() is not None:
            display_price.append(response.xpath('//h5[@class="awaHead"]//text()').extract_first())
        if response.xpath('//p[@class="financial-services-capital-markets-management-program-iim-indore_fee__W6osi"]//text()').extract_first() is not None:
            display_price.append(response.xpath('//p[@class="financial-services-capital-markets-management-program-iim-indore_fee__W6osi"]//text()').extract_first())
        if response.xpath('//p[@class="pgB_F_fee__6jGrp"]//text()').extract_first() is not None:
            display_price.append(response.xpath('//p[@class="pgB_F_fee__6jGrp"]//text()').extract_first())
        if response.xpath('//div[@class="klu_fees__85TIR"]//p/text() | //div[@class="fsd_amount__QWGno"]//p/text() | //div[@class="bimtech_section8_fee_amount___Jdf8"]//p/text() | //p[@class="financial-services-capital-markets-management-program-iim-indore_fee__wd_Ak"]/text() | //p[@class="pgB_F_fee___Vegk"]/text() | //div[@class="csHeader pt-sm-5 pb-5"]//h5[@class="awaHead"]/text()').extract_first() is not None:
            display_price.append(response.xpath('//div[@class="klu_fees__85TIR"]//p/text() | //div[@class="fsd_amount__QWGno"]//p/text() | //div[@class="bimtech_section8_fee_amount___Jdf8"]//p/text() | //p[@class="financial-services-capital-markets-management-program-iim-indore_fee__wd_Ak"]/text() | //p[@class="pgB_F_fee___Vegk"]/text() | //div[@class="csHeader pt-sm-5 pb-5"]//h5/text()').extract_first())
        display_price = list(filter(None, display_price))
        if display_price is not None:
            try:
                display_price = [i for i in re.findall(r'-?\d+\.?\d*', display_price[0])]
            except:
                try:
                    display_price = [i for i in re.findall(r'-?\d+\.?\d*', display_price[0][0])]
                except:
                    pass
        else:
            display_price = display_price
        if display_price == "":
            currency = ""
        else:
            currency = "INR"


        teaching_mode = []
        for i in response.css("div.col-xl-4.col-md-3.col-sm-6.updateBox"):
            if i.css("div.updateDetails::text").extract_first() is not None:
                teaching_mode.append(i.css("div.updateDetails::text").extract_first())
        for j in response.css("div.col-12.mt-2.col-lg-12"):
            if j.css("p::text").extract_first() is not None:
                teaching_mode.append(j.css("p::text").extract_first())
        for k in response.css("div.col-6.border-right.mt-2.col-lg-6") and response.css("div.col-6.mt-2.col-lg-6"):
            if k.css("p::text").extract_first() is not None:
                teaching_mode.append(k.css("p::text").extract_first())
        for l in response.css("div.col-3.d-flex.justify-content-center.align-items-center.pgB_F_border_right__YGH7I"):
            if l.css("span.pgB_F_externalDivHeaderPara__U5LGl::text").extract_first() is not None:
                teaching_mode.append(l.css("span.pgB_F_externalDivHeaderPara__U5LGl::text").extract_first())
        for m in response.css("div.col-xl-3.col-md-3.col-sm-6.updateBox"):
            if m.css("div.updateDetails::text").extract_first() is not None:
                teaching_mode.append(m.css("div.updateDetails::text").extract_first())
        if response.css("span.iit_roorkee_cybersecurity_externalDivHeaderPara__i1ceB::text").extract() is not None:
            teaching_mode.append(
                response.css("span.iit_roorkee_cybersecurity_externalDivHeaderPara__i1ceB::text").extract_first())
        if response.css(
                "span.financial-services-capital-markets-management-program-iim-indore_externalDivHeaderPara__gfYn4::text").extract() is not None:
            teaching_mode.append(response.css(
                "span.financial-services-capital-markets-management-program-iim-indore_externalDivHeaderPara__gfYn4::text").extract_first())
        teaching_mode = list(filter(None, teaching_mode))


        emi_start = []
        try:
            if response.css("p.feesType3").css("span.feesAmount2::text").extract_first() is not None:
                emi_start.append(response.css("span.feesAmount2::text").extract_first())
            if response.css("div.fsd_emiamount__HJKLH").css("p::text").extract_first() is not None:
                emi_start.append(response.css("div.fsd_emiamount__HJKLH").css("p::text").extract_first())
            if response.xpath('//div[@class="fsd_emiamount__SHRmE"]//p/text() | //div[@class="bimtech_section8_emi_amount__j2IZ4"]//p/text() | //div[@class="feesHead feesHeadcolor2"]//span[@class="feesAmount2"]/text() | //div[@class="feesHead feesHeadcolor2"]//span[@class="feesAmount1"]/text()').extract_first() is not None:
                emi_start.append(response.xpath('//div[@class="fsd_emiamount__SHRmE"]//p/text() | //div[@class="bimtech_section8_emi_amount__j2IZ4"]//p/text() | //div[@class="feesHead feesHeadcolor2"]//span[@class="feesAmount2"]/text() | //div[@class="feesHead feesHeadcolor2"]//span[@class="feesAmount1"]/text()').extract_first())
        except:
            if response.css("p.feesType1").css("span.feesAmount1::text").extract_first() is not None:
                emi_start.append(response.css("span.feesAmount1::text").extract_first())

        try:
            emi_start = [i for i in re.findall(r'-?\d+\.?\d*', emi_start[0])]
        except:
            pass

        instructor = []
        if response.xpath('//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="fsd_studentname__azEA7"]//p/text() | //div[@class="common_css_iim_indore_iit_roorkee_facultyCard1__VmNyX"]//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="pgB_F_onTabImg__kk3PM"]//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="bimtech_facultyname__acrm9"]//p/text() | //p[@class="Prachi-Samant"]/text()').extract() is not None:
            instructor.append(response.xpath('//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="fsd_studentname__azEA7"]//p/text() | //div[@class="common_css_iim_indore_iit_roorkee_facultyCard1__VmNyX"]//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="pgB_F_onTabImg__kk3PM"]//p[@class="pgB_F_cardTitle__oBJPs"]/text() | //div[@class="bimtech_facultyname__acrm9"]//p/text() | //p[@class="Prachi-Samant"]/text()').extract())
        if response.css("div.facultyDetails").css("p.Prachi-Samant::text").extract() is not None:
            instructor.append(response.css("div.facultyDetails").css("p.Prachi-Samant::text").extract())
        if response.css("div.facultyBox").css("strong::text").extract() is not None:
            instructor.append(response.css("div.facultyBox").css("strong::text").extract())
        if response.css("div.common_css_iim_indore_iit_roorkee_facultyCard4__oczhx").css(
                "p.pgB_F_cardTitle__ei1TR::text").extract() is not None:
            instructor.append(response.css("div.common_css_iim_indore_iit_roorkee_facultyCard4__oczhx").css(
                "p.pgB_F_cardTitle__ei1TR::text").extract())
        if response.css("div.financial-services-capital-markets-management-program-iim-indore_facultyCard4__ULYFp").css(
                "p.pgB_F_cardTitle__ei1TR::text").extract() is not None:
            instructor.append(response.css(
                "div.financial-services-capital-markets-management-program-iim-indore_facultyCard4__ULYFp").css(
                "p.pgB_F_cardTitle__ei1TR::text").extract())
        if response.css("div.pgB_F_onWebImg__s_rc_").css("p.pgB_F_cardTitle__ei1TR::text").extract() is not None:
            instructor.append(response.css("div.pgB_F_onWebImg__s_rc_").css("p.pgB_F_cardTitle__ei1TR::text").extract())
        if response.xpath('//div[@class="facultyDetails"]//p[@class="Prachi-Samant"]/text()').extract() is not None:
            instructor.append(response.xpath('//div[@class="eachMentor ml-2 mr-2"]//p[@class="Prachi-Samant"]/text()').extract())
        # if response.css("div.IIT-FacultyBox.pb-5.pl-5.pr-5").css("strong::text").extract() is not None:
        #     instructor.append(response.css("div.IIT-FacultyBox.pb-5.pl-5.pr-5").css("strong::text").extract())
        instructor = list(filter(None, instructor))
        try:
            instructor = instructor[0][:5]
            instructor = "| ".join(instructor)
        except:
            pass

        inst_bio = []
        if response.xpath('//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="fsd_studentdetail__uRSuo"]//p/text() | //div[@class="common_css_iim_indore_iit_roorkee_facultyCard1__VmNyX"]//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="pgB_F_onTabImg__kk3PM"]//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="bimtech_section8_mobile__KdKQi"]//div[@class="bimtech_facultydetail__9Mqj2"]//p/text() | //p[@class="Prachi-Samant-has-cl"]/text()').extract() is not None:
            inst_bio.append(response.xpath('//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="fsd_studentdetail__uRSuo"]//p/text() | //div[@class="common_css_iim_indore_iit_roorkee_facultyCard1__VmNyX"]//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="pgB_F_onTabImg__kk3PM"]//p[@class="pgB_F_cardText__MNnce"]/text() | //div[@class="bimtech_section8_mobile__KdKQi"]//div[@class="bimtech_facultydetail__9Mqj2"]//p/text() | //p[@class="Prachi-Samant-has-cl"]/text()').extract())
        if response.css("div.IIT-FacultyBox.pb-5.pl-5.pr-5").css("p::text").extract() is not None:
            inst_bio.append(response.css("div.IIT-FacultyBox.pb-5.pl-5.pr-5").css("p::text").extract())
        if response.css("div.facultyDetails").css("p.Prachi-Samant-has-cl::text").extract() is not None:
            inst_bio.append(response.css("div.facultyDetails").css("p.Prachi-Samant-has-cl::text").extract())
        # for i in response.css("div.facultyDetails"):
        #     if i.css("p.Prachi-Samant-has-cl::text").extract() is not None:
        #         inst_bio.append(i.css("p.Prachi-Samant-has-cl::text").extract())
        if response.css("div.facultyBox").css("span::text").extract() is not None:
            inst_bio.append(response.css("div.facultyBox").css("span::text").extract())
        # for j in response.css("div.facultyBox"):
        #     if j.css("span::text").extract() is not None:
        #         inst_bio.append(j.css("span::text").extract())
        if response.css("div.common_css_iim_indore_iit_roorkee_facultyCard4__oczhx").css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
            inst_bio.append(response.css("div.common_css_iim_indore_iit_roorkee_facultyCard4__oczhx").css("p.pgB_F_cardText__4Wstm::text").extract())
        # for k in response.css("div.common_css_iim_indore_iit_roorkee_facultyCard4__oczhx"):
        #     if k.css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
        #         inst_bio.append(k.css("p.pgB_F_cardText__4Wstm::text").extract())
        if response.css("div.financial-services-capital-markets-management-program-iim-indore_facultyCard4__ULYFp").css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
            inst_bio.append(response.css("div.financial-services-capital-markets-management-program-iim-indore_facultyCard4__ULYFp").css("p.pgB_F_cardText__4Wstm::text").extract())
        # for l in response.css(
        #         "div.financial-services-capital-markets-management-program-iim-indore_facultyCard4__ULYFp"):
        #     if l.css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
        #         inst_bio.append(l.css("p.pgB_F_cardText__4Wstm::text").extract())
        if response.css("div.pgB_F_onWebImg__s_rc_").css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
            inst_bio.append(response.css("div.pgB_F_onWebImg__s_rc_").css("p.pgB_F_cardText__4Wstm::text").extract())
        # for m in response.css("div.pgB_F_onWebImg__s_rc_"):
        #     if m.css("p.pgB_F_cardText__4Wstm::text").extract() is not None:
        #         inst_bio.append(m.css("p.pgB_F_cardText__4Wstm::text").extract())
        inst_bio = list(filter(None, inst_bio))
        try:
            inst_bio = inst_bio[0]
            inst_bio = "| ".join(inst_bio)
        except:
            pass

        faq_questions = []

        if response.css("div.accordion").css("button.accordion-button.collapsed::text").extract()[:11] is not None:
            faq_questions.append(
                response.css("div.accordion").css("button.accordion-button.collapsed::text").extract()[:11])
        if response.xpath('//ul[@class="What-is-the-format-ooh "]//li/text() | //div[@class="accordion__item"]//li[@class="faqTitle"]/text()').extract() is not None:
            faq_questions.append(
                response.xpath('//div[@class="accordion__item"]//li[@class="faqTitle"]/text()').extract())

        faq_questions = list(filter(None, faq_questions))
        try:
            faq_questions = faq_questions[0]
            faq_questions = "|".join(faq_questions)
        except:
            pass

        try:
            if response.xpath('//script[@data-react-helmet="true"]').extract()[2] is not None:
                q = response.xpath('//script[@data-react-helmet="true"]').extract()[2]
                data_dict = q[q.find('{'):q.rfind('}')+1]
                data_dict = data_dict.replace("\n","")
                json_data = json.loads(data_dict)
                for i in range(len(json_data["mainEntity"])):
                    faq_questions.append(json_data["mainEntity"][i]["name"])
        except:
            try:
                if response.xpath('//script[@data-react-helmet="true"]').extract()[0] is not None:
                    q = response.xpath('//script[@data-react-helmet="true"]').extract()[0]
                    data_dict = q[q.find('{'):q.rfind('}') + 1]
                    data_dict = data_dict.replace("\n", "")
                    json_data = json.loads(data_dict)
                    for i in range(len(json_data["mainEntity"])):
                        faq_questions.append(json_data["mainEntity"][i]["name"])
            except:
                try:
                    if response.xpath('//script[@data-react-helmet="true"]').extract()[1] is not None:
                        q = response.xpath('//script[@data-react-helmet="true"]').extract()[1]
                        data_dict = q[q.find('{'):q.rfind('}') + 1]
                        data_dict = data_dict.replace("\n", "")
                        json_data = json.loads(data_dict)
                        for i in range(len(json_data["mainEntity"])):
                            faq_questions.append(json_data["mainEntity"][i]["name"])
                except:
                    pass

        faq_answers = []

        if response.css("div.accordion").css("div.accordion-body::text").extract() is not None:
            faq_answers.append(response.css("div.accordion").css("div.accordion-body::text").extract()[:11])
        if response.xpath('//div[@class="Genpact-is-a-global"]/text() | //div[@class="Genpact-is-a-global"]/text()').extract() is not None:
            faq_answers.append(response.xpath('//div[@class="Genpact-is-a-global"]/text()').extract())
        faq_answers = list(filter(None, faq_answers))
        try:
            faq_answers = faq_answers[0]
            faq_answers = "|".join(faq_answers)
        except:
            pass

        try:
            if response.xpath('//script[@data-react-helmet="true"]').extract()[2] is not None:
                q = response.xpath('//script[@data-react-helmet="true"]').extract()[2]
                data_dict = q[q.find('{'):q.rfind('}')+1]
                data_dict = data_dict.replace("\n", "")
                json_data = json.loads(data_dict)
                for i in range(len(json_data["mainEntity"])):
                    faq_answers.append(json_data["mainEntity"][i]["acceptedAnswer"]["text"])
        except:
            try:
                if response.xpath('//script[@data-react-helmet="true"]').extract()[0] is not None:
                    q = response.xpath('//script[@data-react-helmet="true"]').extract()[0]
                    data_dict = q[q.find('{'):q.rfind('}') + 1]
                    data_dict = data_dict.replace("\n", "")
                    json_data = json.loads(data_dict)
                    for i in range(len(json_data["mainEntity"])):
                        faq_answers.append(json_data["mainEntity"][i]["acceptedAnswer"]["text"])
            except:
                try:
                    if response.xpath('//script[@data-react-helmet="true"]').extract()[1] is not None:
                        q = response.xpath('//script[@data-react-helmet="true"]').extract()[1]
                        data_dict = q[q.find('{'):q.rfind('}') + 1]
                        data_dict = data_dict.replace("\n", "")
                        json_data = json.loads(data_dict)
                        for i in range(len(json_data["mainEntity"])):
                            faq_answers.append(json_data["mainEntity"][i]["acceptedAnswer"]["text"])
                except:
                    pass

        reviewer_names = []

        if response.css("div.ssBox").css("div.ssUserName::text").extract() is not None:
            reviewer_names.append(response.css("div.ssBox").css("div.ssUserName::text").extract())

        if response.css("div.common_css_iim_indore_iit_roorkee_testCard_cs__Fv_Ju").css(
                "p.common_css_iim_indore_iit_roorkee_testimonialpara__bv_Pt::text").extract() is not None:
            reviewer_names.append(response.css("div.common_css_iim_indore_iit_roorkee_testCard_cs__Fv_Ju").css(
                "p.common_css_iim_indore_iit_roorkee_testimonialpara__bv_Pt::text").extract())

        reviewer_names = list(filter(None, reviewer_names))
        try:
            reviewer_names = reviewer_names[0][:4]
            reviewer_names = "| ".join(reviewer_names)
        except:
            pass

        try:
            if response.xpath('//script[@data-react-helmet="true"]').extract()[1] is not None:
                q = response.xpath('//script[@data-react-helmet="true"]').extract()[1]
                data_dict = q[q.find('{'):q.rfind('}')+1]
                json_data = json.loads(data_dict)
                for i in range(len(json_data["review"])):
                    reviewer_names.append(json_data["review"][i]["name"])
        except:
            pass

        reviews = []

        if response.css("div.common_css_iim_indore_iit_roorkee_testCard_cs__Fv_Ju").css("div::text").extract() is not None:
            reviews.append(response.css("div.common_css_iim_indore_iit_roorkee_testCard_cs__Fv_Ju").css("div::text").extract())
        if response.css("div.ssBoxContent.text-lg-left.text-center::text").extract() is not None:
            reviews.append(response.css("div.ssBoxContent.text-lg-left.text-center::text").extract())
        reviews = list(filter(None, reviews))
        try:
            reviews = reviews[0]
            reviews = "| ".join(reviews)
        except:
            pass

        try:
            if response.xpath('//script[@data-react-helmet="true"]').extract()[1] is not None:
                q = response.xpath('//script[@data-react-helmet="true"]').extract()[1]
                data_dict = q[q.find('{'):q.rfind('}')+1]
                json_data = json.loads(data_dict)
                for i in range(len(json_data["review"])):
                    reviews.append(json_data["review"][i]["reviewBody"])
        except:
            pass

        modules = []

        if response.xpath('//h1[@class="bimtech_trimester__TobzS"]/text() | //li[@class="faqTitle col-md-7 col-lg-9 color-curriculum"]/text() | //div[@class="accordion-item"]//p[@class="pgB_F_accMainHeader___deMJ"]/text() | //h3[@class="financial-services-capital-markets-management-program-iim-indore_accHeader1__sDfw8"]/text() | //h1[@class="fsd_accHeader1__V40GN"]/text() | //h1[@class="financial-services-capital-markets-management-program-iim-indore_accHeader1__sDfw8"]/text() | //div[@class="col-lg-8 col-sm-12 + ihub_curriculummobileacc__yJW5M"]//button[@class="accordion-button"]/text()').extract() is not None:
            modules.append(response.xpath('//h1[@class="bimtech_trimester__TobzS"]/text() | //li[@class="faqTitle col-md-7 col-lg-9 color-curriculum"]/text() | //div[@class="accordion-item"]//p[@class="pgB_F_accMainHeader___deMJ"]/text() | //h3[@class="financial-services-capital-markets-management-program-iim-indore_accHeader1__sDfw8"]/text() | //h1[@class="fsd_accHeader1__V40GN"]/text() | //h1[@class="financial-services-capital-markets-management-program-iim-indore_accHeader1__sDfw8"]/text() | //div[@class="col-lg-8 col-sm-12 + ihub_curriculummobileacc__yJW5M"]//button[@class="accordion-button"]/text()').extract())

        modules = list(filter(None, modules))
        try:
            modules = modules[0]
        except:
            pass

        sub_modules = []
        # if response.css("div.accordion-collapse.collapse.show").css("li::text").extract() is not None:
        #     sub_modules.append(response.css("div.accordion-collapse.collapse.show").css("li::text").extract())
        for i in response.css("div.accordion-collapse.collapse.show"):
            if i.css("li::text").extract() is not None:
                sub_modules.append(i.css("li::text").extract())

        for i in response.css("div.bimtech_accordiondata__6M8g9"):
            if i.css("div.bimtech_rightacc__l_lU9").css("p::text").extract() is not None:
                sub_modules.append(i.css("div.bimtech_rightacc__l_lU9").css("p::text").extract())
        sub_modules = list(filter(None, sub_modules))

        # for i in response.xpath('//div[@class="bimtech_accordiondata__6M8g9"] | //div[@class="bimtech_section3_desc_course_lernout_parent__jQbuf"]'):
        #     sub_modules.append(response.xpath('//div[@class="bimtech_rightacc__l_lU9"]//p/text()').extract())

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
                        submodule = f"<item{submodnum}>{j}</item{submodnum}>"
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
        # print(contents)
        # print("---------------------------")

        if contents == "":
            print("==============Into Contents======================")
            try:
                link = response.xpath('//div[@class="col-sm-6 text-center"]//a/@href').extract_first()
                yield scrapy.Request("https://imarticus.org"+link, callback=self.parser_contents2, cb_kwargs={
                    "title": title,
                    "main_desc": main_desc,
                    "teaching_mode": teaching_mode,
                    "emi_start": emi_start,
                    "instructor": instructor,
                    "inst_bio": inst_bio,
                    "reviewer_names": reviewer_names,
                    "reviews": reviews,
                    "contents": contents,
                    "display_price": display_price,
                    "faq_questions": faq_questions,
                    "faq_answers": faq_answers
                    })
            except:
                #contents = ""
                yield {
                    # "response": response,
                    "title": title,
                    "main_desc": main_desc,
                    "display_price": display_price,
                    "emi_start": emi_start,
                    "currency": currency,
                    "teaching_mode": teaching_mode,
                    "instructor": instructor,
                    "inst_bio": inst_bio,
                    "reviewer_names": reviewer_names,
                    "reviews": reviews,
                    "faq_questions": faq_questions,
                    "faq_answers": faq_answers,
                    # "modules": modules,
                    # "sub_modules": sub_modules,
                    "contents": contents
                }
        else:
            yield {
                #"response": response,
                "title": title,
                "main_desc": main_desc,
                "display_price": display_price,
                "emi_start": emi_start,
                "currency": currency,
                "teaching_mode": teaching_mode,
                "instructor": instructor,
                "inst_bio": inst_bio,
                "reviewer_names": reviewer_names,
                "reviews": reviews,
                "faq_questions": faq_questions,
                "faq_answers": faq_answers,
                # "modules": modules,
                # "sub_modules": sub_modules,
                "contents": contents
            }









        # if response.css("div.col-sm-6.text-center").css("a::attr(href)").extract() is not None:
        #     link = response.css("div.col-sm-6.text-center").css("a::attr(href)")
        #     yield scrapy.Request(response.urljoin(link.get()), callback=self.parser_contents2, cb_kwargs={
        #     "title": title,
        #     "main_desc": main_desc,
        #     "teaching_mode": teaching_mode,
        #     "emi_start": emi_start,
        #     "instructor": instructor,
        #     "inst_bio": inst_bio,
        #     "reviewer_names": reviewer_names,
        #     "reviews": reviews,
        #     "contents": contents,
        #     "display_price": display_price,
        #     "faq_questions": faq_questions,
        #     "faq_answers": faq_answers
        #     })
        # elif response.css("div.row.pb-2").css("div.col-sm-6.text-center").css("a::attr(href)").extract() is not None:
        #     link = response.css("div.row.pb-2").css("div.col-sm-6.text-center").css("a::attr(href)")
        #     yield scrapy.Request(response.urljoin(link.get()), callback=self.parser_contents2, cb_kwargs={
        #     "title": title,
        #     "main_desc": main_desc,
        #     "teaching_mode": teaching_mode,
        #     "emi_start": emi_start,
        #     "instructor": instructor,
        #     "inst_bio": inst_bio,
        #     "reviewer_names": reviewer_names,
        #     "reviews": reviews,
        #     "contents": contents,
        #     "display_price": display_price,
        #     "faq_questions": faq_questions,
        #     "faq_answers": faq_answers
        #     })
        #else:
        # yield {
        #     "response": response,
        #     "title": title,
        #     "main_desc": main_desc,
        #     "display_price": display_price,
        #     "emi_start": emi_start,
        #     "currency": currency,
        #     "teaching_mode": teaching_mode,
        #     "instructor": instructor,
        #     "inst_bio": inst_bio,
        #     "reviewer_names": reviewer_names,
        #     "reviews": reviews,
        #     "faq_questions": faq_questions,
        #     "faq_answers": faq_answers,
        #     # "modules": modules,
        #     # "sub_modules": sub_modules,
        #     "contents": contents
        #     }



# data_pdf = []
        # try:
        #     if response.css("script#__NEXT_DATA__::text").extract_first() is not None:
        #         js_script = json.loads(response.css("script#__NEXT_DATA__::text").extract_first())
        #         for i in range(17):
        #             data_pdf.append(js_script["props"]["pageProps"]["propsdata"]["allData"][i]["brochureFileName"])
        #         #print(pdfs)
        # except:
        #     data_pdf = None
