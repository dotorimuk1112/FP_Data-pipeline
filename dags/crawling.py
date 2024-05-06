import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def crawl_cars():
    df_cars=[]

    url_ko="https://www.bobaedream.co.kr/mycar/mycar_list.php?gubun=K&page={}&order=S11&view_size=20"

    urls_ko=[] #국내 차 전체 리스트 링크
    for i in range(1,11):
        url=url_ko.format(str(i))
        urls_ko.append(url)

    # 'SEQ', 'MNAME', 'PRICE' 'MYERAR', 'MILEAGE', 'COLOR', 'TRANS', 'F_TYPE', 'DISP', 'VTYPE', 'VNUM'
    # 'CU_HIS', 'MVD_HIS', 'AVD_HIS', 'FD_HIS', 'VT_HIS', 'US_HIS', 'LNAME'
    
    info=["MNAME", "PRICE", "MYERAR","MILEAGE", "COLOR", "TRANS", "F_TYPE", "DISP", "VTYPE", "VNUM"]
    colacci_info=["CU_HIS","MVD_HIS", "AVD_HIS", "FD_HIS", "VT_HIS", "US_HIS", "L_NAME"]
    cols = info + colacci_info

    for url in urls_ko:
        res=requests.get(url)
        res.raise_for_status()
        requests.adapters.DEFAULT_RETRIES = 10
        soup=BeautifulSoup(res.text,"lxml")

        cars=soup.find_all("li",attrs={"class":"product-item"})
        links=[]
        #한 url마다 들어있는 모든 차들에 대해 실행
        for car in cars:
            link = "https://www.bobaedream.co.kr" + car.a["href"]
            links.append(link)
        for link in links:
            print(link)
            res2 = requests.get(link, timeout=5)
            res2.raise_for_status()
            soup2 = BeautifulSoup(res2.text, "lxml")
            infobox = soup2.find("div", attrs={"class": "info-util box"})

            name = soup2.find("h3", attrs={"class": "tit"})
            state = soup2.find("div", attrs={"class": "tbl-01 st-low"})
            galdata = soup2.find("div", attrs={"class": "gallery-data"})

            try:
                carnumber = galdata.find("b")
                year = state.find("th", string='연식').find_next_sibling("td")
                km = state.find("th", string='주행거리').find_next_sibling("td")
                fuel = state.find("th", string='연료').find_next_sibling("td")
                amount = state.find("th", string='배기량').find_next_sibling("td")
                color = state.find("th", string='색상').find_next_sibling("td")
                trans = state.find("th", string="변속기").find_next_sibling("td")
                price = soup2.find("span", attrs={"class": "price"})
                car_type = ' '

                time.sleep(5)

                if infobox.find("span", attrs={"class": "round-ln insurance"}).find_next("i").find_next("em") == None:
                    acc1 = '미등록'
                else:
                    acc1 = '등록'

                findacci_info1 = []
                try:
                    if acc1=='등록':
                        try:
                            zero = 0
                            null_string = ''
                            acc1table=soup2.find("div",attrs={"class":"info-insurance"})
                            insurdt1=acc1table.find("th",string="차량번호/소유자변경").find_next_sibling("td").get_text().split('/')[1][1:-1]
                            insuraccis1 = acc1table.find("th", string="자동차보험 특수사고").find_next_sibling("td").get_text().split('/')
                            insurdt2=insuraccis1[0][4:-1]
                            insurdt3 = insuraccis1[1][8:-1]
                            insurdt4 = insuraccis1[2][8:-1]
                            insurdt5 = insuraccis1[3][5:]
                            insuraccis2=acc1table.find("th", string="보험사고(내차피해)").find_next_sibling("td").get_text().split('회')
                            insurdt6=insuraccis2[0]
                            insuraccis3=acc1table.find("th", string="보험사고(타차가해)").find_next_sibling("td").get_text().split('회')
                            insurdt8=insuraccis3[0]
                            # colacci_info=["CU_HIS","MVD_HIS", "AVD_HIS", "FD_HIS", "VT_HIS", "US_HIS", "L_NAME"]
                            findacci_info1=[zero, insurdt6, insurdt8, int(insurdt3)+int(insurdt4), insurdt5, insurdt1, null_string]
                        except AttributeError:
                            try:
                                acc1table = soup2.find("dl",attrs={"class":"flt"})
                                insuraccis1 = acc1table.find_all("dd")
                                insuraccisp0 = insuraccis1[0].get_text().split('/')
                                insuraccisp1 = insuraccis1[1].get_text().split('/')
                                zero = 0
                                null_string = ''
                                insurdt1 = insuraccisp0[1][1:-1] # 소유자 변경
                                insurdt2 = insuraccisp1[0][4:-1] # 전손 이력
                                insurdt3 = insuraccisp1[1][8:-1] # 침수 이력1
                                insurdt4 = insuraccisp1[2][8:-1] # 침수 이력2
                                insurdt5 = insuraccisp1[3][5:]  # 도난 이력
                                acc2table=soup2.find("dl",attrs={"class":"frt"})
                                insuraccis2 = acc2table.find_all("dd")
                                insuraccisp0 = insuraccis2[0].get_text().split('회')
                                insuraccisp1 = insuraccis2[1].get_text().split('회')
                                insurdt6 = insuraccisp0[0] # 자차 피해
                                insurdt8 = insuraccisp1[0] # 타차 피해
                                # colacci_info=["용도변경이력","자차피해이력", "타차피해이력", "침수이력", "도난이력", "소유자변경이력", "모델명"]
                                findacci_info1=[zero, insurdt6, insurdt8, int(insurdt3)+int(insurdt4), insurdt5, insurdt1, null_string]
                            except IndexError:
                                findacci_info1=[0,0,0,0,0,0]
                    else:
                        findacci_info1=['']*(len(colacci_info)-1)
                except IndexError:
                    findacci_info1 = ['']*(len(colacci_info)-1)
            except AttributeError:
                print("AttributeError")
            try:
                temp = [name.get_text(), price.get_text(), year.get_text(), km.get_text(), color.get_text(), trans.get_text(), fuel.get_text(),
                        amount.get_text(), car_type, carnumber.get_text()] + findacci_info1
                df_cars.append(temp)
            except AttributeError:
                print("AttributeError")

    df_cars=pd.DataFrame(data=df_cars,columns=cols)

    return df_cars

# crawl_cars()