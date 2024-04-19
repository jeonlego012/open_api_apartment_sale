import requests
import datetime
import xml.etree.ElementTree as ET
import pandas as pd
import os
from dotenv import load_dotenv

def get_data(gu_code, base_date):
    url="http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade"
    load_dotenv()
    service_key = os.environ.get('service_key')

    payload = dict()
    payload["LAWD_CD"] = gu_code
    payload["DEAL_YMD"] = base_date
    payload["serviceKey"] = service_key

    response = requests.get(url, params=payload)

    return response

def get_items(response):
    root = ET.fromstring(response.content)
    item_list = []
    for child in root.find('body').find('items'):
        elements = child.findall('*')
        data = {}
        for element in elements:
            tag = element.tag.strip()
            text = element.text.strip()
            data[tag] = text
        if(int(data['거래금액'].replace(",","")) <= 59999):
            item_list.append(data)
            
    return item_list


#기간 설정
year = [str("%02d" %(y)) for y in range(2024, 2025)]
month = [str("%02d" %(m)) for m in range(4, 5)]
base_date_list = ["%s%s" %(y, m) for y in year for m in month ]

#법정동 코드 불러오기
code_file = "comtcadministcode_utf8.txt"
code_series = pd.read_csv(code_file, sep='\t')
code_series.columns = ['code', 'name', 'is_exist']
code_series = code_series[code_series['is_exist'] == '존재']
code_series = code_series.reset_index(drop=True)

#int64타입 string타입으로 변경
code_series['code'] = code_series['code'].apply(str)
code_list = code_series['code'].to_list()

#'구'코드 : 법정동코드 10자리 중 앞 5자리
gu_code_list = []
for code in code_list:
    code = str(code[0:5])
    gu_code_list.append(code)

gu_code_list = list(dict.fromkeys(gu_code_list))

items_list = []
for base_date in base_date_list:
    for gu_code in gu_code_list:
        try:
            #API 호출
            response = get_data(gu_code, base_date)
            items_list += get_items(response)
        except Exception as err:
            print(base_date, " ", gu_code, " error occurred")
    print(base_date, " done")

#추출된 데이터 갯수
print(len(items_list))

#csv파일로 추출
items = pd.DataFrame(items_list)
items.head()
items.to_csv(os.path.join("apartment_sale_data_%s~%s.csv" %(year[0], year[-1])), index=False,encoding="utf-8")
