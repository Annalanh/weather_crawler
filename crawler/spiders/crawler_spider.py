from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import FormRequest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class CrawlerSpider(Spider):
    name = "crawler"
    allowed_domains = ["worldweatheronline.com"]

    def start_requests(self):
        current_datetime = datetime.today() 
        #lấy 89 ngày tính từ ngày 
        for i in range(90):
            date = current_datetime - timedelta(days= i + 1)
            date_str = str(date.year)+"-"+str(date.month)+"-"+str(date.day)
            yield FormRequest("https://www.worldweatheronline.com/ha-noi-weather-history/vn.aspx?",
                formdata={
                    "__VIEWSTATE": "2BL92YbE92g36J39wjLVRlT0PW5MXXEH3jwWiHMj/hyDRrRrudYoHAoc6PdZ3j9ky7YjazK59GgqEhDiK0BdviNGqKXxza36ZfLPEI7d70kyq0zF",
                    "__VIEWSTATEGENERATOR": "F960AAB1",
                    "ctl00$tp1$rblTemp": "1",
                    "ctl00$tp1$rblPrecip": "1",
                    "ctl00$tp1$rblWindSpeed": "2",
                    "ctl00$tp1$rblPressure": "1",
                    "ctl00$tp1$rblVis": "1",
                    "ctl00$tp1$rblheight": "1",
                    "ctl00$hdlat": "21.033",
                    "ctl00$hdlon": "105.850",
                    "ctl00$areaid": "2309",
                    "ctl00$MainContentHolder$txtPastDate": date_str,
                    "ctl00$MainContentHolder$butShowPastWeather": "Get Weather"
                },
                dont_filter=True)

    def parse(self, response):
        data_table = Selector(response).xpath('//*[@id="aspnetForm"]/div[2]/main/div[6]/div/div[1]/div/div[2]/div//div')
        request_date = Selector(response).xpath('//*[@id="aspnetForm"]/div[2]/main/div[4]/div/div/div/input/@value').get()

        time1_data = data_table[12:23]
        time2_data = data_table[24:35]
        time3_data = data_table[36:47]
        time4_data = data_table[48:59]
        time5_data = data_table[60:71]
        time6_data = data_table[72:83]
        time7_data = data_table[84:95]
        time8_data = data_table[96:107]

        weather_data = []
        time1_data_text = []
        time2_data_text = []
        time3_data_text = []
        time4_data_text = []
        time5_data_text = []
        time6_data_text = []
        time7_data_text = []
        time8_data_text = []

        for data in time1_data:
            time1_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time1_data_text)
        for data in time2_data:
            time2_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time2_data_text)
        for data in time3_data:
            time3_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time3_data_text)
        for data in time4_data:
            time4_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time4_data_text)
        for data in time5_data:
            time5_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time5_data_text)
        for data in time6_data:
            time6_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time6_data_text)
        for data in time7_data:
            time7_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time7_data_text)
        for data in time8_data:
            time8_data_text.append(data.xpath(".//text()").extract_first())
        weather_data.append(time8_data_text)

        df = pd.DataFrame(np.array(weather_data))
        df['date'] = np.full(
                        shape=8,
                        fill_value=request_date,
                        )
        df.to_csv('output.csv', mode='a', header=False, index=False)

 