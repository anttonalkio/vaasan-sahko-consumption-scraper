from scrapy.http import FormRequest
from datetime import datetime

import scrapy
import json
import zoneinfo

class VaasanSahkoSpider(scrapy.Spider):
    name = 'vaasansahko'

    def start_requests(self):
        login_url = 'https://online.vaasansahko.fi/eServices/Online/IndexNoAuth'
        return [scrapy.Request(login_url, callback=self.login)]
    
    def login(self, response):
        token = response.css("form input[name=__RequestVerificationToken]::attr(value)").extract_first()
        return FormRequest.from_response(response,
                                         formdata={
                                            '__RequestVerificationToken': token,
                                            'UserName': self.settings.get('VAASANSAHKO_USERNAME'),
                                            'Password': self.settings.get('VAASANSAHKO_PASSWORD')
                                        },
                                         callback=self.change_customer)

    def change_customer(self, response):
        target_user_path = response.css("#userMenu").xpath(f".//a[contains(., '{self.settings.get('VAASANSAHKO_TARGET_USER')}')]/@href").get()
        return scrapy.Request(f'https://online.vaasansahko.fi{target_user_path}', callback=self.start_scraping)

    def start_scraping(self, response):
        return scrapy.Request('https://online.vaasansahko.fi/Reporting/CustomerConsumption', callback=self.call_api)
    
    def call_api(self, response):
        token = response.css("#rootContainer input[name=__RequestVerificationToken]::attr(value)").extract_first()
        pattern = r"\bvar\s+GraphContext\s*=\s*(\{.*?\})\s*;\s*\n"
        json_data = response.css("script::text").re_first(pattern)
        context = json.loads(json_data)

        formData = {
            '__RequestVerificationToken': token,
            'customerCode': context['CustomerCode'],
            'networkCode': context['NetworkCode'],
            'meteringPointCode': context['MeteringPointCode'],
            'enableTemperature': 'false',
            'enablePriceSeries': 'false',
            'enableTemperatureCorrectedConsumption': 'false',
            'mpSourceCompanyCode': context['MpSourceCompanyCode'],
            'activeTarifficationId': ''
        }

        return scrapy.FormRequest('https://online.vaasansahko.fi/Reporting/CustomerConsumption/GetHourlyConsumption', formdata=formData, callback=self.parse_hourly_consumption)

    def parse_hourly_consumption(self, response):
        consumption = json.loads(response.text)['Consumptions'][0]['Series']['Data']
        self.logger.info(f'Number of data points found from Vaasan Energia: {len(consumption)}')
        self.logger.info(f'Latest timestamp found from consumption data: {consumption[-1][0] / 1000} - {datetime.fromtimestamp(consumption[-1][0] / 1000, tz=zoneinfo.ZoneInfo("Europe/Helsinki"))}')
        consumption_parsed = map(lambda x: {'consumption': x[1], 'ts': datetime.fromtimestamp(x[0] / 1000, tz=zoneinfo.ZoneInfo('Europe/Helsinki'))}, consumption)
        for item in consumption_parsed:
            yield item
