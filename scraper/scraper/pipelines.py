# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
    
class InfluxDbPipeline:
    def __init__(self, url, org, bucket, token) -> None:
        self.url = url
        self.org = org
        self.bucket = bucket
        self.token = token
        self.items = []
        self.delta_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            url=crawler.settings.get("INFLUXDB_URL", "https://127.0.0.1:8086"),
            org=crawler.settings.get("INFLUXDB_ORG"),
            bucket=crawler.settings.get("INFLUXDB_BUCKET"),
            token=crawler.settings.get("INFLUXDB_TOKEN"),
        )
    
    def open_spider(self, spider):
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        query_api = self.client.query_api()

        tables = query_api.query(f'from(bucket: "{self.bucket}") |> range(start: -7d) |> filter(fn: (r) => r["_measurement"] == "consumption") |> max(column: "_time")')
        if tables and tables[0].records:
            # if so, set delta_start to the value of the first record's _time field
            self.delta_start = tables[0].records[0].values['_time']
            spider.logger.info(f'Latest timestamp found from InfluxDB: {self.delta_start}')
        else:
            spider.logger.info(f'No records found from InfluxDB within 7 days, using default value for delta_start: {self.delta_start}')

    def process_item(self, item, spider):
        if item['ts'] >= self.delta_start:
            self.items.append(Point("consumption").tag("unit", "kWh").field("_value", item['consumption']).time(item['ts'], "s").to_line_protocol())

        return item

    def close_spider(self, spider):
        spider.logger.info(f'Number of records to be inserted: {len(self.items)}')
        with self.client.write_api(write_options=ASYNCHRONOUS) as write_api:
            write_api.write(bucket=self.bucket, record=self.items, write_precision=WritePrecision.S)
        self.client.close()