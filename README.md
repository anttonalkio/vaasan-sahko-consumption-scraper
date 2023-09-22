# Consumption readings from Vaasan Sähkö

Simple [scrapy](https://docs.scrapy.org/en/latest/index.html) scraper that fetches electricity consumption readings from Vaasan Sähkö website and sends them to InfluxDb

## Installation

Scrapy is strongly recommended to be installed in a dedicated virtualenv, to avoid conflicting with system packages

See [here](https://docs.python.org/3/tutorial/venv.html#tut-venv) for more instructions on venv

```bash
python -m venv env
source env/Scripts/activate
pip install -r requirements
```

## Usage

```bash
scrapy crawl vaasansahko
```