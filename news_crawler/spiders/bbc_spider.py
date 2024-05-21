# import of the necessary libraries
import scrapy
from readability import Document
from google.cloud import bigquery
import os

class BBCSpider(scrapy.Spider):
    name = 'bbc'
    allowed_domains = ['bbc.com']
    start_urls = ['https://www.bbc.com/news']

    def parse(self, response):
        # Build a list to store the links Inicializa uma lista para armazenar os links dos artigos
        article_links = response.css('a.gs-c-promo-heading::attr(href)').getall()
        
        # Go through the links e make a request for each one 
        for href in article_links:
            article_url = response.urljoin(href)
            request = scrapy.Request(article_url, callback=self.parse_article)
            request.meta['article_url'] = article_url
            yield request

    def parse_article(self, response):
        doc = Document(response.text)
        article_title = doc.title()
        article_content = doc.summary()
        article_url = response.meta['article_url']

        # Collect extra data
        author = response.css('meta[name="byl"]::attr(content)').get()
        publish_date = response.css('meta[name="date"]::attr(content)').get()

        article = {
            'title': article_title,
            'content': article_content,
            'author': author,
            'url': article_url,
            'publish_date': publish_date
        }

        # Store the data at BigQuery
        self.store_in_bigquery(article)

    def store_in_bigquery(self, article):
        client = bigquery.Client()
        table_id = 'newsdata-423622.newsdata.DATABASE'
        
        errors = client.insert_rows_json(table_id, [article])
        if errors:
            self.log(f"Encountered errors while inserting rows: {errors}")
