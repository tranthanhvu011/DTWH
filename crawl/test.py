import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from crawl_data import CrawlData

class TestCrawlData:
    def __init__(self):
        self.crawl_data = CrawlData()

    def run_tests(self):
        self.crawl_data.start_crawling()
        print("Crawl completed.")

if __name__ == "__main__":
    test = TestCrawlData()
    test.run_tests()
