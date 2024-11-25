import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from staging_data import StagingData

class TestStagingData:
    def __init__(self):
        self.staging_data = StagingData()

    def run_tests(self):
        self.staging_data.staging_data()
        print("Dữ liệu đã được tải lên database thành công.")

if __name__ == "__main__":
    test = TestStagingData()
    test.run_tests()
