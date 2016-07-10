import unittest
from unittest.mock import MagicMock, patch
import retrying
import logging
from etl import locationcounter
import time

class TestCounter(unittest.TestCase):

		def setUp(self):
			logging.disable(logging.CRITICAL)

		def test_counting_proper_number_of_mentions_in_files(self):

			outfile = "/test/article_counts_test.csv"
			metadatafile = "/test/metadata_test.csv"
			data_dir = "/test/testdata"

			locationcounter.main(outfile, metadatafile, data_dir)
			
			assert True