import unittest
from unittest.mock import MagicMock, patch
import retrying
import logging
from etl import locationcounter
import time

## Usage ##
# cd dsforthepeople
# python3 -m unittest -v tests.testcounter

class TestCounter(unittest.TestCase):

		def setUp(self):
			logging.disable(logging.CRITICAL)

		def test_counting_proper_number_of_mentions_in_files(self):

			outdir = "/test/"
			outfile = "article_counts_test.csv"
			metadatafile = "/test/metadata_test.csv"
			data_dir = "/test/testdata"

			locationcounter.main(outdir, outfile, metadatafile, data_dir)
			
			assert True