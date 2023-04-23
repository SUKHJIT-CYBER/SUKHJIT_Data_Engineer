"""

@author:SUKHJIT SINGH

This module create unittests for the main functions  and test them 

"""


from unittest.mock import patch

import unittest
import os
from unittest.mock import  patch
from main_fun import *


class TestGetXmlData(unittest.TestCase):
    """
    A class for unit testing the get_xml_data function.
    """

    def test_get_xml_data_success(self):
        """
        Test the get_xml_data function with a successful URL request.
        """
        url = "https://example.com/my_data.xml"
        xml_data = get_xml_data(url)
        self.assertIsInstance(xml_data, str)
        self.assertGreater(len(xml_data), 0)

    def test_get_xml_data_failure(self):
        """
        Test the get_xml_data function with a failed URL request.
        """
        url = "https://example.com/404"
        xml_data = get_xml_data(url)
        self.assertIsNone(xml_data)


class TestGetDownloadLink(unittest.TestCase):
    """
    Unit tests for the get_download_link function.
    
    """

    def test_valid_xml_data(self):
        """Test the function with valid XML data."""
        xml_data = (
            "<root><doc><str name='file_type'>DLTINS</str>"
            "<str name='file_name'>test.zip</str>"
            "<str name='download_link'>https://example.com/test.zip</str>"
            "</doc></root>"
        )
        expected_link = "https://example.com/test.zip"
        self.assertEqual(get_download_link(xml_data), expected_link)

    def test_invalid_xml_data(self):
        """Test the function with invalid XML data."""
        xml_data = "<root><doc><str name='file_type'>INVALID</str></doc></root>"
        self.assertIsNone(get_download_link(xml_data))

    def test_missing_download_link(self):
        """Test the function when the download link is missing."""
        xml_data = "<root><doc><str name='file_type'>DLTINS</str></doc></root>"
        self.assertIsNone(get_download_link(xml_data))


class TestDownloadAndExtractFile(unittest.TestCase):
    """Tests for download_and_extract_file function"""

    @patch('urllib.request.urlretrieve')
    @patch('zipfile.ZipFile')
    def test_download_and_extract_file_success(self, mock_zipfile, mock_urlretrieve):
        """Test download_and_extract_file function successfully downloads and extracts the file"""
        download_url = "http://example.com/file.zip"
        file_name = "test.zip"
        mock_urlretrieve.return_value = (file_name, None)
        mock_zipfile.return_value.__enter__.return_value.extractall.return_value = None

        download_and_extract_file(download_url, file_name)

        mock_urlretrieve.assert_called_once_with(download_url, file_name)
        mock_zipfile.assert_called_once_with(file_name, 'r')
        mock_zipfile.return_value.__enter__.return_value.extractall.assert_called_once_with(
            '.')
        os.remove(file_name)

    @patch('urllib.request.urlretrieve')
    @patch('zipfile.ZipFile')
    def test_download_and_extract_file_failure(self, mock_zipfile, mock_urlretrieve):
        """Test download_and_extract_file function fails to download or extract the file"""
        download_url = "http://example.com/file.zip"
        file_name = "test.zip"
        mock_urlretrieve.side_effect = Exception("Failed to download file")
        mock_zipfile.side_effect = Exception("Failed to extract file")

        download_and_extract_file(download_url, file_name)

        mock_urlretrieve.assert_called_once_with(download_url, file_name)
        mock_zipfile.assert_called_once_with(file_name, 'r')
        self.assertTrue(
            mock_zipfile.return_value.__enter__.return_value.extractall.called is False)


class TestCovert_XML_to_CSV(unittest.TestCase):

    def setUp(self):
        self.xml_data = '''<?xml version="1.0" encoding="UTF-8"?>
                            <response>
                                <result>
                                    <doc>
                                        <str name="file_type">DLTINS</str>
                                        <str name="download_link">http://example.com/file.zip</str>
                                        <str name="file_name">file.zip</str>
                                    </doc>
                                </result>
                            </response>'''
        self.invalid_xml_data = '<invalid_xml_data>'
        self.download_url = 'http://example.com/file.zip'
        self.file_name = 'file.zip'
        self.xml_file = 'test.xml'
        self.csv_path = 'csv_files'

    def test_get_download_link(self):
        # Test with valid XML data
        self.assertEqual(get_download_link(self.xml_data), self.download_url)

        # Test with invalid XML data
        self.assertIsNone(get_download_link(self.invalid_xml_data))

    def test_download_and_extract_file(self):
        # Download and extract file
        download_and_extract_file(self.download_url, self.file_name)

        # Test that the file was downloaded and extracted
        self.assertTrue(os.path.exists(self.file_name))
        self.assertTrue(os.path.exists('File1.xml'))

        # Cleanup downloaded files
        os.remove(self.file_name)
        os.remove('File1.xml')

    def test_convert_xml_to_csv(self):
        # Convert XML to CSV
        convert_xml_to_csv(self.xml_file, self.csv_path)

        # Test that the CSV file was created
        self.assertTrue(os.path.exists(
            os.path.join(self.csv_path, 'test.csv')))

        # Cleanup created files
        os.remove(os.path.join(self.csv_path, 'test.csv'))
        

class TestUploadCSVLambda(unittest.TestCase):
    @patch('main_fun.boto3.resource')
    def test_upload_csv_lambda_success(self, mock_s3_resource):
        # Arrange
        event = {
            'file': '/path/to/local/file.csv',
            'region_name': 'us-west-2',
            'bucket_name': 'my-bucket',
            'aws_access_key_id': 'my-access-key-id',
            'aws_secret_access_key': 'my-secret-access-key'
        }

        # Act
        upload_csv_lambda(event, None)

        # Assert
        mock_s3_resource.assert_called_once_with(
            service_name='s3',
            region_name='us-west-2',
            aws_access_key_id='my-access-key-id',
            aws_secret_access_key='my-secret-access-key'
        )

        mock_s3_resource().Bucket.assert_called_once_with('my-bucket')
        mock_s3_resource().Bucket().upload_file.assert_called_once_with(
            Filename='/path/to/local/file.csv',
            Key='file.csv'
        )



if __name__ == "__main__":
    unittest.main()




   
