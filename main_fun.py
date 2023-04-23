"""

@author:SUKHJIT SINGH
Module: Main Functions
-------------------
This module contains code to download, extract and process Zip data from a URL , extract it , parse it , convert to csv  and upload to AWS S3 bucket  as lambda function

Functions:
----------
    - download_data: Downloads data from a URL and returns the data in the form of an IO stream.
    - extract_zip: Extracts data from a ZIP file and returns the data in the form of an IO stream.
    - process_csv: Processes CSV data from an IO stream and returns a pandas DataFrame.
    - process_xml: Processes XML data from an IO stream and returns a pandas DataFrame.
    - upload_to_s3: Uploads a pandas DataFrame to an S3 bucket.

"""
import requests  # Importing the requests module for handling HTTP requests
import zipfile  # Importing the zipfile module for working with zip files
# Importing the ElementTree module for working with XML data
import xml.etree.ElementTree as ET
import urllib.request    # Importing the urllib.request module for working with URLs
from env_variables import *   # Importing the variables from the env_variables module
import pandas as pd     # Importing the pandas library for data manipulation and analysis
import os   # Importing the os module for working with the operating system
# Importing the boto3 module for interacting with Amazon Web Services (AWS) services
import boto3 
from logger import log



def get_xml_data(url):
    """
    Retrieve XML data from the specified URL using the requests library.

    Args:
        url: A string representing the URL to retrieve the XML data from.

    Returns:
        A string containing the XML data retrieved from the URL, or None if an error occurred.
    """
    try:
        response = requests.get(url)
        xml_data = response.text
        log.info(f"Successfully retrieved XML data from {url}")
        return xml_data
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to retrieve XML data from {url}: {e}")
        return None


def get_download_link(xml_data):
    """
    Retrieve the download link for a DLTINS file from XML data.

    :param xml_data: The XML data to search for the download link.
    :type xml_data: str
    :return: The download link for the DLTINS file, or None if the link could not be found.
    :rtype: str or None
    """
    try:
        root = ET.fromstring(xml_data)
        set_zip_name = None
        set_download_link = None
        for doc in root.findall(".//doc"):
            file_type = doc.find("str[@name='file_type']").text
            if file_type is not None and file_type == "DLTINS":
                download_link = doc.find("str[@name='download_link']").text
                set_zip_name = doc.find("str[@name='file_name']").text
                if download_link is not None:
                    set_download_link = download_link
                    break
        if set_download_link is not None:
            log.info(f"Found download link {set_download_link} for file {set_zip_name}")
            return set_download_link
        else:
            log.warning("Failed to find download link in XML data")
            return None
    except ET.ParseError as e:
        log.error(f"Failed to parse XML data: {e}")
        return None
    

def download_and_extract_file(download_url, file_name):
    """
    Download a file from a given URL and extract its contents.

    :param download_url: The URL to download the file from.
    :type download_url: str
    :param file_name: The name to save the downloaded file as.
    :type file_name: str
    """
    try:
        # Download the file using urllib
        urllib.request.urlretrieve(download_url, file_name)
        log.info(f"Downloaded file {file_name} from URL {download_url}")
        # Extract the contents of the ZIP file using zipfile
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall('.')
        log.info(f"Extracted contents of file {file_name}")
    except Exception as e:
        log.error(f"Failed to download or extract file: {e}")


def convert_xml_to_csv(xml_file, csv_path):
    """
    Convert an XML file to a CSV file with specified columns.

    :param xml_file: The XML file to convert.
    :type xml_file: str
    :param csv_path: The directory to save the CSV file in.
    :type csv_path: str
    """
    try:
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)

        csv_fname = xml_file.split(os.sep)[-1].split(".")[0] + ".csv"
        csv_file = os.path.join(csv_path, csv_fname)

        csv_columns = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]
        df = pd.DataFrame(columns=csv_columns)
        extracted_data = []

        xml_iter = ET.iterparse(xml_file, events=("start",))

        for event, element in xml_iter:
            if event == "start":
                if "TermntdRcrd" in element.tag:
                    data = {}
                    reqd_elements = [
                        (elem.tag, elem)
                        for elem in element
                        if "FinInstrmGnlAttrbts" in elem.tag or "Issr" in elem.tag
                    ]
                    for tag, elem in reqd_elements:
                        if "FinInstrmGnlAttrbts" in tag:
                            for child in elem:
                                if "Id" in child.tag:
                                    data[csv_columns[0]] = child.text
                                elif "FullNm" in child.tag:
                                    data[csv_columns[1]] = child.text
                                elif "ClssfctnTp" in child.tag:
                                    data[csv_columns[2]] = child.text
                                elif "CmmdtyDerivInd" in child.tag:
                                    data[csv_columns[3]] = child.text
                                elif "NtnlCcy" in child.tag:
                                    data[csv_columns[4]] = child.text
                        else:
                            data[csv_columns[5]] = child.text
                    extracted_data.append(data)

        df1 = pd.DataFrame(extracted_data)
        df = pd.concat([df, df1], ignore_index=True)
        df.dropna(inplace=True)
        df.to_csv(csv_file, index=False)
        log.info(f"Converted {xml_file} to {csv_file}")
    except Exception as e:
        log.error(f"Error converting {xml_file} to csv: {e}")



def upload_csv_lambda(event, context):
    """
    Uploads a file to an S3 bucket.

    Args:
        event (dict): The event that triggered the Lambda function. Must contain the following keys:
            - file (str): The local file path of the file to upload.
            - region_name (str): The AWS region name of the S3 bucket.
            - bucket_name (str): The name of the S3 bucket.
            - aws_access_key_id (str): The AWS access key ID for the credentials.
            - aws_secret_access_key (str): The AWS secret access key for the credentials.
        context (LambdaContext): The context object for the Lambda function.

    Returns:
        None
    """
    log.info('Received event: %s', event)

    file = event['file']
    file_name_in_s3 = os.path.split(file)[-1]
    s3 = boto3.resource(
        service_name="s3",
        region_name=event['region_name'],
        aws_access_key_id=event['aws_access_key_id'],
        aws_secret_access_key=event['aws_secret_access_key'],
    )
    s3.Bucket(event['bucket_name']).upload_file(
        Filename=file, Key=file_name_in_s3)
    log.info('File uploaded to S3: %s', file_name_in_s3)




if __name__ == '__main__':
    #Driver Function
    XML_DATA = get_xml_data(set_url)   # Get XML data from set URL
    URL = get_download_link(XML_DATA)  # Get download link from XML data
    # Download and extract file from URL with set file name
    download_and_extract_file(URL, set_file_name)

    # Convert XML file to CSV with set file name and CSV path
    convert_xml_to_csv(set_xml_file, csv_path)




