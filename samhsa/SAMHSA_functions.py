
%run ./SAMHSA_config.ipynb
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import requests
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

import re
import time
from datetime import datetime, date, timedelta
import pandas as pd
from protego import Protego
from pathlib import Path


from bs4 import BeautifulSoup
from urllib.parse import urljoin
def get_html_response(url, rp, delay_time = DEFAULT_WAIT_TIME, retries = 3):
    """
   Function to get the HTML content of a page
      Also checks that url scraping is permitted under robots.txt file and delay requests
      And retries, with a delay, if exception
    
    Args:
        url (str): The URL for the page being accessed.
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
        delay_time: default time for delaying before accessing the page, may be specified in robots.txt
        retries (int): default 3
    Returns:
        requests reponse object
    """
    # if crawl_delay defined in robots.txt, use specified value for delay time
    if not rp.crawl_delay('*') is None:
        delay_time = rp.crawl_delay('*')
    time.sleep(delay_time)
   
    if rp.can_fetch(url, "*") == False:
        print(f'Restricted from scraping data from this URL: {url}')
        return None
    else:
        for i in range(retries):
            try:
                response = requests.get(url)
                return response
            except:
                if i < retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
                    return None
    
def set_directory(folder, subfolder):
    """
   Returns full file path to gcs folder specified. If folder does not exist it is created
    
    Args:
        folder (str): file path to folder
        subfolder (str): name of subfolder
        
    Returns:
       Path file path 
    """
    output_path_base = Path.home()
    gcs_base = output_path_base / folder
    directory = gcs_base /  subfolder
    # check if folder exists, if not create it
    if directory.is_dir() == False:
        directory.mkdir(parents=True, exist_ok=True)
    return directory
def get_metapath(folder, filename):
    """
   Returns full file path to metadata specified
    
    Args:
        folder (str): file path to folder
        filename (str): name of metadata file
        
    Returns:
       Path file path 
    """
    output_path_base = Path.home()
    gcs_base = output_path_base / folder
    metapath = gcs_base / filename
    
    return metapath

def get_metadata(metapath):
    """
   Returns existing metadata, if it does not already exist pandas dataframe of specified format is returned
    
    Args:
        metapath (str): file path to metadata file
        
    Returns:
       Pandas dataframe
    """
    # if metadata exists read in the file, otherwise create an empty dataframe
    if metapath.is_file() == True:
        meta_data = pd.read_parquet(metapath)
    else:
        meta_data = pd.DataFrame(columns=METADATA_SCHEMA)
    return meta_data
def find_meta_date(soup):
    """
    Search for publication date, based on a list of possible meta data tag
        if any date is included, return that, otherwise return None
    
    Args:
        soup (obj): BeautifulSoup parsed html
        
    Returns:
        first date found in metadata from specified tags, or None if there is no date
    
    """
    # Define potential meta tag attributes for publication date
    publication_date_meta_tags = [
        {'name': 'dc.date'},
        {'property': 'article:published_time'},
        {'itemprop': 'datePublished'},
        {'name': 'publication_date'}
    ]
    for attrs in publication_date_meta_tags:
        tag = soup.find('meta', attrs=attrs)
        if tag and 'content' in tag.attrs:
            return tag['content']
    return None

def extract_meta(url, rp):
    """
    extract meta data including author, keywords, description, title, and any publication date
    
    Args:
        url (str): url for webpage
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego 
    Returns:
        dictionary of metadata 
    """
    html_content = get_html_response(url, rp).text
    soup = BeautifulSoup(html_content, 'html.parser')

    meta_tags = {
    'author': soup.find('meta', attrs={'name': 'author'}),
    'keywords': soup.find('meta', attrs={'name': 'keywords'}),
    'description': soup.find('meta', attrs={'name': 'description'}),
    'title': soup.find('meta', attrs={'property': 'og:title'})
    }
    
    meta_data = {}

    for key, tag in meta_tags.items():
        if tag and 'content' in tag.attrs:
            meta_data[key] = tag['content']
        else:
            meta_data[key] = None

    meta_data['date'] = find_meta_date(soup)
    
    return meta_data
def combine_metadata_descriptions(metainfo, description_fields = ['keywords', 'description']):
    """
    combine some metadata fields into dictionary
    
    Args:
        metainfo (dictionary): dictionary of metadata, output of extract_meta()
        description_fields (lst): list of strings with keys to extract from metadata
    Returns:
        dictionary of subset of metadata, for json_additional_metadata field
    """
    metadata_descriptions = {}
    for k in description_fields:
        if not metainfo[k] is None:
            metadata_descriptions[k] = metainfo[k]
    return metadata_descriptions
def find_element_with_retry(driver, by, value, retries=3, delay=1):
    """
   Function to find element with retry mechanism - if stale element attempts to find it again and adds a delay
    
    Args:
        driver (object): Chrome webdriver
        by: argument to driver.find_elements, specifies which attributes are used to find the element
        value (str): string used to find element 
        retries (int): default 3, number of attempts to find the element
        delay (int): default 1, delay time before attempting to find the element again
        
    Returns:
       element 
    """
    for i in range(retries):
        try:
            element = driver.find_element(by, value)
            return element
        except (StaleElementReferenceException, TimeoutException, NoSuchElementException):
            if i < retries - 1:
                time.sleep(delay)
                continue
            else:
                return None
       
def find_elements_with_retry(driver, by, value, retries=5, delay=8):
    """
   Function to find elements with retry mechanism - if stale element attempts to find it again and adds a delay
    
    Args:
        driver (object): Chrome webdriver
        by: argument to driver.find_elements, specifies which attributes are used to find the element
        value (str): string used to find element 
        retries (int): default 5, number of attempts to find the element
        delay (int): default 8, delay time before attempting to find the element again
        
    Returns:
       element 
    """
    time.sleep(delay)
    for i in range(retries):
        try:
            elements = driver.find_elements(by, value)
            return elements
        except (StaleElementReferenceException, TimeoutException, NoSuchElementException):
            if i < retries - 1:
                time.sleep(delay)
                continue
            else:
                raise
    return None
def download_report(url, directory, rp):
    """
   Function to download a file from a URL, file downloaded is printed out
    
    Args:
        url (str): The URL for the file to download
        destination_folder (Path object): file path to save file to
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
    Returns:
        (str) name of file downloaded
    """
    time.sleep(5)
    response = get_html_response(url, rp, delay_time = 10)
    if not response is None:
        # set name based on url - add report number to filename (if in url) to prevent files with same name
        file_name = re.sub(".*/", "", url)
        report_num = re.findall(r'rpt\d+', url)
        if len(report_num)>0:
            file_name = report_num[0] + "_" + file_name
        file_path = directory / file_name
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_name}")
    else:
        return ""

    # Wait for the download to complete (you may need to adjust the time.sleep duration)
    time.sleep(5)
    
    return file_name
def get_sahmsa_report_info(new_driver):
    """
    extract text in sidebar with info relating to that SAHMSA report page and title of page
    
    Args:
        new_driver (obj): webdriver Chrome object
    Returns:
        dictionary of metadata 
    """
     # Initialize an empty dictionary to store key-value pairs
    data_dict = {}

    # get sidebar of metadata (if it exists)
    sidebar_div = find_element_with_retry(new_driver, By.CSS_SELECTOR, 'div.desktop\:grid-col-6.content-column.report-sidebar1')
    if sidebar_div is None:
        data_dict['Page Title'] = ''
        data_dict['Publication Date'] = ''
    else:
        # Find all elements within the <div>
        time.sleep(1)
        subclasses = sidebar_div.find_elements(By.XPATH, './/*')

        # Extract text and create dictionary
        current_key = None
        for element in subclasses:
            text = element.text.strip()
            if text.endswith(':'):
                current_key = re.sub(":", "", text)
            elif current_key:
                data_dict[current_key] = text
                current_key = None

        # add page title to dictionary
        page_title = new_driver.find_element(By.CLASS_NAME, 'hide-for-iframe').text
        data_dict['Page Title'] = page_title
    
    return data_dict