
%run ./cdc_stacks_config.ipynb
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
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.select import Select
import re
import time
from datetime import datetime, date, timedelta
import pandas as pd
from protego import Protego
from pathlib import Path


from bs4 import BeautifulSoup
from urllib.parse import urljoin
def get_html_response(url, rp, delay_time = 5):
    """
   Function to get the HTML content of a page
      Also checks that url scraping is permitted under robots.txt file and delay requests
    
    Args:
        url (str): The URL for the page being accessed.
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
        delay_time: default time for delaying before accessing the page, may be specified in robots.txt
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
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response
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
def get_stacks_info(link, rp):
    """
    extract info related to page - date, title etc
    
    Args:
        url (str): The URL for the page being accessed.
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
    Returns:
        dictionary of metadata 
    """
    response = get_html_response(link, rp).text
    soup = BeautifulSoup(response, 'html.parser')
    page_text = soup.find('div', class_='stacks-flex').get_text(separator='\n', strip=True)
    page_date = soup.find('div', class_='col-3 bookHeaderListData').get_text(separator='\n', strip=True)
    
    form_element = soup.find('form', id='download-document')

    # Extract the value of the action attribute
    action_value = form_element['action']

    return page_text, page_date, action_value
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

# extract meta data including author, keywords, description, title, and any publication date
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
        except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
            if i < retries - 1:
                time.sleep(delay)
                continue
            else:
                #print(f"Encountered an error: {e}")
                return None
                
def download_stacks(url, directory):
    """
   Function to download a file from a URL - finds download button and download files to specified directory
    
    Args:
        url (str): The URL for the file to download
        directory (Path object): file path to save file to
    Returns:
        
    """
    # Configure Chrome options to set the download directory
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    prefs = {"download.default_directory": directory}
    chrome_options.add_experimental_option("prefs", prefs)

    # Initialize the Chrome webdriver
    new_driver = webdriver.Chrome(options=chrome_options)

    # Open the webpage
    new_driver.get(url)

    # Wait for the download button to be clickable
    download_button = find_element_with_retry(new_driver, By.ID, "download-document-submit")
    download_button.click()

    # Wait for the download to complete (you may need to adjust the time.sleep duration)
    time.sleep(10)

    # Close the webdriver
    new_driver.quit()
def filter_stacks_search(driver, collection_name = "", dataset_type = "", language = "", date_start = "", date_end = ""):
    """
   Set document type, collection, date range, and language in cdc stacks advanced filtering for documents 
    
    Args:
        driver (obj): webdriver Chrome object
        collection_name (str): name of collection, optional
        dataset_type (str): name of document type, optional
        language (str): language of document, optional
        date_start (str): beginning of date range for document filtering in m/d/Y format, optional
        date_end (str): end of date range for document filtering in m/d/Y format, optional

    Returns:
    
    """
    if collection_name != "":
        collection_drop_down_element = driver.find_element(By.ID, 'edit-fedora_terms10')
        collection_drop_down_element = Select(collection_drop_down_element)
        collection_drop_down_element.select_by_visible_text(collection_name)

    if dataset_type != "":
        dataset_drop_down_element = driver.find_element(By.ID, 'edit-fedora_terms9')
        dataset_drop_down_element = Select(dataset_drop_down_element)
        dataset_drop_down_element.select_by_visible_text(dataset_type)


    if language != "":
        language_drop_down_element = driver.find_element(By.ID, 'edit-fedora_terms5')
        language_drop_down_element = Select(language_drop_down_element)
        language_drop_down_element.select_by_visible_text(language)


    if date_start != "":
        # Find the input element by its id
        date_start_element = driver.find_element(By.ID, "fedora_terms6")
        # Set the value of the input element
        date_start_element.clear()  # Clear any existing value
        date_start_element.send_keys(date_start)

    if date_end != "":
        # Find the input element by its id
        date_end_element = driver.find_element(By.ID, "fedora_terms7")
        # Set the value of the input element
        date_end_element.clear()  # Clear any existing value
        date_end_element.send_keys(date_end)
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