
%run ./who_config.ipynb
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
from selenium.common.exceptions import StaleElementReferenceException
import re
import time
from datetime import datetime, date, timedelta
import pandas as pd
from protego import Protego
from pathlib import Path


from bs4 import BeautifulSoup
from urllib.parse import urljoin
def get_html_response(url, rp, delay_time = DEFAULT_WAIT_TIME):
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
def download_file(url, destination_folder, file_name, rp):
    """
   Function to download a file from a URL, file downloaded is printed out
    
    Args:
        url (str): The URL for the file to download
        destination_folder (Path object): file path to save file to
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
    Returns:
        
    """
    response = get_html_response(url, rp)
    file_path = destination_folder / file_name
    
    if not response is None:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_name}")
def find_elements_with_retry(driver, by, value, retries=3, delay=1):
    """
   Function to find elements with retry mechanism - if stale element attempts to find it again and adds a delay
    
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
            elements = driver.find_elements(by, value)
            return elements
        except StaleElementReferenceException:
            if i < retries - 1:
                time.sleep(delay)
                continue
            else:
                raise
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
def set_document_type(driver, doc_type):
    """
   Set document type in WHO advanced filtering for documents and searches for those documents
    
    Args:
        driver (obj): webdriver Chrome object
        doc_type (str): name of document
        
    Returns:
    
    """
    # filter to documents of type manual
    # Locate the dropdown toggle by its class name
    dropdown_toggle = driver.find_element(By.CLASS_NAME, 'k-i-arrow-60-down')

    # Click the dropdown toggle to reveal the options
    dropdown_toggle.click()

    # Optionally, wait for the dropdown options to be visible
    time.sleep(2)  # Adjust the sleep time as needed, or use WebDriverWait for a better approach

    # Locate the dropdown options container (you may need to adjust the selector)
    dropdown_options_container = driver.find_element(By.CLASS_NAME, 'k-list-container')  # Adjust as necessary
    
    # Locate and click the desired option within the container
    desired_option = dropdown_options_container.find_element(By.XPATH, f"//li[text()='{doc_type}']")
    desired_option.click()

    driver.implicitly_wait(50)
    time.sleep(2)
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
    
# Function to parse the main fact-sheets page and get all the links
def get_fact_sheet_links(base_url, rp):
    """
    get links to all fact sheets from main page
    
    Args:
        url (str): string for webpage
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
    Returns:
        list of urls
    """
    html_content = get_html_response(base_url, rp).text
    fact_sheet_links = []
    if not html_content is None:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find all <a> tags within the specified class
        for li_tag in soup.find_all('li', class_='alphabetical-nav--list-item'):
            a_tag = li_tag.find('a')
            link= a_tag.get('href')
            if link and link.startswith('/news-room/fact-sheets'):
                full_link = 'https://www.who.int' + link
                fact_sheet_links.append(full_link)

    return fact_sheet_links
def save_fact_sheet(url, directory, rp):
    """
    Function to extract and save the text content of a WHO fact-sheet
    
    Args:
        url (str): string for webpage
        directory: file path to directory to save file
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego 

    Returns:
        if fact sheet can be saved, returns name of file and publication date, otherwise returns empty string
    """
    html_content = get_html_response(url, rp).text
    if html_content is None:
        return ''
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Assuming the fact-sheet content is within the row sf-detail-content
    text_content = ''
    for content in soup.find_all('div', class_='row sf-detail-content'):
        cur_text_content = content.get_text(separator='\n', strip=True)
        text_content = text_content + cur_text_content
    
    pub_date = soup.find('div', class_='date').get_text(separator='\n', strip=True)
    pub_date = datetime.strptime(pub_date, "%d %B %Y").strftime("%Y-%m-%d")

    if text_content != '':
    # Create a filename based on the last part of the URL
        filename = url.split('/')[-1] + '.txt'
        filepath = directory / filename
        # Save the text content to a file
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(text_content)
        print(f'Saved: {filename}')
        return filename, pub_date
    else:
        print(f'No content found for URL: {url}')
        return ''