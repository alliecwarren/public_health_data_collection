
%run ./cdc_config.ipynb
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime, timedelta, date
from urllib.parse import urljoin
from pathlib import Path
from protego import Protego
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
# Function to download a file
def download_file(pdf_url, folder, filename, rp, delay_time = DEFAULT_WAIT_TIME):
    """
   Function to download a pdf file from a URL, file downloaded is printed out
    
    Args:
        url (str): The URL for the file to download
        folder (Path object): file path to save file to
        filename (str): filename for saved file
        rp (protego robots.txt parsing): result of parsing robots.txt file for the page using Protego
        delay_time (int): delay time before downloaded the file
    Returns:
        name of file if successfully downloaded, None otherwise
    """
    # add delay to download
    if not rp.crawl_delay('*') is None:
        delay_time = rp.crawl_delay('*')
    time.sleep(delay_time)
    
    if rp.can_fetch(pdf_url, "*") == False:
        print(f'Restricted from scraping data from this URL: {url}')
        return None
    else:  
        response = requests.get(pdf_url, stream=True)
        if response.status_code == 200:
            file_path = folder / filename
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            print(f'Successfully downloaded {filename}')
            return filename
        else:
            print(f'Failed to download {filename}')
            return None
def get_update_date(link):
    """
   Returns year that file was updated based on string Updated in [year]
    
    Args:
        link (str): link to file
        
    Returns:
      year (str), if found in text
    """
    parent_p = link.find_parent('p')
    if parent_p:
        associated_text = parent_p.get_text(strip=True)
        pattern = r'Updated in (\d{4})'
        match = re.search(pattern, associated_text)
        if match:
            return match.group(1)
    
    return None 
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
    