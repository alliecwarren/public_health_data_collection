
# get function and config for scraping data from WHO
%run ./who_functions.ipynb
# set url and filepaths for saving data and metadata
url = WHO_DOCUMENTS_URL
destination_folder = set_directory(folder = RAW_DATA_BASE_FOLDER, subfolder = WHO_DOCUMENTS_FOLDER)
metadata_filename = METADATA_PREFIX + WHO_DOCUMENTS_FOLDER + '.' + METADATA_EXT
metapath = get_metapath(METADATA_FOLDER, metadata_filename) 
meta_data = get_metadata(metapath)
# Set up Selenium with headless Chrome
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
chromedriver_autoinstaller.install()

driver = webdriver.Chrome(options=options)

# check that page can be scraped
if rp.can_fetch(url, "*") == False:
    print(f'Restricted from scraping data from this URL: {url}')
else:
    # Load the page
    driver.get(url)
# read robots.txt file
r = requests.get(WHO_ROBOTS_TXT)
rp = Protego.parse(r.text)
# WHO has 10,000s of documents, we are not downloading all of these
# Just downloading a subset of document types -  Manuals and position papers - for now, which we can filter for
# Loop through each of these filter, find all documents for download on each page, and click through all pages
for doc_type in WHO_DOCUMENT_TYPES:
    # filter document types to those specified
    set_document_type(driver, doc_type)   
    page = 1
    while True:
        print(page)
        # wait for page to load
        driver.implicitly_wait(10)
        try:
            # Find all download links
            download_elements = find_elements_with_retry(driver, By.CLASS_NAME, 'download-url')
            # Title for the documents
            download_titles = driver.find_elements(By.CLASS_NAME, 'sf-publications-item__title')
            # link to the page, which contains metadata relating to the document
            download_pages = driver.find_elements(By.CLASS_NAME, 'page-url')
            # publication date element for the document (not stored in the metadata)
            publication_dates = driver.find_elements(By.CLASS_NAME, 'sf-publications-item__date')
            # Download each file and collect related items
            for i, download_element in enumerate(download_elements):
                print(i)
                download_link = download_element.get_attribute('href')
                page_link = download_pages[i].get_attribute('href')
                # check that a download link is available for the document - some are missing
                if not download_link is None:
                    # check that the document has not already been downloaded
                    if meta_data["url"].str.contains(download_link).any() == False:
                        # get the title, which is used to name the file
                        download_title = download_titles[i].get_attribute('title')
                        download_filename = re.sub(r'[^A-Za-z0-9 ]+', '', download_title) + "." + DOCUMENTS_EXT
                        
                        # get the publication date and format
                        doc_publication_date = publication_dates[i].text
                        doc_publication_date = datetime.strptime(doc_publication_date, "%d %B %Y").strftime("%Y-%m-%d")
                        
                        # download the file to specified destination folder with the specified title
                        download_file(download_link, destination_folder, download_filename, rp)
                       
                        # extract additional metadata
                        meta_data_info = extract_meta(page_link, rp)
                        # set current date for metadata
                        now = datetime.now()
                        date_time_string = now.strftime(METADATA_DATE_FMT)
                        # create row of metadata and add to table
                        cur_metadata = {'url':download_link, 
                                        'datetime_captured': date_time_string,
                                        'title': download_title,
                                        'author': meta_data_info['author'],
                                        'publication_date': doc_publication_date,
                                        'license': WHO_LICENSE,
                                        'file_type': DOCUMENTS_EXT,
                                        'storage_location': str(destination_folder) + "/" + download_filename,
                                        'datetime_last_updated': date_time_string,
                                        'json_additional_metadata': combine_metadata_descriptions(meta_data_info)}
                        cur_metadata = pd.DataFrame([cur_metadata])
                        meta_data = pd.concat([meta_data, cur_metadata], ignore_index = True)
                driver.implicitly_wait(DEFAULT_WAIT_TIME)

            # click right button to move to the next page of documents
            right_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'k-i-arrow-60-right'))
            )

            right_button.click()
            time.sleep(3)
            page = page + 1
        # exceptions
        except StaleElementReferenceException:
            print("Encountered a stale element reference. Retrying...")
            continue
        except Exception as e:
            print(f"No more pages to load or encountered an error: {e}")
            break
    # return to base url to collect documents of the next type
    driver.get(url)
driver.quit()

meta_data.to_parquet(metapath, index = False)
 