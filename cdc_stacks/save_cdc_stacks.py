
# get function and config for scraping data from WHO
%run ./cdc_stacks_functions.ipynb
# get current date
# used to filter files up to current date
now = datetime.now()
current_date = now.strftime("%m/%d/%Y")
# read robots.txt file
r = requests.get(CDC_ROBOTS_TXT)
rp = Protego.parse(r.text)
# set url and filepaths to directory for saving data and metadata
url = CDC_STACKS_URL
destination_folder = set_directory(folder = RAW_DATA_BASE_FOLDER, subfolder = CDC_STACKS_FOLDER)
directory = str(destination_folder)
metadata_filename = METADATA_PREFIX + CDC_STACKS_FOLDER + '.' + METADATA_EXT
metapath = get_metapath(METADATA_FOLDER, metadata_filename) 
meta_data = get_metadata(metapath)
# Set up Selenium with headless Chrome
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
chromedriver_autoinstaller.install()

driver = webdriver.Chrome(options=options)
# CDC Stacks has 10,000s (or 100,000s) of documents, we are not downloading all of these
# Just downloading a subset of document types and/or collections - reports, guidelines and recommendations, Stephen B. Thacker CDC Library collection, Preventing Chronic Disease
# filter for these types of files, and click through all pages to download files and collect related info
for i in range(3, len(CDC_STACKS_COLLECTIONS)):
    page = 1
    # Load the page
    driver.get(url)
    # filter to specified document types
    filter_stacks_search(driver,
                         collection_name = CDC_STACKS_COLLECTIONS[i],
                         dataset_type = CDC_STACKS_DOCUMENT_TYPE[i],
                         language = CDC_LANGUAGE,
                         date_start = CDC_STACKS_START_DATE[i],
                         date_end = current_date)  
    # search for these documents
    search_button = driver.find_element(By.ID, 'searchButtonAdvanced')
    search_button.click()
    driver.implicitly_wait(10)
    time.sleep(2)
    
    while True:
        driver.implicitly_wait(10)
        print(f'page: {page}')
        try:
            # there are 20 results per page, get all links of a page
            for pg in range(0, 20): 
                stack_links = find_element_with_retry(driver, By.ID, f'paginationSubmit{pg}', retries=1)
                if not stack_links is None:
                    stack_link = stack_links.get_attribute('href')
                    # check that document has not already be downloaded
                    if meta_data["url"].str.contains(stack_link).any() == False and not stack_link is None:
                        # download document
                        download_stacks(stack_link, directory)
                        # get additional metadata
                        stacks_meta = extract_meta(stack_link, rp)
                        stacks_info = get_stacks_info(stack_link, rp)
                        # set current date/time
                        now = datetime.now()
                        date_time_string = now.strftime(METADATA_DATE_FMT)
                        download_filename = re.sub(r'^.*/', '', stacks_info[2])
                        cur_file_ext = re.sub(r'^.*\.', '', download_filename)
                        cur_metadata = {'url':stack_link, 
                                        'datetime_captured': date_time_string,
                                        'title': stacks_info[0],
                                        'author': stacks_meta['author'],
                                        'publication_date': stacks_info[1],
                                        'license': CDC_LICENSE,
                                        'file_type': cur_file_ext,
                                        'storage_location': directory + "/" + download_filename,
                                        'datetime_last_updated': date_time_string,
                                        'json_additional_metadata': combine_metadata_descriptions(stacks_meta)}
                        cur_metadata = pd.DataFrame([cur_metadata])
                        meta_data = pd.concat([meta_data, cur_metadata], ignore_index = True)
                        
            # click next button to move to the next page of documents
            next_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, 'next'))
            )

            next_button.click()
            time.sleep(DEFAULT_WAIT_TIME)
            page = page + 1
        # exceptions
        except StaleElementReferenceException:
            print("Encountered a stale element reference. Retrying...")
            continue
        except Exception as e:
            print(f"No more pages to load or encountered an error: {e}")
            break

driver.quit()


display(meta_data)


meta_data.to_parquet(metapath, index = False)
 