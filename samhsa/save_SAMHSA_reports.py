
%run ./SAMHSA_functions.ipynb
# set url and filepaths to directory for saving the files and metdata
url = SAMHSA_URL
destination_folder = set_directory(folder = RAW_DATA_BASE_FOLDER, subfolder = SAMHSA_FOLDER)
directory = str(destination_folder)
metadata_filename = METADATA_PREFIX + SAMHSA_FOLDER + '.' + METADATA_EXT
metapath = get_metapath(METADATA_FOLDER, metadata_filename) 
meta_data = get_metadata(metapath)
# read robots.txt file
r = requests.get(SAMHSA_ROBOTS_TXT)
rp = Protego.parse(r.text)
# Set up Selenium with headless Chrome
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
chromedriver_autoinstaller.install()

driver = webdriver.Chrome(options=options)
# check that page can be scraped
# get first page of files
# and find the total number of pages
if rp.can_fetch(url, "*") == False:
    print(f'Restricted from scraping data from this URL: {url}')
    num_pages = 0
else:
    driver.get(url)
    num_pages = driver.find_elements(By.CLASS_NAME, 'pagerer-center-pane')
    num_pages = num_pages[0].text
    num_pages = re.sub(".*of ", "", num_pages)
    num_pages = int(re.sub("[^0-9]", "", num_pages))
print(f'Number of pages: {str(num_pages)}')
Number of pages: 206
# stopped downloading after page ~95 as files we older
for page_num in range(0, num_pages):
    # get current page
    new_url = url + '?page=' + str(page_num)
    print(f'Current page: {str(page_num)}')
    # check that page can be scraped
    if rp.can_fetch(new_url, "*") == False:
        print(f'Restricted from scraping data from this URL: {new_url}')
        report_links = []
    else:
        driver.get(new_url)
        # get all links to report of that page
        report_links = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/data/report/"]')
    for link in report_links:
        # Extract href attribute from each element and go to that page
        href = link.get_attribute('href')
        # check that link can be scraped
        if rp.can_fetch(href, "*") == False:
            print(f'Restricted from scraping data from this URL: {href}')
            download_links = None
        else:
            page_driver = webdriver.Chrome(options=options)
            page_driver.get(href)
            # find all pdfs, excel files, or csv for download
            download_links = find_elements_with_retry(page_driver, By.CSS_SELECTOR, 'a[href^="/data/sites/"][href$=".pdf"], a[href^="/data/sites/"][href$=".xlsx"], a[href^="/data/sites/"][href$=".csv"], a[href^="/data/sites/"][href$=".xls"]')
        if len(download_links) > 0 and not download_links is None:
            # get info related to atricle page
            page_info = get_sahmsa_report_info(page_driver)
            page_meta = extract_meta(href, rp)
            # download each link and save to directory, and get metadata
            for dl in download_links:
                dl_url = dl.get_attribute('href')
                if meta_data["url"].str.contains(dl_url).any() == False:
                    report_name = download_report(dl_url,destination_folder, rp)
                    if report_name != "":
                        now = datetime.now()
                        date_time_string = now.strftime(METADATA_DATE_FMT)
                        cur_file_ext = re.sub(r'^.*\.', '', report_name)
                        cur_metadata = {'url':dl_url, 
                                        'datetime_captured': date_time_string,
                                        'title': page_info['Page Title'],
                                        'author': page_meta['author'],
                                        'publication_date': page_info['Publication Date'],
                                        'license': 'Public',
                                        'file_type': cur_file_ext,
                                        'storage_location': directory + "/" + report_name,
                                        'datetime_last_updated': date_time_string,
                                        'json_additional_metadata': combine_metadata_descriptions(page_meta)}
                        cur_metadata = pd.DataFrame([cur_metadata])
                        meta_data = pd.concat([meta_data, cur_metadata], ignore_index = True)
        page_driver.quit()

driver.quit()



meta_data.to_parquet(metapath, index = False)
 