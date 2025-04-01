
%run ./cdc_functions.ipynb
# set url and filepaths 
url = CDC_CASE_STUDY_URL
destination_folder = set_directory(folder = RAW_DATA_BASE_FOLDER, subfolder = CDC_CASE_STUDY_FOLDER)
metadata_filename = METADATA_PREFIX + CDC_CASE_STUDY_FOLDER + '.' + METADATA_EXT
metapath = get_metapath(METADATA_FOLDER, metadata_filename) 
cdc_meta_data = get_metadata(metapath)
# read robots.txt file
r = requests.get(CDC_ROBOTS_TXT)
rp = Protego.parse(r.text)
html_content = get_html_response(url, rp)
if not html_content is None:
    soup = BeautifulSoup(html_content.text, 'html.parser')
    
    # Find all links that end with .pdf
    pdf_links = soup.find_all('a', href=lambda href: href and href.endswith('.pdf'))
    # Download each PDF
    for link in pdf_links:
        pdf_url = urljoin(url, link['href'])
        # check that file has not already been downloaded
        if cdc_meta_data["url"].str.contains(pdf_url).any() == False:
            filename = re.sub(r'.*/', '',link['href'])
            file_title = re.sub(r'\.pdf', '', filename)
            download_res = download_file(pdf_url, destination_folder, filename, rp)
            # extract additional metadata if successfully downloaded file
            if not download_res is None:
                meta_data_info = extract_meta(pdf_url, rp)
                now = datetime.now()
                date_time_string = now.strftime(METADATA_DATE_FMT)
                update_date = get_update_date(link)
                cur_metadata = {'url': pdf_url, 
                                'datetime_captured': date_time_string,
                                'title': file_title,
                                'author': meta_data_info['author'],
                                'publication_date': update_date,
                                'license': CDC_LICENSE,
                                'file_type': CASE_STUDY_EXT,
                                'storage_location': str(destination_folder) + "/" + filename,
                                'datetime_last_updated': date_time_string,
                                'json_additional_metadata': combine_metadata_descriptions(meta_data_info)}
                cur_metadata = pd.DataFrame([cur_metadata])
                cdc_meta_data = pd.concat([cdc_meta_data, cur_metadata], ignore_index = True)
       

display(cdc_meta_data)
cdc_meta_data['json_additional_metadata'] = ''
cdc_meta_data.to_parquet(metapath)
 