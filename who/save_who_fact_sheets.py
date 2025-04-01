
%run ./who_functions.ipynb
# set url and filepaths 
url = WHO_FACT_SHEETS_URL
destination_folder = set_directory(folder = RAW_DATA_BASE_FOLDER, subfolder = WHO_FACT_SHEETS_FOLDER)
metadata_filename = METADATA_PREFIX + WHO_FACT_SHEETS_FOLDER + '.' + METADATA_EXT
metapath = get_metapath(METADATA_FOLDER, metadata_filename) 
sheet_meta_data = get_metadata(metapath)
# read robots.txt file
r = requests.get(WHO_ROBOTS_TXT)
rp = Protego.parse(r.text)
fact_sheet_links = get_fact_sheet_links(url, rp)
for link in fact_sheet_links:
    if not link is None:
        # check that the document has not already been downloaded
        if sheet_meta_data["url"].str.contains(link).any() == False:
            fact_sheet_meta = save_fact_sheet(link, destination_folder, rp)
            if fact_sheet_meta != "" and fact_sheet_meta is not None:
                # extract additional metadata
                meta_data_info = extract_meta(link, rp)
                now = datetime.now()
                date_time_string = now.strftime(METADATA_DATE_FMT)
                cur_metadata = {'url': link, 
                                'datetime_captured': date_time_string,
                                'title': meta_data_info['title'],
                                'author': meta_data_info['author'],
                                'publication_date': fact_sheet_meta[1],
                                'license': WHO_LICENSE,
                                'file_type': FACT_SHEETS_EXT,
                                'storage_location': str(destination_folder) + "/" + fact_sheet_meta[0],
                                'datetime_last_updated': date_time_string,
                                'json_additional_metadata': combine_metadata_descriptions(meta_data_info)}
                cur_metadata = pd.DataFrame([cur_metadata])
                sheet_meta_data = pd.concat([sheet_meta_data, cur_metadata], ignore_index = True)

sheet_meta_data.to_parquet(metapath, index = False)
 