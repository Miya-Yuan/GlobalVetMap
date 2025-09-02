### 1_Merge_File.py
=================================

Overview:
This Python script merges multiple CSV files—each potentially stored in different folders and with slightly different column names—into a single, clean CSV output.

What it does:
1. Normalizes column names by removing punctuation, spaces, and casing inconsistencies.
2. Fuzzy matches columns to correct typos and align with the expected schema.
3. Keeps only the specified final columns in a consistent order.
4. Fills missing columns with empty strings ("").
5. Concatenates all cleaned files into one.
6. Drops duplicates based on the "Name" column.

INPUT files:
1. vet practices csv files from national website, phonebook and OSM

OUTPUT file:
1. merged_output.csv
----------------------
### 2_Data_Cleaning.py
=================================

Overview:
This script performs deduplication, validation, geocoding, and integration of veterinary practice data from two main sources:
1. Website dataset including the data from websites and OSM.
2. Google Maps dataset.
It uses fuzzy string matching (Levenshtein similarity = 85%) to identify similar or identical practice names+addresses and merges the data with preference for the Google Maps record when a match is found.

What it does:
1. Preprocess & Deduplicate Google Data: filter bad entries using fuzzy bad word list; apply spatial deduplication using DBSCAN clustering (within 50m); score entries using available metadata (Specialization and Website), keep highest scoring per cluster.
2. Matches the combination of each website practice name and address to the most similar one in the Google Maps dataset. Remove closed practices using Google Map data that is labeled as "closed".
	a. If a match is found, keep the Google Maps record, and merges in any extra columns from the website database.
	b. If no match is found, keeps the original record, regardless of which dataset it came from. Unmatched Google records are preserved with any missing field filled as blank to ensure consistency.
3. Each row is compared against the list of known closed practices using two levels of fuzzy matching:
	a. Primary: Match on Name + Address (threshold ≥ 85)
	b. Fallback: Match on Address only (threshold ≥ 90)
4. Geocode remaining entries using OpenStreetMap, with Googla Maps API as fallback.
5. Country border filtering and spatial deduplication.
	a. Ensures all practices fall within the country boundary using a shapefile.
	b. Runs DBSCAN again on the final dataset to eliminate last-mile spatial duplicates.
Matching rows are excluded from the final output.

INPUT files:
1. merged_output.csv from 1_Merge_File.py
2. vet practice csv files extracted from Google Map (both open and closed practices)
3. nonclinic_keywords.csv

OUTPUT file:
1. VP_text_matched.csv
2. VP_geocoded.csv
3. VP_cleaned.csv
----------------------
### 3_Web_Finding.py
=================================

Overview:
This script automates the process of finding official website URLs for veterinary practices listed in a CSV file. It uses Selenium to perform Google searches (use Bing as fallback) to identify and fill in missing website information.

What it does:
1. Searching the practice's name + address on Google. Skip rows with missing names and already-filled website fields
2. Collect the first URL on Google search result page. Retries up to 3 times per row if an error occurs, use Bing as a fallback if Google is blocked or failed. Blacklisted URLs stored in blacklist_config.py are excluded. Any URL link that is not starting with http or https is excluded.
3. Writing the resolved URL back to the dataset

INPUT file:
1. VP_cleaned.csv from
2. Name_Match.py

OUTPUT file:
1. VP_website_filled.csv
----------------------
### 4_Category_Specialization.py
=================================

Overview:
This Python script automates the identification and classification of veterinary practice websites. It determines whether a site belongs to a veterinary clinic and, if so, detects the animal species treated (e.g., small animals, large animals, horses). It also handles multilingual cookie consent banners and supports scraping both the homepage and service-related subpages for improved accuracy.

What it does:
1. Veterinary detection: detects if a website is a veterinary clinic (yes / no / uncertain) using multilingual veterinary-related keywords 
	a. If the website field is empty or invalid, the row is marked as Clinic = uncertain
	b. If fetching the initial URL fails or no vet keywords are found: extract the root homepage, reattempt the fetch and vet keywords search using the normalized homepage. If successful, update the website column in the row
2. Animal specialization extraction: identifies animal species treated (small animals, large animals, horses) using fuzzy matching against language-specific keyword sets
	a. If specialization is already filled, no further detection is done
	b. Otherwise, searches homepage text for animal category keywords. If not found, it searches service-related pages linked from the homepage using service-related anchor text. If animal types are detected, specialization is 		updated
3. Service page scraping: automatically follows internal links to service-related pages when homepage analysis in inconclusive
4. Splits final results into two CSVs: vet_or_uncertain and non_vet

INPUT file:
1. VP_website_filled.csv from 3_Web_Finding.py
2. vet_keywords.csv
3. animal_keywords.csv
4. nonclinic_keywords.csv
5. service_config.py
6. cookie_config.py

OUTPUT file:
1. VP_filtered_vet_or_uncertain.csv
2. VP_filtered_not_vet.csv
----------------------
### 5_TeamPage_Text.py
=================================

Overview:
This Python script find and extract team page of the practice websites. It converts team page and possible profiles HTML to text files and save them for further information extraction. 

What it does:
1. Team webpage finding: use keywords and blacklist keywords from team_config to detect possible team webpage and the team profiles of the practice website.
2. Team webpage extraction: convert the HTML of the team and profile webpages to text file, if text file cannot be converted, take screenshot of the team page found.
3. Save the text file and image file in the same folder, seperate the large text files that have more than 100,000 characters in another folder for debugging.

INPUT file:
1. VP_filtered.csv
2. team_config.py

OUTPUT file:
1. VP_text_image
2. VP_text_large
----------------------
### 6_Specialization_withTeam.py
=================================

Overview:
This Python script extract clinic and specialization information (animal species treated) from the extracted text file of the team page of the practices website. 

What it does:
1. Veterinary detection: reprocess the rows that marked as "uncertain" in the step 4.
2. Animal species detection: reprocess the rows that Specialization column has no valid value.

INPUT file:
1. VP_filtered.csv
2. VP_text_image
3. animal_keywords.csv
4. vet_keywords.csv
5. nonclinic_keywords.csv

OUTPUT file:
1. VP_filtered_team.csv
----------------------
### 7_Image_to_Text_GPT.py
=================================

Overview:
This Python script extract text from image using OpenAI API.

What it does:
Detect image file from the folder from step 5, and extract the text inside the image using OCR.

INPUT file:
1. VP_text_image

OUTPUT file:
1. VP_text_image
----------------------
### 8_TeamText_GPT.py
=================================

Overview:
This Python script extract team information from text file using OpenAI API.

What it does:
1. Extract names and roles of the employees from the practices website text file from the folder saved from step 5, save the json files in the cache folder.
2. Extract team information from the json files, and count the number of female doctor (FD), male doctor (MD), female non-doctor (FND) and male non-doctor (MND).

INPUT file:
1. VP_text_image
2. VP_filtered_team.csv

OUTPUT file:
1. team_cache
2. VP_team.csv
----------------------
### 9_Team_reprocess.py
=================================

Overview:
This Python script reprocess the extraction of team information from json file saved in step 8.

What it does:
1. Extract team information from the json files, and count the number of female doctor (FD), male doctor (MD), female non-doctor (FND) and male non-doctor (MND) for those failed in the first try.
2. Copy those failed text in another folder for debugging.

INPUT file:
1. VP_text_image
2. VP_team.csv
3. team_cache

OUTPUT file:
1. Failed_text
----------------------
### blacklist_config.py
=================================

This blacklist is used to exclude non-authoritative websites when identifying official veterinary clinic websites through automated web search and classification pipelines. It enforces a strict filtering policy, ensuring that results are limited to genuine, standalone veterinary practice websites — not social media pages, directories, aggregator listings, landing pages, or hosted page builders.

The blacklist includes domain patterns associated with:
1. Social media platforms: (e.g. facebook.com, instagram.com, linkedin.com)
2. Global and regional business directories: (e.g. yelp.com, pagesjaunes.fr, gelbeseiten.de, local.ch)
3. Hosted site builders and landing page platforms: (e.g. wixsite.com, site123.me, strikingly.com, clickfunnels.com)
4. Content platforms, personal blogs, or generic profiles: (e.g. wordpress.com, blogspot.com, about.me, tumblr.com)
5. Review/appointment aggregators and lead gen tools: (e.g. trustpilot.com, healthgrades.com, zocdoc.com)
These domains are not acceptable as authoritative veterinary websites in data collection or classification pipelines.
