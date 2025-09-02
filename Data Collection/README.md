### GooglePlaceSearch.py
====================================================

Overview:
This script use Google Place API to extract veterinary practices in any country
1. Loads the country's shapefile
2. Builds a lat/lng grid only inside the border of this country
3. Uses Google maps Places API to find veterinary clinics 
    (type='veterinary_care' is used to filter the properties that are related to veterinary practice)
4. Collects names, address, location, business status, types and website
5. Save practices separately based on the business status, operational or closed. The closed ones are saved for deduplication in the next steps. 

This script scrape vet pratice as much as possible because:
1. Grid-based search using actual country borders
    It generates points only within the selected country, based on the shapefile
    It ensures the scraper systematically and evenly covers an entire country, without relying on place names or administrative boundaries
    This avoids missing remote areas and prevents unnecessary API calls outside the country
2. Uses places_nearby() with radius
    Each grid point performs a search in a 10 km radius (configurable)
    Covers rural and urban areas alike
    You can adjust GRID_SPACING_KM and SEARCH_RADIUS to reduce overlap or increase coverage
3. Pagination handled
    The script handles next_page_token to fetch up to 60 results per location (3 pages × 20 each)
4. Deduplication via place_id
    Avoids counting the same clinic multiple times when it appears in overlapping search circles

OUTPUT file:
1. VP_GM.csv that contain all operational practices
2. VP_GM_dedup.csv contain all closed practices
--------------------
### GoogleTextSearch_city.py
====================================================

Overview:
This script use Google Map API to extract veterinary practices in any country using Text Search with a searching query "veterinarian in {city}, {country}". Each city has a limit 3 pages results (60 results in maximum).
1. Page 1: always search, if page 1 has less than 20 results, stop searching for this city
2. Page 2: conditional, if page 2 has less than 20 results or has 80% duplicated results, stop searching for page 3
3. Page 3: conditional, only when conditions in page 1 and 2 fulfilled

OUTPUT file:
1. VP_GM.csv that contain all operational practices
2. VP_GM_CLOSED.csv contain all closed practices
--------------------
### GoogleTextSearch_grid.py
====================================================

Overview:
This script use Google Map API to extract veterinary practices in any country using Text Search with a searching query "veterinarian" in grids made by shapefile of each country. Each grid has a limit 3 pages results (60 results in maximum).
1. Page 1: always search, if page 1 has less than 20 results, stop searching for this grid
2. Page 2: conditional, if page 2 has less than 20 results or has 80% duplicated results, stop searching for page 3
3. Page 3: conditional, only when conditions in page 1 and 2 fulfilled

OUTPUT file:
1. VP_GM.csv that contain all operational practices
2. VP_GM_CLOSED.csv contain all closed practices
--------------------
### OSM_PlaceSearch.py
====================================================

Overview:
This script use OpenStreetMap to extract veterinary practices in any country
1. Uses a country shapefile to generate a grid of points
2. For each point, queries OSM for 'amenity=veterinary'
3. Deduplicates based on OSM @id
4. Supports progress saving and resuming
5. Works with any country shapefile
6. Rows with both empty name and address are deleted

OUTPUT file:
1. progress.csv 
2. VP_OSM.csv
--------------------
### OSM_pbf.py
====================================================

Overview:
This script use OpenStreetMap to extract veterinary practices in any country, the data is stored as a pbf file that can be downloaded from [Geofabrik](https://download.geofabrik.de/)
1. Either download files for each country, or for continent and separate each country using the shapefile of their boundaries
2. Only extract data using 'amenity=veterinary'

OUTPUT file:
1. VP_OSM.csv
--------------------
### address_fill.py
====================================================

Overview:
reverse the geocoding process, for those only have latitude and longitude, using OpenStreetMap reverse tool to fill the text version address
