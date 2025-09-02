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
