# Import what we need

import os 
import json 
import pandas as pd
import re # Regular expressions, to help us parse html, json.
import requests # To pull internet pages
import time # To allow us to "sleep" the scraping, so as not to overwhelm the servers
import numpy as np
import csv
from bs4 import BeautifulSoup
import random

# Scrape Listings
cities = ['asheville',
         'austin',
         'boston',
         'browardcounty',
         'cambridge',
         'chicago',
         'clarkcounty',
         'columbus',
         'dallas',
         'denver',
         'hawaii',
         'jerseycity',
         'losangeles',
         'nasheville',
         'neworleans',
         'newyorkcity',
         'oakland',
         'pacificgrove',
         'portland',
         'rhodeisland',
         'salem',
         'sandiego',
         'sanfrancisco',
         'sanmateo',
         'santaclara',
         'santacruz',
         'seattle',
         'twincities',
         'washington']



propertyIDs = []
propertyCities = []

headers_search = {'Accept':'*/*',
           'Accept-Encoding':'gzip, deflate, br',
           'Accept-Language':'en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7',
           'Apollographql-Client-Name':'web_serp',
           'Content-Length':'10658',
           'Content-Type':'application/json',
           'Cookie':'hal=ga=1&ua=1&si=1&ui=1&vi=1&pr=0; 83025ed8-3345-7b00-68ae-ecaad79262b9SL=1; HMS=be8200bb-3f93-4606-9929-9ce3f1a027b3; hav=c494c01c-fd5c-8838-a354-701c06a9f619; MC1=GUID=c494c01cfd5c8838a354701c06a9f619; DUAID=c494c01c-fd5c-8838-a354-701c06a9f619; ha-device-id=c494c01c-fd5c-8838-a354-701c06a9f619; hav=c494c01c-fd5c-8838-a354-701c06a9f619; has=83025ed8-3345-7b00-68ae-ecaad79262b9; eu-site=0; ak_bmsc=DCB0EED250C645ED4A73947CA4162455~000000000000000000000000000000~YAAQCY0hFx2X6kuIAQAAxs0PZBOuLz72ix3H5hczIg37bk2pfY1To3/KWAkbpI6VWAVOMJAwOA3670dzouKEXaLHh/RombvUmy6yulGN4CEhm6jkokybJ0kugCRF/LmQ02huwBo2BD2TiEG66BCpkIr8cZAtSQocR8O48PwjO6+Q27HrfGyl/Mgupu3HZrWTxR5+QunCUhQZeqTJG+EZL44I4Yu7ASqlvwW10DL4fbhMMt8MgMS0Z20YH1MdNBXy47eCvPIj7vslzBJ3qnHl+ROCvu8DVhXv4TLHlSaR9ZpGlK0/P8kmX01/TJqj8jdMGNnuH30IaNMqNo8BjNnqFqRzNMZ7no9wDfWmDM8/4RPXao0W1AB5ZetZQkAHL8xli+97U0yOmIXT; DUAID=c494c01c-fd5c-8838-a354-701c06a9f619; _ga=GA1.2.68000584.1685305941; _gid=GA1.2.1366219359.1685305941; _gac_UA-188611-1=1.1685305941.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; ensighten:source={"source":"GOOGLE","medium":"cpc","lastAffiliate":null,"sessionid":"83025ed8-3345-7b00-68ae-ecaad79262b9"}; 83025ed8-3345-7b00-68ae-ecaad79262b9UAL=1; site=vrbo; ln_or=eyIxMjI2OTUzIjoiZCJ9; ta_timeout=1; _gcl_au=1.1.1229644858.1685305946; _ha2_aw=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; _ha2_dc=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; _gcl_aw=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; _gcl_dc=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; _ha_aw=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; _ha_dc=GCL.1685305946.Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB; xdid=a8667815-7fcc-4678-ace2-ae2405322f62|1685305953|vrbo.com; eg_ppid=6e924c3d-975e-45ac-afdb-23095aabaaa7; __utmuaepi=home%20page:home; crumb=AxtQ_Hvf6gjQ3g5bEtxl6jGBnorYn_ASNr9X8jHsRs6; edge-polyfill-device-classification=10; SawSIalready=Yes; cesc=%7B%22gclid%22%3A%5B%22Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB%22%2C1685305941511%5D%2C%22marketingClick%22%3A%5B%22false%22%2C1685306825802%5D%2C%22hitNumber%22%3A%5B%2214%22%2C1685306825802%5D%2C%22visitNumber%22%3A%5B%221%22%2C1685305937328%5D%2C%22cidVisit%22%3A%5B%22SEM.VRBO-US.B.GOOGLE.BT-c-EN.GT%22%2C1685306825802%5D%2C%22entryPage%22%3A%5B%22Zq9wZdD0HsM0wH%2BVQfYb5CSu7%2BSYNJo7XZZZeMDWxTg%3D%22%2C1685306825802%5D%2C%22sem%22%3A%5B%22SEM.VRBO-US.B.GOOGLE.BT-c-EN.GT.k_user_id.%22%2C1685305941511%5D%2C%22seo%22%3A%5B%22SEO.U.google.com%22%2C1685305937328%5D%2C%22semdtl%22%3A%5B%22a118251470060%3Ab1141973518615%3Ag1kwd-13405466%3Al1%3Ae1c%3Am1Cj0KCQjw98ujBhCgARIsAD7QeAi-v9jjH8ojBLQ1OuYPh3bzj2zjHsuG2IgngyLn2YovQttCmx3K_VQaAp3oEALw_wcB%3Ar1499329b22b12540d5a8716f7ebc04d327bd8304dc546ac97134c31fc48e20280%3Ac1O741gx-3gIsTI6MUvk7V2w%3Aj19028819%3Ak1%3Ad1624922945643%3Ah1e%3Ai1%3An1%3Ao1%3Ap1%3Aq1%3As1%3At1%3Ax1%3Af1%3Au1%3Av1%3Aw1%22%2C1685305941511%5D%2C%22cid%22%3A%5B%22SEM.VRBO-US.B.GOOGLE.BT-c-EN.GT%22%2C1685305941511%5D%7D; cto_bundle=Pxz6719nTzZsa3FaUUlSWlRoMWtsdkp1YUJJZ2FaNmt1Z1RJRHVvUkgxNXdVRWQlMkZLZnNubGltODFibmd6VEtBdmJ6dE1Nb0xnZGd6ZFhsZE1ubHhDWDV1VXpJMkJwciUyQmRiNWdlbEVTS2FVVnhLbFlUN0k1Z1BwRnhFY0J3S2pDS25XQ3pwYVNOV1EzWDdPZ2o4WDE3ZUI3JTJCZlElM0QlM0Q; edge-polyfill-location=city%3DBOULDER%2C%20region%3DCO%2C%20country%3DUS%2C%20lat%3D40.0440%2C%20lng%3D-105.194%2C%20asn%3D104%2C%20city%3DBOULDER%2C%20region%3DCO%2C%20country%3DUS%2C%20lat%3D40.0440%2C%20lng%3D-105.194%2C%20asn%3D104; ha-trip-prst=%7B%22petIncluded%22%3Afalse%7D; _gat_edap=1; QSI_HistorySession=https%3A%2F%2Fwww.vrbo.com%2Fsearch%2Fkeywords%3Aminneapolis-minnesota-united-states-of-america%2FminNightlyPrice%2F0%3FfilterByTotalPrice%3Dtrue%26petIncluded%3Dfalse%26ssr%3Dtrue~1685306571662%7Chttps%3A%2F%2Fwww.vrbo.com%2Fsearch%2Fkeywords%3Aasheville~1685306635698%7Chttps%3A%2F%2Fwww.vrbo.com%2Fsearch%2Fkeywords%3Aasheville-north-carolina-united-states-of-america%2FminNightlyPrice%2F0%3FfilterByTotalPrice%3Dtrue%26petIncluded%3Dfalse%26ssr%3Dtrue~1685307049721; ha-state-prst=%7B%22lbsKeywords%22%3A%22Asheville%2C%20North%20Carolina%2C%20United%20States%20of%20America%22%2C%22lastSearchUrl%22%3A%22%2Fsearch%2Fkeywords%3Aasheville-north-carolina-united-states-of-america%2FminNightlyPrice%2F0%3FfilterByTotalPrice%3Dtrue%26petIncluded%3Dfalse%26ssr%3Dtrue%22%7D; bm_sv=64243C0096EA39765E8033218DD8A32C~YAAQHo0hF8c3QziIAQAAq88gZBOB4GoJEQjjRHqD1AbOVOQD5kkE4Nz8f2I49wp9q44vVPGGr2r4kNTX9Q1drw/goe37FRKWMn7jZOy6eStmEN+KdBu+27Mytk4hAdbo22Y+99UyTTQFy9uCjXeClJSZ9/IqAh589wV/Hx6ZEwH1bXvQuObQ9kkv3byURsiP7PPVtpKoDhavRcf+7VYbZHnhHFz2iVN/P2uLkwWJ5cPdN3418lmpxOLTmVg9Bxo=~1; _uetsid=c1ac1b80fd9611ed92702f3a1f8429b1; _uetvid=c1ac8b40fd9611ed8e2e710d9b5ea735; _dd_s=rum=0&expire=1685307982918',
           'Ha-Serp-Force-Graphql-Err':'undefined',
           'Origin':'https://www.vrbo.com',
           'sec-ch-ua': 'Google Chrome;v=113, Chromium;v=113, Not-A.Brand;v=24',
           'Sec-Ch-Ua-Mobile':'?0',
           'Sec-Ch-Ua-Platform':"macOS",
           'Sec-Fetch-Dest':'empty',
           'Sec-Fetch-Mode':'cors',
           'Sec-Fetch-Site':'same-origin',
           'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
           'X-Csrf-Token':'AxtQ_Hvf6gjQ3g5bEtxl6jGBnorYn_ASNr9X8jHsRs6',
           'X-Homeaway-Site':'vrbo'}

for city in cities:
    search_url = f'https://www.vrbo.com/search/keywords:{city}'
    time.sleep(5)
    try:
        query_search = """query SearchRequestQuery(
            $request: SearchResultRequest!, 
            $filterCounts: Boolean!, 
            $optimizedBreadcrumb: Boolean!, 
            $vrbo_web_global_messaging_banner: Boolean!
            ) 
            {
            results: search(
            request: $request
              )
              {
            ...querySelectionSet
            ...DestinationBreadcrumbsSearchResult
            ...DestinationMessageSearchResult
            ...FilterCountsSearchRequestResult
            ...HitCollectionSearchResult
            ...ADLSearchResult
            ...MapSearchResult
            ...ExpandedGroupsSearchResult
            ...PagerSearchResult
            ...InternalToolsSearchResult
            ...SEOMetaDataParamsSearchResult
            ...GlobalInlineMessageSearchResult
            ...GlobalBannerContainerSearchResult @include(if: $vrbo_web_global_messaging_banner)
            __typename
          }
        }

        fragment querySelectionSet on SearchResult {
          id
          typeaheadSuggestion {
            uuid
            term
            name
            __typename
          }
          geography {
            lbsId
            gaiaId
            location {
              latitude
              longitude
              __typename
            }
            isGeocoded
            shouldShowMapCentralPin
            __typename
          }
          propertyRedirectUrl
          __typename
        }

        fragment DestinationBreadcrumbsSearchResult on SearchResult {
          destination(optimizedBreadcrumb: $optimizedBreadcrumb) {
            breadcrumbs {
              name
              url
              __typename
            }
            __typename
          }
          __typename
        }

        fragment HitCollectionSearchResult on SearchResult {
          page
          pageSize
          pageCount
          queryUUID
          percentBooked {
            currentPercentBooked
            __typename
          }
          listings {
            ...HitListing
            __typename
          }
          resultCount
          pinnedListing {
            headline
            listing {
              ...HitListing
              __typename
            }
            __typename
          }
          __typename
        }

        fragment HitListing on Listing {
          virtualTourBadge {
            name
            id
            helpText
            __typename
          }
          amenitiesBadges {
            name
            id
            helpText
            __typename
          }
          images {
            altText
            c6_uri
            c9_uri
            mab {
              banditId
              payloadId
              campaignId
              cached
              arm {
                level
                imageUrl
                categoryName
                __typename
              }
              __typename
            }
            __typename
          }
          ...HitInfoListing
          __typename
        }

        fragment HitInfoListing on Listing {
          listingId
          ...HitInfoDesktopListing
          ...HitInfoMobileListing
          ...PriceListing
          __typename
        }

        fragment HitInfoDesktopListing on Listing {
          detailPageUrl
          instantBookable
          minStayRange {
            minStayHigh
            minStayLow
            __typename
          }
          listingId
          listingNumber
          rankedBadges(rankingStrategy: SERP) {
            id
            helpText
            name
            __typename
          }
          propertyId
          propertyMetadata {
            headline
            __typename
          }
          superlativesBadges: rankedBadges(
              rankingStrategy: SERP_SUPERLATIVES
              ) 

          {
            id
            helpText
            name
            __typename
          }
          unitMetadata {
            unitName
            __typename
          }
          webRatingBadges: rankedBadges(rankingStrategy: SRP_WEB_RATING) {
            id
            helpText
            name
            __typename
          }
          ...DetailsListing
          ...GeoDistanceListing
          ...PriceListing
          ...RatingListing
          __typename
        }

        fragment DetailsListing on Listing {
          bathrooms {
            full
            half
            toiletOnly
            __typename
          }
          bedrooms
          propertyType
          sleeps
          petsAllowed
          spaces {
            spacesSummary {
              area {
                areaValue
                __typename
              }
              bedCountDisplay
              __typename
            }
            __typename
          }
          __typename
        }

        fragment GeoDistanceListing on Listing {
          geoDistance {
            text
            relationType
            __typename
          }
          __typename
        }

        fragment PriceListing on Listing {
          priceSummary: priceSummary {
            priceAccurate
            ...PriceSummaryTravelerPriceSummary
            __typename
          }
          priceSummarySecondary: priceSummary(summary: \"displayPriceSecondary\") {
            ...PriceSummaryTravelerPriceSummary
            __typename
          }
          priceLabel: priceSummary(summary: \"priceLabel\") {
            priceTypeId
            pricePeriodDescription
            __typename
          }
          prices {
            ...VrboTravelerPriceSummary
            __typename
          }
          __typename
        }

        fragment PriceSummaryTravelerPriceSummary on TravelerPriceSummary {
          priceTypeId
          edapEventJson
          formattedAmount
          roundedFormattedAmount
          pricePeriodDescription
          __typename
        }

        fragment VrboTravelerPriceSummary on PriceSummary {
          perNight {
            amount
            formattedAmount
            roundedFormattedAmount
            pricePeriodDescription
            __typename
          }
          total {
            amount
            formattedAmount
            roundedFormattedAmount
            pricePeriodDescription
            __typename
          }
          label
          mainPrice
          __typename
        }

        fragment RatingListing on Listing {
          averageRating
          reviewCount
          __typename
        }

        fragment HitInfoMobileListing on Listing {
          detailPageUrl
          instantBookable
          minStayRange {
            minStayHigh
            minStayLow
            __typename
          }
          listingId
          listingNumber
          rankedBadges(rankingStrategy: SERP) {
            id
            helpText
            name
            __typename
          }
          propertyId
          propertyMetadata {
            headline
            __typename
          }
          superlativesBadges: rankedBadges(rankingStrategy: SERP_SUPERLATIVES) {
            id
            helpText
            name
            __typename
          }
          unitMetadata {
            unitName
            __typename
          }
          webRatingBadges: rankedBadges(rankingStrategy: SRP_WEB_RATING) {
            id
            helpText
            name
            __typename
          }
          ...DetailsListing
          ...GeoDistanceListing
          ...PriceListing
          ...RatingListing
          __typename
        }

        fragment ExpandedGroupsSearchResult on SearchResult {
          expandedGroups {
            ...ExpandedGroupExpandedGroup
            __typename
          }
          __typename
        }

        fragment ExpandedGroupExpandedGroup on ExpandedGroup {
          listings {
            ...HitListing
            ...MapHitListing
            __typename
          }
          mapViewport {
            neLat
            neLong
            swLat
            swLong
            __typename
          }
          __typename
        }

        fragment MapHitListing on Listing {
          ...HitListing
          geoCode {
            latitude
            longitude
            __typename
          }
          __typename
        }

        fragment FilterCountsSearchRequestResult on SearchResult {
          id
          resultCount
          filterGroups {
            groupInfo {
              name
              id
              __typename
            }
            filters {
              count @include(if: $filterCounts)
              checked
              filter {
                id
                name
                refineByQueryArgument
                description
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }

        fragment MapSearchResult on SearchResult {
          mapViewport {
            neLat
            neLong
            swLat
            swLong
            __typename
          }
          page
          pageSize
          listings {
            ...MapHitListing
            __typename
          }
          pinnedListing {
            listing {
              ...MapHitListing
              __typename
            }
            __typename
          }
          __typename
        }

        fragment PagerSearchResult on SearchResult {
          fromRecord
          toRecord
          pageSize
          pageCount
          page
          resultCount
          __typename
        }

        fragment DestinationMessageSearchResult on SearchResult {
          destinationMessage(assetVersion: 4) {
            iconTitleText {
              title
              message
              icon
              messageValueType
              link {
                linkText
                linkHref
                __typename
              }
              __typename
            }
            ...DestinationMessageDestinationMessage
            __typename
          }
          __typename
        }

        fragment DestinationMessageDestinationMessage on DestinationMessage {
          iconText {
            message
            icon
            messageValueType
            __typename
          }
          __typename
        }

        fragment ADLSearchResult on SearchResult {
          parsedParams {
            q
            coreFilters {
              adults
              children
              pets
              minBedrooms
              maxBedrooms
              minBathrooms
              maxBathrooms
              minNightlyPrice
              maxNightlyPrice
              minSleeps
              __typename
            }
            dates {
              arrivalDate
              departureDate
              __typename
            }
            sort
            __typename
          }
          page
          pageSize
          pageCount
          resultCount
          fromRecord
          toRecord
          pinnedListing {
            listing {
              listingId
              __typename
            }
            __typename
          }
          listings {
            listingId
            __typename
          }
          filterGroups {
            filters {
              checked
              filter {
                groupId
                id
                __typename
              }
              __typename
            }
            __typename
          }
          geography {
            lbsId
            name
            description
            location {
              latitude
              longitude
              __typename
            }
            primaryGeoType
            breadcrumbs {
              name
              countryCode
              location {
                latitude
                longitude
                __typename
              }
              primaryGeoType
              __typename
            }
            __typename
          }
          __typename
        }

        fragment InternalToolsSearchResult on SearchResult {
          internalTools {
            searchServiceUrl
            __typename
          }
          __typename
        }

        fragment SEOMetaDataParamsSearchResult on SearchResult {
          page
          resultCount
          pageSize
          geography {
            name
            lbsId
            breadcrumbs {
              name
              __typename
            }
            __typename
          }
          __typename
        }

        fragment GlobalInlineMessageSearchResult on SearchResult {
          globalMessages {
            ...GlobalInlineAlertGlobalMessages
            __typename
          }
          __typename
        }

        fragment GlobalInlineAlertGlobalMessages on GlobalMessages {
          alert {
            action {
              link {
                href
                text {
                  value
                  __typename
                }
                __typename
              }
              __typename
            }
            body {
              text {
                value
                __typename
              }
              link {
                href
                text {
                  value
                  __typename
                }
                __typename
              }
              __typename
            }
            id
            severity
            title {
              value
              __typename
            }
            __typename
          }
          __typename
        }

        fragment GlobalBannerContainerSearchResult on SearchResult {
          globalMessages {
            ...GlobalBannerGlobalMessages
            __typename
          }
          __typename
        }

        fragment GlobalBannerGlobalMessages on GlobalMessages {
          banner {
            body {
              text {
                value
                __typename
              }
              link {
                href
                text {
                  value
                  __typename
                }
                __typename
              }
              __typename
            }
            id
            severity
            title {
              value
              __typename
            }
            __typename
          }
          __typename
        }
        """
# Mess with different search terms, because VRBO will only send us 500 in total
        for beds in [0, 2, 4, 6, 8]:
            minBeds = beds
            maxBeds = beds + 1
            
            for price in [0, 200, 400, 600, 800, 10000]:
                minPrice = price
                maxPrice = price + 199
                
                for page in range(1, 4):
                    time.sleep(2)
                    json_search = {'operationName':"SearchRequestQuery",
                                   'variables':{
                                       'filterCounts':True,
                                              'request':{
                                                  'paging':{
                                                      'page':page,
                                                      'pageSize':200},
                                                  'filterVersion':"1",
                                                  'filters':[],
                                                  'coreFilters':{
                                                      'maxBathrooms':None,
                                                      'maxBedrooms':maxBeds,
                                                      'maxNightlyPrice':maxPrice,
                                                      'maxTotalPrice':None,
                                                      'minBathrooms':0,
                                                      'minBedrooms':minBeds,
                                                      'minNightlyPrice':minPrice,
                                                      'minTotalPrice':None,
                                                      'pets':0},
                                                  'q':f"{city}-united-states-of-america"},
                                       'optimizedBreadcrumb':False,
                                       'vrbo_web_global_messaging_banner':True},
                                   'extensions':{
                                       'isPageLoadSearch':False},
                                   'query':query_search}

                    req = requests.post(url = 'https://www.vrbo.com/serp/g', headers = headers_search, json = json_search)
                    data = json.loads(req.text)['data']['results']['listings']

                    for entry in data:
                        property_id = entry['propertyId']
                        citi = city
                        propertyIDs.append(property_id)
                        propertyCities.append(citi)
                        
    except:
        continue
                        
# Writing the combined data to a CSV file
with open('vrbo_pids.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['propertyId', 'city'])  # Write header row
    writer.writerows(list(zip(propertyIDs, propertyCities)))
    
    
# Read in a list of the property IDs, set it to filtered_list
pid_list = set(pd.read_csv('VRBO/vrbo_pids.csv').iloc[:, 0].values)

scraped_list = set(pd.read_csv('VRBO/vrbo_properties.csv').iloc[:, 0].values)

# Create an empty list to hold property data
property_data = []

# Create an empty list to hold property ids who we can't scrape
#missed_pids = []

# Create a list of headers we'll rotate between to scrape properties
properties_headers = [
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Host': 'www.vrbo.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.vrbo.com/1526575',
        'Connection': 'keep-alive'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Host': 'www.vrbo.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.vrbo.com/1526575',
        'Connection': 'keep-alive'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://www.vrbo.com/1526575',
        'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Host': 'www.vrbo.com',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0'
    }
]

# Loop through the property IDs that are held in filtered_lists
for pid in pid_list:
    if pid not in scraped_list:
    
        # Set the scraper to sleep for seconds between runs CHILL OUT MAN
        time.sleep(random.uniform(1, 3))

        # Set a random header to pick from properties
        random_properties_headers = random.choice([0,1,2,3,4,5,6])
    
        # Request the property page
        req = requests.get(url=f'https://www.vrbo.com/{pid}', 
                           headers = properties_headers[random_properties_headers])

        # Use beautiful soup to get some of the info prettier
        # Note that this is completely unecessary. I was thinking of using bs for more
        html = BeautifulSoup(req.text, 'html.parser')
        
        if html.title == None:
            continue
        else:
            if html.title.text == "Bot or Not?":
                print("STOPPED! BOT")
                break
            else:
                # Try to pull property info using regular expressions
                try: 
                    title = html.title.text
                except:
                    title = "NA"
                price = re.search(r"\s\$(\d+)\s", req.text)[1] if re.search(r"\s\$(\d+)\s", req.text) else "NA"
                host_name = re.search(r"Hosted\sby\s(.*?)\</h4", req.text)[1] if re.search(r"Hosted\sby\s(.*?)\</h4", req.text) else "NA"
                premierHost = 1 if re.search(r"is\sa\sPremier\sHost", req.text) else 0
                sleeps = re.search(r"Sleeps\s(.*?)\</li", req.text)[1] if re.search(r"Sleeps\s(.*?)\</li", req.text) else "NA"
                latitude = re.search(r"\"latitude\":(.*?\d+\.\d+)", req.text)[1] if re.search(r"\"latitude\":(.*?\d+\.\d+)", req.text) else "NA"
                longitude = re.search(r"\"longitude\":(.*?\d+\.\d+)", req.text)[1] if re.search(r"\"longitude\":(.*?\d+\.\d+)", req.text) else "NA"
                avg_rate = re.search(r"\"average\":(\d\.\d+)", req.text)[1] if re.search(r"\"average\":(\d\.\d+)", req.text) else "NA"
                count_rate = re.search(r"\"reviewCount\":\s(\d+)", req.text)[1] if re.search(r"\"reviewCount\":\s(\d+)", req.text) else "NA"
                description = re.search(r"description\":\"(.*)\",\"detailPageUrl\"", req.text)[1] if re.search(r"description\":\"(.*)\",\"detailPageUrl\"", req.text) else "NA"
                try:
                    features = [] if re.search(r'"amenityFeature": \[(.*?)\]', req.text, re.DOTALL) == None else re.findall(r'"name":\s*"([^"]+)",\s*"value":\s*"([^"]+)"', re.search(r'"amenityFeature": \[(.*?)\]', req.text, re.DOTALL)[1])
                except AttributeError:
                    features = []
                if features:
                    feature_dict = {name: 1 if value == "true" else 0 for name, value in features}
                else:
                    feature_dict = {}

                # Set the listing ID. We need this to scrape reviews later

                listing_id = re.search(r"\"listingId\":\"(.*?)\",", req.text, re.DOTALL)[1] if re.search(r"\"listingId\":\"(.*?)\",", req.text, re.DOTALL) else "NA"


                # Create a dictionary for the property data
                property_dict = {
                    'pid': pid,
                    'title': title,
                    'price': price,
                    'host_name': host_name,
                    'premierHost': premierHost,
                    'sleeps': sleeps,
                    'latitude': latitude,
                    'longitude': longitude,
                    'avg_rate': avg_rate,
                    'count_rate': count_rate,
                    'description': description,
                    'listing_id': listing_id,
                    **feature_dict  # Add feature columns dynamically
                }

                # Append the property dictionary to the list
                property_data.append(property_dict)
    else:
        continue

    
# Convert the list of dictionaries to a pandas DataFrame
df = pd.DataFrame(property_data)

df.to_csv(f'VRBO/vrbo_properties.csv', index=False)


reviews_headers = [
    {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.vrbo.com",
        "Origin": "https://www.vrbo.com",
        "Content-Length": "967",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
        "Referer": "",
        "apollographql-client-version": "2.0",
        "x-csrf-jwt-pdp": 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3ZjEzYjVlNWIyMWNlNDE0NTE3NzdlMTAyNjFiYzJlZWU2M2I2OTc5NWM5YzljMGYzNWNlNjIzOTcyZGYzN2JhZDY1M2U0ZTUwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYwYjBhZTUzYmMxNWZlOGY4MCIsImlhdCI6MTY4NTQ5ODE0MSwiZXhwIjoxNjg2MTAyOTQxfQ.hzxOxKnyOpQZu-q3v93t5TuIsKLwngW7tQ3Yb22c2Y0',
        "x-homeaway-site": "vrbo",
        "apollographql-client-name": "web_pdp",
        "x-ha-location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922"
    },
    {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "Content-Length": "969",
        "Content-Type": "application/json",
        "Origin": "https://www.vrbo.com",
        "Referer": "",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"macOS\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Csrf-Jwt-Pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3NjEzZTRlN2U2NDdlOTEyNTEyODJlMTY3NTFiOTBiYmUxMzY2OTI5NTg5OWNlMGYzYzkzNjgzYjcyZGY2NWJkZDQ1NGJhYjMwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYzYjJhYTVjYmExYWZmOGY4MCIsImlhdCI6MTY4NTQ5MjYxNywiZXhwIjoxNjg2MDk3NDE3fQ.jCAai42-SlC8brFQlqifK3V3oy5OR8Jqaq4TmYhIsww",
        "X-Ha-Location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922",
        "X-Homeaway-Site": "vrbo"
    },
    {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.vrbo.com",
        "Origin": "https://www.vrbo.com",
        "Content-Length": "969",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
        "Referer": "",
        "apollographql-client-version": "2.0",
        "x-csrf-jwt-pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3ZTE4YjZlNmU3NGZiYjE3NTEyNDJkNDYyNTFiYzNlZWVlMzg2OTJlMGU5YWM3MGYzMDlmNmQ2OTc2ZGUzZWVlODYwNGIzYmIwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYwYjBhZTUzYmMxNWZlOGY4MCIsImlhdCI6MTY4NTQ5ODMyNCwiZXhwIjoxNjg2MTAzMTI0fQ.L70g4aJwcXHac4XZcKbtFb4kXTEoVbjz2Asz_lYvzuU",
        "x-homeaway-site": "vrbo",
        "apollographql-client-name": "web_pdp",
        "x-ha-location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922"
    },
    {
        "Host": "www.vrbo.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "",
        "Content-Type": "application/json",
        "X-Homeaway-Site": "vrbo",
        "X-Ha-Location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=104",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "X-CSRF-Jwt-Pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3MzQ5YjVlMWIwMWNlZDQyNTE3MzdlMTcyNjFiYzJlZmU3NmU2OTJjMDc5ZDliMGY2NmM5NmE2ZDcwZGIzNWI4ZDI1MWJhZTUwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmY2YjFhZTU2YjgxN2Y4OGY4MCIsImlhdCI6MTY4NTQ3MDg4OSwiZXhwIjoxNjg2MDc1Njg5fQ.tXgyMld0OBHxylZuOOaT9RA0Eoprn05RMnCap3Lvaj0",
        "Content-Length": "969",
        "Origin": "https://www.vrbo.com",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "TE": "trailers"
    },
    {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "Content-Length": "969",
        "Content-Type": "application/json",
        'sec-ch-ua': 'Google Chrome;v=113, Chromium;v=113, Not-A.Brand;v=24',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Referer': ''
    }
]

pid = propertyIds['pid'][0]
listing_id = propertyIds['listing_id'][0]
random_reviews_headers = random.choice([0, 1, 2, 3, 4])
        
#json_data is the call we make to the API. This is what we needed listing_id for.
json_data = {
    "operationName": "Reviews",
    "variables": {
        "isInitial": False,
        "listingId": f"{listing_id}",
        "page": 1,
        "pageSize": 500
    },
    "query": "query Reviews($isInitial: Boolean = false, $listingId: String!, $page: Int!, $pageSize: Int!) {\n  reviews(listingId: $listingId, page: $page, pageSize: $pageSize) {\n    uuid\n    headline: title\n    rating\n    body: text\n    arrivalDate\n    datePublished\n    ownershipTransferred\n    voteCount\n    reviewLanguage\n    reviewer {\n      location\n      nickname\n      profileUrl\n      __typename\n    }\n    response: reviewResponse {\n      status\n      body\n      language\n      country\n      __typename\n    }\n    source\n    unverifiedDisclaimer\n    __typename\n  }\n  reviewSummary(listingId: $listingId) @include(if: $isInitial) {\n    reviewCount\n    guestbookReviewCount\n    averageRating\n    verificationDisclaimerLinkText\n    verificationDisclaimerLinkUrl\n    verificationDisclaimerText\n    __typename\n  }\n}\n"
}

#We wrote reviews_headers without a "Referer" because this depends on pid. So re-write it now.
reviews_headers[random_reviews_headers]["Referer"] = f"https://www.vrbo.com/{pid}"

# Call the API!
response = requests.post(url='https://www.vrbo.com/mobileapi/graphql',
                         headers=reviews_headers[random_reviews_headers],json=json_data)


# SCRAPE REVIEWS
# Create a list of headers we'll rotate between to scrape reviews

propertyIds = pd.read_csv('VRBO/vrbo_properties.csv')

missed_pids = []
reviews_headers = [
    {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.vrbo.com",
        "Origin": "https://www.vrbo.com",
        "Content-Length": "967",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
        "Referer": "",
        "apollographql-client-version": "2.0",
        "x-csrf-jwt-pdp": 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3ZjEzYjVlNWIyMWNlNDE0NTE3NzdlMTAyNjFiYzJlZWU2M2I2OTc5NWM5YzljMGYzNWNlNjIzOTcyZGYzN2JhZDY1M2U0ZTUwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYwYjBhZTUzYmMxNWZlOGY4MCIsImlhdCI6MTY4NTQ5ODE0MSwiZXhwIjoxNjg2MTAyOTQxfQ.hzxOxKnyOpQZu-q3v93t5TuIsKLwngW7tQ3Yb22c2Y0',
        "x-homeaway-site": "vrbo",
        "apollographql-client-name": "web_pdp",
        "x-ha-location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922"
    },
    {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "Content-Length": "969",
        "Content-Type": "application/json",
        "Origin": "https://www.vrbo.com",
        "Referer": "",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"macOS\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Csrf-Jwt-Pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3NjEzZTRlN2U2NDdlOTEyNTEyODJlMTY3NTFiOTBiYmUxMzY2OTI5NTg5OWNlMGYzYzkzNjgzYjcyZGY2NWJkZDQ1NGJhYjMwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYzYjJhYTVjYmExYWZmOGY4MCIsImlhdCI6MTY4NTQ5MjYxNywiZXhwIjoxNjg2MDk3NDE3fQ.jCAai42-SlC8brFQlqifK3V3oy5OR8Jqaq4TmYhIsww",
        "X-Ha-Location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922",
        "X-Homeaway-Site": "vrbo"
    },
    {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.vrbo.com",
        "Origin": "https://www.vrbo.com",
        "Content-Length": "969",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
        "Referer": "",
        "apollographql-client-version": "2.0",
        "x-csrf-jwt-pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3ZTE4YjZlNmU3NGZiYjE3NTEyNDJkNDYyNTFiYzNlZWVlMzg2OTJlMGU5YWM3MGYzMDlmNmQ2OTc2ZGUzZWVlODYwNGIzYmIwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmYwYjBhZTUzYmMxNWZlOGY4MCIsImlhdCI6MTY4NTQ5ODMyNCwiZXhwIjoxNjg2MTAzMTI0fQ.L70g4aJwcXHac4XZcKbtFb4kXTEoVbjz2Asz_lYvzuU",
        "x-homeaway-site": "vrbo",
        "apollographql-client-name": "web_pdp",
        "x-ha-location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=7922"
    },
    {
        "Host": "www.vrbo.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "",
        "Content-Type": "application/json",
        "X-Homeaway-Site": "vrbo",
        "X-Ha-Location": "city=BOULDER, region=CO, country=US, lat=40.0440, lng=-105.194, asn=104",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "X-CSRF-Jwt-Pdp": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbiI6ImI4YzZhZmU2MzFiMWEwYmZkODA4ZWZkZmY3YmFlODY5MDk2ZmMwZDllYzEwYTgwMjcyODlmYWI0ZGY1MzY5MDUxYmM4YmVjNjFlZmFiZDMzYjY3NWY3MTU0NzdhM2U4MjBhYTRiZTFiMzBlMWE1NTMyM2QxNTViZGQ4YzBkNDQyZGE3MzQ5YjVlMWIwMWNlZDQyNTE3MzdlMTcyNjFiYzJlZmU3NmU2OTJjMDc5ZDliMGY2NmM5NmE2ZDcwZGIzNWI4ZDI1MWJhZTUwOGQ2MzllZjI4NWEyZmE3MDVmZDljYzUzZWMwNmJiMTI2NGMxMmY2YjFhZTU2YjgxN2Y4OGY4MCIsImlhdCI6MTY4NTQ3MDg4OSwiZXhwIjoxNjg2MDc1Njg5fQ.tXgyMld0OBHxylZuOOaT9RA0Eoprn05RMnCap3Lvaj0",
        "Content-Length": "969",
        "Origin": "https://www.vrbo.com",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "TE": "trailers"
    },
    {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en-CA;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Apollographql-Client-Name": "web_pdp",
        "Apollographql-Client-Version": "2.0",
        "Content-Length": "969",
        "Content-Type": "application/json",
        'sec-ch-ua': 'Google Chrome;v=113, Chromium;v=113, Not-A.Brand;v=24',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Referer': ''
    }
]

subfolder_path = "/VRBO/Reviews"

# Loop through rows of propertyIds
for i, row in propertyIds.iterrows():
    time.sleep(2)
    pid = row['pid']
    listing_id = row['listing_id']
    
    # Create the file path
    file_path = os.path.join(subfolder_path, f"{pid}.csv")
    
    # Check if the file exists
    if not os.path.isfile(file_path):
        try:
        # Randomly select the headers
            random_reviews_headers = random.choice([0, 1, 2, 3, 4])

            #json_data is the call we make to the API. This is what we needed listing_id for.
            json_data = {
                "operationName": "Reviews",
                "variables": {
                    "isInitial": False,
                    "listingId": f"{listing_id}",
                    "page": 1,
                    "pageSize": 500
                },
                "query": "query Reviews($isInitial: Boolean = false, $listingId: String!, $page: Int!, $pageSize: Int!) {\n  reviews(listingId: $listingId, page: $page, pageSize: $pageSize) {\n    uuid\n    headline: title\n    rating\n    body: text\n    arrivalDate\n    datePublished\n    ownershipTransferred\n    voteCount\n    reviewLanguage\n    reviewer {\n      location\n      nickname\n      profileUrl\n      __typename\n    }\n    response: reviewResponse {\n      status\n      body\n      language\n      country\n      __typename\n    }\n    source\n    unverifiedDisclaimer\n    __typename\n  }\n  reviewSummary(listingId: $listingId) @include(if: $isInitial) {\n    reviewCount\n    guestbookReviewCount\n    averageRating\n    verificationDisclaimerLinkText\n    verificationDisclaimerLinkUrl\n    verificationDisclaimerText\n    __typename\n  }\n}\n"
            }

            #We wrote reviews_headers without a "Referer" because this depends on pid. So re-write it now.
            reviews_headers[random_reviews_headers]["Referer"] = f"https://www.vrbo.com/{pid}"

            # Call the API!
            response = requests.post(url='https://www.vrbo.com/mobileapi/graphql',
                                     headers=reviews_headers[random_reviews_headers],json=json_data)

            # Set rdf (reviews data frame) from the response
            rdf = pd.DataFrame(json.loads(response.text)['data']['reviews'])


            # Apply the conversion function to the 'datetime' column using the 'apply()' method
            rdf['reviewDate'] = rdf['datePublished'].apply(lambda datetime_str: pd.to_datetime(datetime_str).strftime("%B %d, %Y"))
            rdf['stayDate'] = rdf['arrivalDate'].apply(lambda datetime_str: pd.to_datetime(datetime_str).strftime("%B %Y"))
            rdf['pid'] = pid
            
            # T

            rdf.to_csv(f'VRBO/Reviews Short/{pid}.csv', index=False)

            common_df = pd.DataFrame(columns=['pid', 'id', 'title', 'body', 'rating', 'reviewDate', 'stayDate'])

            common_df['pid'] = rdf['pid']
            common_df['id'] = rdf['uuid']
            common_df['title'] = rdf['headline']
            common_df['body'] = rdf['body']
            common_df['rating'] = rdf['rating']
            common_df['reviewDate'] = rdf['reviewDate']
            common_df['stayDate'] = rdf['stayDate']

            # Saving the common DataFrame to a CSV file
            common_df.to_csv(f'VRBO/Reviews/{pid}.csv', index=False)

        except:
            missed_pids.append(pid)
            continue
        pass

