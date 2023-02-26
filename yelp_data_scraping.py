# -*- coding: utf-8 -*-
"""Yelp Data Scraping

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1lDGzMJtNjL84tCfEf1Rra3C4hC1mCD7H
"""


from yelpapi import YelpAPI
import json
import pandas as pd

api_key = 'okoCFHwA-iVej6G1Ym3pQRr11Gz1C9xHFhY35Ro07ke24UzSBg2d_v-cNEAioKmIJHMBFX2I56HD0GvP4BowfT5CglSKDK7cyepN9mx0tGlS6pJGEq9x91T-mtznY3Yx'

print(api_key)

with YelpAPI(api_key) as yelp_api:
  cuisines = ["French", "Greek", "Indian", "Chinese", "Italian", "Japanese"]
  restaurants = []
  for cuisine in cuisines:
    for iter in range(20):
      response = yelp_api.search_query(term=cuisine, location='Manhattan', sort_by='rating', limit=50, offset=iter*50)
      response = response['businesses']
      for i in range(len(response)):
        name = response[i]['name']
        id = response[i]['id']
        address = response[i]['location']['display_address']
        coords = response[i]['coordinates']
        coords = [coords['latitude'], coords['longitude']]
        num_reviews = response[i]['review_count']
        rating = response[i]['rating']
        zip_code = response[i]['location']['zip_code']
        restaurants.append([name, id, address, coords, num_reviews, rating, zip_code, cuisine])
    print(len(restaurants))
  print(restaurants)

df_dynamo = pd.DataFrame([r[:-1] for r in restaurants], columns=['Name', 'Id', 'Address', 'Coordinates', 'Reviews', 'Rating', 'Zip Code'])
df_elastic = pd.DataFrame([[r[1], r[-1]] for r in restaurants], columns=['Id', 'Cuisine'])
df_dynamo.to_csv("hw1_restaurants_list_dynamo_db.csv")
df_elastic.to_csv("hw1_restaurants_list_elastic_search.csv")

df = pd.read_csv('hw1_restaurants_list_elastic_search.csv', index_col=0)

df = df.rename(columns={'Id': 'BusinessID'})
df = df.astype(str)
df = df.drop_duplicates(subset = 'BusinessID')
restaurants = df.to_dict('records')

index_list = []
for idx,restaurant in enumerate(restaurants):
  index_line = {"index": {"_index": "restaurants", "_id": idx+1}}
  restaurant_line = {"RestaurantID": restaurant["BusinessID"], "Cuisine": restaurant["Cuisine"]}
  index_list.append(index_line)
  index_list.append(restaurant_line)

print(index_list)
print(len(index_list))

with open('data.json', 'a') as f:
  f.seek(0)
  for line in index_list:
    json.dump(line, f)
    f.write('\n')