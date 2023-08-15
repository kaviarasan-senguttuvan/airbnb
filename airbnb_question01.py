
pip install pymongo

import pymongo
import pandas as pd

client=pymongo.MongoClient("mongodb+srv://kavi:kaviarasan@kaviarasan.obhf2rg.mongodb.net/")
database = client["sample_airbnb"]
coll = database["listingsAndReviews"]

task = []
for i in coll.find({"$and":[{"property_type":{"$in":["Boat","House","Farm stay"]}},{"address.country":"United States"},{"accommodates":{"$gte":6,"$lte":8}},{"review_scores.review_scores_rating":{"$gt":75}},{"review_scores.review_scores_cleanliness":{"$gte":8}}]},{"_id":0,"name":1,"house_rules":1,"property_type":1,"last_review":1,"price":1,"image.picture_url":1,"address":1,"review_scores.review_scores_rating":1,"review_scores.review_scores_cleanliness":1}):
  task.append(i)
df = pd.DataFrame(task)
df[["total_rating","cleanliness_rating"]]=df["review_scores"].apply(lambda i:pd.Series([i["review_scores_rating"],i["review_scores_cleanliness"]]))
df[["country_code","country","market","government_area","suburb","street"]]=df["address"].apply(lambda i:pd.Series([i["country_code"],i["country"],i["market"],i["government_area"],i["suburb"],i["street"]]))
df.drop("review_scores",axis=1,inplace=True)
df.drop("address",axis=1,inplace=True)
df.to_csv("question1.csv")
