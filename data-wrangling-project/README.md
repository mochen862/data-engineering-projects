# CONTEXT
- to wrangle WeRateDogs Twitter data to create insights and visualisations
- the Twitter data is good, but contains only basic tweet information
- additional gathering, assessing and cleaning is required to create meaningful insights & visualisations

# THE DATA

#### 1. Enhanced Twitter Archive
The WeRateDogs Twitter archive contains basic tweet data for all of their tweets, but not everything. I have filtered for tweets with ratings only (there are 2356). Ratings were extracted from the each tweet's text, but they're probably not all correct.

#### 2. Additional data via the Twitter API (`tweepy`)
I gathered data on retweet count and favourite count using Twitter's API (tweepy)for each of the tweet ids in the enhanced twitter archive.

#### 3. Image predictions file
Contains a table full of image predictions (top 3 only) alongside each tweet ID, image URL, and the image number that corresponded to the most confident prediction (numbered 1 to 4 since tweets can have up to four images)

# DATA WRANGLING

#### Gathering
- I directly downloaded the twitter-archive-enhanced.csv data and loaded it as a dataframe into the archive table
- I used `requests.get()` to download and write out the `image-predictons.tsv` file. I then loaded this `image_predictions.tsv` file to a pandas dataframe
- I used tweepy to to download the retweet counts and favourite counts, saved the file as `tweets_json.txt` and then loaded this text file to the pandas dataframe api_data table

#### Cleaning
- I cleaned 7 quality issues and 3 tidiness issues detailed in the `dog_tweets_project` notebook

#### Insights and Visualisations
- I used `seaborn` and `matplotlib` to create data visualisations and meaningful insights
