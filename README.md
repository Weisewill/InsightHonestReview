# Insight project: Honest Review 

## Motivation
Sometimes we have short weekends or holidays not enough for trips to facinating places such as Iceland or Peru. At this situation, we might want to stay home playing video games that people has been talking about or go hang out with friends or family enjoying party video games together. However, we are not sure if the game is too hard to appreciate or if everyone in our group loves the game. So, before buying the game we had better check user reviews or comments from internet to see if it match our interests. However, there are too many sources to check, eg. Amazon, Reddit, and Twitter, etc..., and we do not want to spend a whole day just for looking through them. Now, if there is a platform which collects all the reviews/comments and show positive and negative ones to give a balanced picture on the product, then it could save a lot of our precious time surfing the internet cluelessly.

## Objectives
  - Build a web platform collecting online reviews for video game products
  - Perform sentiment analysis to identify positive and negative reviews
  - Obtain statistics such as how many reviews and what percent are positive

## Future directions
  - Collect more data from other sources, eg. Twitter
  - Customize NLP pipeline and re-train ML model
  - Add user inputs such as upvote on reviews
  - Could apply to other kinds of products, eg. movie, apparel, PC, etc...
  
## Business cases
  - Provide a platform attracting users who are in needs to visit
  - Display Ads on relavent or competing games
  - Investigate users' opinions on product reviews and send feedback to companies who seek consult on their products
  
## Datasets
  - Amazon reviews (S3 public bucket)
  - Reddit (https://files.pushshift.io/reddit/comments/)

## Tech stack
![](/fig/HonestReview_tech_stack.jpg)

## Challenges
  - Clean large amount of data, over 3 TB after decompression
  - Process text data to extrct useful informations, eg. finding the name of the product and count # of words in a review
  - Perform sentiment analysis using natural language processing pipeline
  
## Sentiment Analysis
  ### Spark-NLP (https://nlp.johnsnowlabs.com/)
  - NLP: Sentiment analysis 
  - Sentence Embeddings
  - Tokenizer
  - Spell Checker
  - Vivikn Sentiment Analyzer
  ![](/fig/Vivekn_Sentiment_analysis.jpg)

## Presentation
http://bit.ly/honest-review-slides
