
#Dataset
Ths [dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?datasetId=411512&sortBy=voteCount&select=2019-Nov.csv)
 contains the following fields:

1. event_time (Timestamp) - Timestamp when the event occurred.
2. event_type (String) - Type of event performed by the user (e.g., "click", "purchase", "view").
3. product_id (String) - Unique identifier of the product.
4. category_id (String) - Unique identifier of the category.
5. category_code (String) - Code representing the category of the product (e.g., "electronics.smartphone").
6. brand (String) - Brand of the product.
7. price (Double) - Price of the product.
8. user_id (String) - Unique identifier of the user.
9. user_session (String) - Unique identifier of the user session.

#Exercises:

## Exercise 1 (Beginner):
Write a Flink program to read the streaming dataset from a source (e.g., CSV file) and print the user events to the console.

## Exercise 2 (Beginner):
Enhance the previous program to filter out and print only "purchase" events to the console.

## Exercise 3 (Intermediate):
Modify the program to calculate and print the total number of "click" events per user within a 5-minute tumbling window.

## Exercise 4 (Intermediate):
Extend the previous program to find and print the top 3 products with the most "view" events within a 1-hour sliding window.

## Exercise 5 (Advanced):
Build a Flink program to detect fraudulent activities. Implement a function to flag users who perform more than five "purchase" events within a 1-hour session window as potential frauds.

## Exercise 6 (Advanced):
Implement a time-aware join to combine the streaming e-commerce dataset with a static dataset of product categories based on the product_id. The program should enrich the stream with category information and print the result.

## Exercise 7 (Advanced):
Utilize Flink's stateful processing to maintain a user's activity history and detect users who show a sudden increase in "purchase" events compared to their historical behavior.

Feel free to experiment and add more complexity to the exercises as you progress with Apache Flink. Happy learning!