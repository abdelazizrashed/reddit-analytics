# Reddit Data Analytics using Hadoop Ecosystem

This project is a combination between Data Science and Big Data Analytics.
The project answers questions like what is the most discussed topic in each subreddit, the association between the rate of controversy and reply rate, as well as the rate of upvotes and downvotes.

## [Dataset](https://drive.google.com/file/d/1-D_uHkn37M5ptWVQl8a5-q8NBv9jaLWr/view "@embed")

## Code description

There are three main scipts that run the tasks as follows:
- task1_ai.py to run task 1 part i of part a
- task1_aii.py to run task 1 part ii of part a
- task1_aiii.py to run task 1 part iii of part a

The project code is divided into two modules.
- mappers module: contains the mappers for all three tasks with each mapper defined as a static method in the Mapper class.
- reducers module contain the reducer for all three tasks with each mapper defined in one or more static methods in Reducer class.
