#!/bin/sh
pkill -f "python"
/Users/andreasditte/opt/anaconda3/envs/hate_on_twitter/bin/python /Users/andreasditte/Desktop/Private_Projekte/hate_on_twitter/src/crawler/crawl_hashtags.py
for i in 0 1 2 3 
do
  echo "Looping ... number $i"
  /Users/andreasditte/opt/anaconda3/envs/hate_on_twitter/bin/python /Users/andreasditte/Desktop/Private_Projekte/hate_on_twitter/src/main.py --nb-hashtag $i&
  sleep 10
done