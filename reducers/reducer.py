import os
import sys
from names import *


class Reducer:
    @staticmethod
    def t1ai_process_data(is_hadoop: bool) -> list:
        """
        This method is used to procoess the data and returns [list_of_data].\n
        
        \t[input_file] is the name of the data to be procoessed. Will throw error if doesn't exists.\n
        \t[output_file] is the name of the output file.\n
        \t[override_existing] if set to True will override the output file if it exists. If False will return None.\n
        
        \tThe output in the form of [list_of_data, all_sub_reddits]\n
        \t[list_of_data] is a list coontaining data like the coount, the number of links an the reddit id\n
        """
        list_of_data = []       ##this list of all reddit with cooreesponding links,coounts
        all_sub_reddits = {}    ##this dicot to gather all the subreddits
        
        #I know it's not a good solution but I don't have to risk errors :)
        if is_hadoop: 
            for ind in sys.stdin:
                stripped_line = line.strip()
                values_arr = stripped_line.split("\t")
                reddit = {"subreddit": "", "c": 0, "all_links": {}}
                if len(values_arr) < 3:
                    break
                sub, co, topic_link = values_arr[0], values_arr[1], values_arr[2]
                co = int(co)
                if sub  not in all_sub_reddits:
                    all_sub_reddits[sub] = ind
                    ind =ind+1
                    reddit["subreddit"] = sub
                    reddit["c"] = 1
                    reddit["all_links"][topic_link] = 1
                    list_of_data.append(reddit)
                else:
                    value = all_sub_reddits[sub]
                    if value >= len(list_of_data):
                        continue
                    subre=list_of_data[value]
                    subre["c"] += co
                    if topic_link not in subre["all_links"]:
                        (subre["all_links"])[topic_link] = 1
                        
                    else:
                        (subre["all_links"])[topic_link] += 1
        else:           
            if not os.path.isfile(tai_mapper):
                raise ("Input file doesn't exist")
            with open(tai_mapper, "r") as data:
                lines = data.readlines()
            for ind in range(len(lines)):
                line = lines[ind]
                stripped_line = line.strip()
                values_arr = stripped_line.split("\t")
                reddit = {"subreddit": "", "c": 0, "all_links": {}}
                if len(values_arr) < 3:
                    break
                sub, co, topic_link = values_arr[0], values_arr[1], values_arr[2]
                co = int(co)
                if sub  not in all_sub_reddits:
                    all_sub_reddits[sub] = ind
                    ind =ind+1
                    reddit["subreddit"] = sub
                    reddit["c"] = 1
                    reddit["all_links"][topic_link] = 1
                    list_of_data.append(reddit)
                else:
                    value = all_sub_reddits[sub]
                    if value >= len(list_of_data):
                        continue
                    subre=list_of_data[value]
                    subre["c"] += co
                    if topic_link not in subre["all_links"]:
                        (subre["all_links"])[topic_link] = 1
                        
                    else:
                        (subre["all_links"])[topic_link] += 1
        return list_of_data

    @staticmethod
    def t1ai_reduce_to_highest_n(list_of_data: list, n: int):
        """
        This method is used to coompute the highest n subreddits based on the coount of posts.\n
        
        \t[list_of_data] is a list coontaining data like the coount, the number of links an the reddit id\n
        \t[n] the number of highest subreddits to get\n
        
        \t[highest_subriddits] is a dicot. The key simply the ind and the value is a dicot coontaining the coount and the ind in [list_of_data]
        in the form {"coount":<int>, "ind":<int>}
        """
        highest_subreddits = dict(
            enumerate({"c": 0, "ind": 0} for _ in range(n)))
        for i in range(len(list_of_data)):
            data = list_of_data[i]
            for j in highest_subreddits.keys():
                t= highest_subreddits[j]
                if data["c"] > t["c"]:
                    highest_subreddits[j] = {
                        "c": data["c"], "ind": i}
                    break
        return highest_subreddits

    @staticmethod
    def t1ai_print_highest_n_discusses(highest_subreddits: dict, list_of_data: list, n: int):
        if n > len(highest_subreddits.keys()):
            raise (
                "n must not be greater than the number of values in [highest_subreddits]")
        for i in range(n):
            ind = highest_subreddits[i]["ind"]
            if ind != 0:
                topics_links = list_of_data[ind]["all_links"]
                sorted_links = dict(sorted(topics_links.items(), key=lambda x: x[1]))
                sorted_links=list(sorted_links)
                highest=sorted_links[len( sorted_links)-5:len(sorted_links)-1]
                c=list_of_data[ind]["c"]
                max_subreddit=list_of_data[ind]["subreddit"]

                print(f"{max_subreddit}\t{c}\t{highest}") 


    @staticmethod
    def t1aii_run(is_hadoop: bool):
        frequency = {}
        scores = {}
        list_links = {}


        if is_hadoop:
            for line in sys.stdin:
                line = line.strip()
                line = line.split("\t")
                if len(line) > 4:
                    continue
                parent_id, score, link_id, c = line[0], line[1], line[2], line[3]       
                c = int(c)
                if link_id == parent_id:
                    if link_id in frequency:
                        pass
                    else:
                        frequency[link_id] = 0
                    if link_id in list_links:
                        pass
                    else:
                        list_links[link_id] = 1
                    if link_id in scores:
                        pass
                    else:
                        scores[link_id] = score
                    
                else:
                    if not(link_id in frequency):
                        frequency[link_id] = 1
                    else:
                        frequency[link_id] += c
                
                
            for key in list_links.keys():
                print(f"{key}\t{scores[key]}\t{frequency[key]}")

        else:
            with open(taii_mapper, "r") as data:
                lines = data.readlines()
            for line in lines:
                line = line.strip()
                line = line.split("\t")
                if len(line) > 4:
                    continue
                parent_id, score, link_id, c = line[0], line[1], line[2], line[3]       
                c = int(c)
                if link_id == parent_id:
                    if link_id in frequency:
                        pass
                    else:
                        frequency[link_id] = 0
                    if link_id in list_links:
                        pass
                    else:
                        list_links[link_id] = 1
                    if link_id in scores:
                        pass
                    else:
                        scores[link_id] = score
                    
                else:
                    if not(link_id in frequency):
                        frequency[link_id] = 1
                    else:
                        frequency[link_id] += c
                
                
            for key in list_links.keys():
                print(f"{key}\t{scores[key]}\t{frequency[key]}")

    @staticmethod
    def t1aiii_run(is_hadoop: bool):
        ups_downs = []
        links= {}
        ind = 0
        if is_hadoop:
            for line in sys.stdin:
                ups_down_count = {"link_id": "", "ups": 0, "downs": 0}
                line = line.strip()
                line = line.split("\t")
                link_id, downs, ups = line[0], line[1], line[2]
                ups = int(ups)
                downs = int(downs)
                if not(link_id in links):
                    links[link_id] = ind
                    ind += 1
                    ups_down_count["link_id"] = link_id
                    ups_down_count["downs"] += downs
                    ups_down_count["ups"] += ups
                    ups_downs.append(ups_down_count)
                else:
                    index = links[link_id]
                    ups_downs[index]["ups"] += ups
                    ups_downs[index]["downs"] += downs
        else:
            with open(taiii_mapper) as w:
                lines = w.readlines()
            for line in lines:
                ups_down_count = {"link_id": "", "ups": 0, "downs": 0}
                line = line.strip()
                line = line.split("\t")
                link_id, downs, ups = line[0], line[1], line[2]
                ups = int(ups)
                downs = int(downs)
                if not(link_id in links):
                    links[link_id] = ind
                    ind += 1
                    ups_down_count["link_id"] = link_id
                    ups_down_count["downs"] += downs
                    ups_down_count["ups"] += ups
                    ups_downs.append(ups_down_count)
                else: 
                    index = links[link_id]
                    ups_downs[index]["ups"] += ups
                    ups_downs[index]["downs"] += downs
        max = 0
        min = 0
        maximum=""
        minmum=""
        for i in range(len(ups_downs)):
            data = ups_downs[i]
            if data["ups"] > max :
               max=data["ups"]
               maximum=data["link_id"]
            if data["downs"] < min :
                min[data["downs"]]
                minmum=data["link_id"]
        print(f"{minmum}\t{min}")
        print(f"{maximum }\t{[max]}")
            
