import json
import os
import sys

from names import *


class Mapper:
    @staticmethod
    def run_t1ai(is_hadoop: bool):
        """
        This function is used to map the data from [input_file] and store it in [output_file]\n
        #inputs\n
        \t[input_file] is the name of the data to be processed. Will throw error if doesn't exists.\n
        \t[output_file] is the name of the output file.\n
        \t[override_existing] if set to True will override the output file if it exists. If False will return None.\n
        \t[verbose] if set to True will print the data as they are mapped.
        #output\n
        \tNone
        """
        def loop_iteration(line)->str:
                line = json.loads(line)

                subred = line["subreddit_id"]
                link_id = line["link_id"]
                return f"{subred}\t1\t{link_id}"

        if is_hadoop:
            for line in sys.stdin:
                print(loop_iteration(line))
        else:
            if not os.path.isfile(sample):
                raise ("Input file doesn't exist")

            with open(sample, "r") as data:
                lines = data.readlines()

            with open(tai_mapper, 'w') as output:
                for line in lines:
                    output.write(loop_iteration(line))
                    output.write("\n")



    @staticmethod
    def run_t1aii(is_hadoop: bool):
        def loop_iter(line)->str:
            line = json.loads(line)
            parent_id = line["parent_id"]
            link_id = line["link_id"]
            score = line["score"]
            return f"{parent_id}\t{score}\t{link_id}\t1"

        if is_hadoop:
            for line in sys.stdin:
                print(loop_iter(line))
        else:
            if not os.path.isfile(sample):
                raise ("Input file doesn't exist")

            with open(sample, "r") as data:
                lines = data.readlines()

            with open(taii_mapper, 'w') as output:
                for line in lines:
                    output.write(loop_iter(line))
                    output.write("\n")

    @staticmethod
    def run_t1aiii(is_hadoop: bool):
        def loop_iter(line)->str:
            line = json.loads(line)
            ups = line["ups"]
            downs = line["downs"]
            link_id = line["link_id"]
            return f"{link_id}\t{downs}\t{ups}"
        if is_hadoop:
            for line in sys.stdin:
                print(loop_iter(line))
        else:
            if not os.path.isfile(sample):
                raise ("Input file doesn't exist")

            with open(sample, "r") as data:
                lines = data.readlines()

            with open(taiii_mapper, 'w') as output:
                for line in lines:
                    output.write(loop_iter(line))
                    output.write("\n")
