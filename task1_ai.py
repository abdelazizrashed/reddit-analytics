from mappers.mapper import Mapper
from reducers.reducer import Reducer

run_mode = input("How would like to run the code?\n0 for running like a normal python code with the sample data defined in \"sample.txt\" file at the same level as this script.\n1 for running using Hadoop.\nType 0 or 1 for your choice:")
if run_mode != "0" and run_mode != "1":
    raise("wrong answer")
run_mode = run_mode == "1"

# running the mapper
Mapper.run_t1ai(run_mode)

# running the reducer
all_data = Reducer.t1ai_process_data(run_mode)

highest_subreddits = Reducer.t1ai_reduce_to_highest_n(all_data, n=20)

Reducer.t1ai_print_highest_n_discusses(
    highest_subreddits, all_data, n=5)
