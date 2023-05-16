from mappers.mapper import Mapper
from reducers.reducer import Reducer

run_mode = input("How would like to run the code?\n0 for running like a normal python code with the sample data defined in \"sample.txt\" file at the same level as this script.\n1 for running using Hadoop.\nType 0 or 1 for your choice:")
if run_mode != "0" and run_mode != "1":
    raise("wrong answer")
run_mode = run_mode == "1"

Mapper.run_t1aii(run_mode)
# print('finished mapping ')
Reducer.t1aii_run(run_mode)
