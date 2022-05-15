from functools import wraps
import numpy as np
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
from atomicwrites import atomic_write


# 1.	Implement a decorator for atomic writing
def deco(logfile):
    def log_deco(func):
        @wraps(func)
        def wrap_func(*args, **kw):
            # 3.	Implement a demo of your decorator on a parquet file.
            res = func(*args, **kw)
            if logfile == "parquet":
                table = pa.Table.from_pandas(res, preserve_index=False)
                pq.write_table(table, logfile)
            else:
                try:
                    with atomic_write(logfile, overwrite=True) as log_obj:
                        log_obj.write("doing "+func.__name__+', include' + str(res) +'\n')
                except EnvironmentError as e:
                    print("error writing file at {}: errno={}".format(logfile, e.errno))
            # return func(*args, **kw)
        return wrap_func
    return log_deco


# 2.	Implement unit tests for your code.
@deco("logfile")
def my_add(x, y):
    return x + y

@deco("parquet")
def my_dec(x, y):
    df = pd.DataFrame(data={'doing': x, 'include': y},index=[0])
    return df

print(my_add(1, 1))
print(my_dec(1, 1))


