import time
import ray
import os
from ray.data._internal.execution.util import memory_string
import random

DATASET_URI = "s3://ray-benchmark-data/parquet/128MiB-file/1TiB/"

def benchmark(ds_factory):
    start_time = time.time()

    ds = ds_factory()

    nbytes = 0 
    time_to_first_bundle = None
    for bundle in ds.iter_internal_ref_bundles():
        nbytes += bundle.size_bytes()
        if time_to_first_bundle is None:
            time_to_first_bundle = time.time() - start_time

    print(f"Read {memory_string(nbytes)} in {time.time() - start_time:.02f}s")
    print(f"First bundle in {time_to_first_bundle:.02f}s")


def ds_operation():
    ds = ray.data.read_parquet(DATASET_URI)
    random.seed(time.time()) 
    filter_val = random.randint(2**62 - 1, 2**62 + 1000_000)
    ds = ds.filter(expr=f"column0 > {filter_val}")
    return ds
    
# S3 scales to high request rates. To ensure that the second method called doesn't have 
# a performance advantage, I'm doing a "warmup".
ds = ds_operation()
count = ds.count()
print(f"Warmup returned {count}")

# # Benchmark runtime
benchmark(ds_operation)
