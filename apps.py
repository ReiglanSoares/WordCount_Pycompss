from pycompss.api.task import task
from pycompss.api.parameter import FILE_IN
import os
from hashlib import md5

@task(input_file=FILE_IN, returns=list)
def wordcount_bucketed(input_file, out_dir, B, idx):
   
    os.makedirs(out_dir, exist_ok=True)

    bucket_files = [os.path.join(out_dir, f"map_{idx}_bucket_{b}.txt") for b in range(B)]
    outs = [open(bf, "w") for bf in bucket_files]

    with open(input_file, "r") as f:
        for line in f:
            for w in line.split():
                h = int(md5(w.encode()).hexdigest(), 16) % B
                outs[h].write(f"{w} 1\n")

    for o in outs:
        o.close()

    return bucket_files

@task(returns=str)
def reduce_bucket(f1, f2, out_path):
   
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    counts = {}

    with open(f1, "r") as a:
        for line in a:
            parts = line.split()
            if len(parts) == 2:
                k, v = parts
                counts[k] = counts.get(k, 0) + int(v)

    with open(f2, "r") as b:
        for line in b:
            parts = line.split()
            if len(parts) == 2:
                k, v = parts
                counts[k] = counts.get(k, 0) + int(v)

    with open(out_path, "w") as o:
        for k, v in counts.items():
            o.write(f"{k} {v}\n")

    return out_path
