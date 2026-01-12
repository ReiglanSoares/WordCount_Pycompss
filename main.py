import glob
import argparse
import os
import shutil
from pycompss.api.api import compss_wait_on, compss_barrier
from apps import wordcount_bucketed, reduce_bucket

def tree_reduce_bucket(bucket_files, bucket_id, intermediate_dir):
    tasks = bucket_files.copy()
    n = 1
    L = len(tasks)

    while n < L:
        for r in range(0, L, 2*n):
            if r + n < L:
                # Arquivos em intermediate/
                out_file = os.path.join(intermediate_dir, f"reduce_bucket_{bucket_id}_{r}_{r+n}.txt")
                tasks[r] = reduce_bucket(tasks[r], tasks[r+n], out_file)
        n *= 2

    return tasks[0] 
def run(files, output_file, B):
    # Cria pastas necessárias
    intermediate_dir = os.path.abspath("intermediate")
    outputs_dir = os.path.abspath("outputs")
    os.makedirs(outputs_dir, exist_ok=True)
    os.makedirs(intermediate_dir, exist_ok=True)

    files_abs = [os.path.abspath(f) for f in files]

    print(f"[INFO] Iniciando fase MAP com {len(files_abs)} arquivos...")
    print(f"[INFO] Cada arquivo será processado em 1 task, gerando {B} buckets")

    map_futures = []
    for idx, input_file in enumerate(files_abs):
        future = wordcount_bucketed(input_file, intermediate_dir, B, idx)
        map_futures.append(future)

    print(f"[INFO] {len(map_futures)} tasks de MAP submetidas.")

    # Sincroniza os resultados do MAP
    print(f"[INFO] Aguardando conclusão da fase MAP...")
    map_results = [compss_wait_on(f) for f in map_futures]

    # Organiza os arquivos por bucket
    print(f"[INFO] Organizando arquivos em {B} buckets...")
    bucket_lists = [[] for _ in range(B)]
    for file_list in map_results:
        for bucket_id in range(B):
            bucket_lists[bucket_id].append(file_list[bucket_id])

    print(f"[INFO] Iniciando fase REDUCE com {B} buckets...")
    final_bucket_files = []
    for b in range(B):
        print(f"[INFO] Reduzindo bucket {b}/{B-1} com {len(bucket_lists[b])} arquivos...")
        final_future = tree_reduce_bucket(bucket_lists[b], b, intermediate_dir)
        final_bucket_files.append(final_future)

    print("[INFO] Aguardando finalização de todos os buckets...")
    final_paths = [compss_wait_on(f) for f in final_bucket_files]

    # Combina todos os buckets no arquivo final
    out_final = os.path.join(outputs_dir, output_file)
    print(f"[INFO] Combinando resultados em {out_final}...")
    with open(out_final, "w") as fout:
        for fpath in final_paths:
            with open(fpath, "r") as f:
                for line in f:
                    fout.write(line)

    print("\n[OK] Resultado final salvo em:", out_final)
  
    # print("[INFO] Limpando arquivos intermediários...")
    # shutil.rmtree("intermediate")
    # print("[OK] Intermediários removidos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce WordCount com PyCOMPSs")
    parser.add_argument("-i", "--input", required=True, help="Pasta ou padrão glob para arquivos de entrada")
    parser.add_argument("-o", "--output", required=True, help="Nome do arquivo de saída")
    parser.add_argument("--buckets", type=int, default=48, help="Número de buckets para particionar palavras")

    args = parser.parse_args()

    if os.path.isdir(args.input):
        files = sorted(glob.glob(os.path.join(args.input, "*")))
        files = [f for f in files if os.path.isfile(f)]
    else:
        files = sorted(glob.glob(args.input))

    print(f"[INFO] {len(files)} arquivos detectados.")

    if not files:
        print("[ERRO] Nenhum arquivo encontrado com o padrão fornecido!")
        exit(1)

    run(files, args.output, args.buckets)      
