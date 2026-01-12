import os
import random
import string

def generate_block(block_size=1024 * 1024):
    words = []
    size = 0
    while size < block_size:
        w = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 12)))
        words.append(w)
        size += len(w) + 1
    return " ".join(words)


def generate_file_1GB(path):
    target_size = 1024 * 1024 * 1024  # 1 GB

    written = 0
    if os.path.exists(path):
        written = os.path.getsize(path)
        print(f"[INFO] Continuando arquivo existente: {path} (já tem {written / (1024*1024):.2f} MB)")

    mode = "a" if written > 0 else "w"

    with open(path, mode) as f:
        while written < target_size:
            block = generate_block()
            f.write(block + "\n")
            written += len(block) + 1

    print(f"[OK] Arquivo finalizado: {path} (~1GB)")

def generate_48GB_dataset(out_dir="inputs48GB"):

    os.makedirs(out_dir, exist_ok=True)

    for i in range(48):  
        path = os.path.join(out_dir, f"file_{i}.txt")

        if os.path.exists(path) and os.path.getsize(path) >= 1024 * 1024 * 1024:
            print(f"[SKIP] {path} já completo (~1GB)")
            continue

        generate_file_1GB(path)

    print(f"\n[OK] Dataset completo de 48GB em: {out_dir}")

if __name__ == "__main__":
    generate_48GB_dataset()
