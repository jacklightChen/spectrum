import subprocess


if __name__ == '__main__':
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    for n_threads in range(8, 129, 4):
        n_partitions    = n_threads * 8
        protocols       = [
            f"Aria:{n_threads}:{n_partitions}:128:FALSE", 
            f"Sparkle:{n_threads}:{n_partitions}", 
            f"Spectrum:{n_threads}:{n_partitions}:{n_threads + 16}:COPYONWRITE"
        ]
        for cc in protocols:
            print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
            result = subprocess.run(["../build/bench", cc, "Smallbank:100000:2.0", "2s"], **conf)
            print(result.stderr.decode('utf-8').strip())
