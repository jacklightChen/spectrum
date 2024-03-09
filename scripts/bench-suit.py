import subprocess


if __name__ == '__main__':
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    for n_threads in range(8, 129, 4):
        for cc in [f"Aria:{n_threads}:128:128:FALSE", f"Sparkle:{n_threads}:8:128", "Spectrum:8:128:16:COPYONWRITE"]: 
            print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
            result = subprocess.run(["./build/bench", cc, "Smallbank:100000:2.0", "2s"], **conf)
            print(result.stderr.decode('utf-8').strip())
