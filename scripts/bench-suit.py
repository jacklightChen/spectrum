import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'threads', 'zipf', 'partition', 'commit', 'abort'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    for n_threads in range(6, 36, 6):
        # n_partitions    = n_threads * 8
        n_partitions    = 1
        n_dispatchers   = 2
        protocols       = [
            # f"Aria:{n_threads}:{n_partitions}:128:FALSE", 
            # f"AriaReordering:{n_threads}:{n_partitions}:128:TRUE",
            f"Sparkle:{n_threads}:{n_dispatchers}:{n_partitions}", 
            # f"Spectrum:{n_threads}:{n_partitions}:COPYONWRITE"
        ]
        for cc in protocols:
            print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
            result = subprocess.run(["../build/bench", cc, "Smallbank:100000:0", "2s"], **conf)
            result_str = result.stderr.decode('utf-8').strip()
            print(result_str)
            commit = float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
            execution = float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
            df.loc[len(df)] = {
                'protocol': cc.split(':')[0], 
                'threads': n_threads, 
                'zipf': 0, 
                'partition': n_partitions, 
                'commit': commit,
                'abort': execution - commit
            }
    df.to_csv('bench_results.csv')

    df = pd.read_csv('bench_results.csv')
    fig ,ax = plt.subplots()
    for protocol in df['protocol'].unique():
        df_protocol = df[df['protocol'] == protocol]
        ax.plot(df_protocol['threads'], df_protocol['commit'], label=protocol)
    
    ax.set_xlabel('Threads')
    ax.set_ylabel('Throughput (txn/s)')
    ax.legend()
    plt.savefig('bench_results.png')

            
