import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv('bench_results.csv')
print(df)
fig ,ax = plt.subplots()
for protocol in df['protocol'].unique():
    df_protocol = df[df['protocol'] == protocol]
    print(df_protocol)
    ax.plot(df_protocol['threads'], df_protocol['commit'], label=protocol)

ax.set_xlabel('Threads')
ax.set_ylabel('Throughput (txn/s)')
ax.legend()
plt.savefig('bench_results.png')