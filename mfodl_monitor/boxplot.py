import numpy as np
import matplotlib.pyplot as plt
import os

sizes = [300, 600, 900, 1800]
worker_counts = [1, 2, 4, 8]
iterations = 20

data_dir = './benchmark_data/pipe/step1000/'

plot_data = {size: [] for size in sizes}

for size in sizes:
    for workers in worker_counts:
        # filename = f"json_data{size}_{workers}w.txt"
        # filename = f"csv_data{size}_{workers}w.txt"
        filename = f"pipe_data{size}_{workers}w.txt"
        filepath = os.path.join(data_dir, filename)
        
        try:
            with open(filepath, 'r') as file:
                times = np.array([float(line.strip()) for line in file])
                
                mean = np.mean(times)
                std = np.std(times)
                median = np.median(times)
                minimum = np.min(times)
                maximum = np.max(times)
                percentiles = np.percentile(times, [25, 50, 75])
                
                print(f"Statistics for {filename}:")
                print(f"Mean: {mean:.3f}, Std: {std:.3f}, Median: {median:.3f}, Min: {minimum:.3f}, Max: {maximum:.3f}")
                print(f"25th percentile: {percentiles[0]:.3f}, 50th percentile: {percentiles[1]:.3f}, 75th percentile: {percentiles[2]:.3f}\n")
                
                plot_data[size].append(times)
        
        except FileNotFoundError:
            print(f"File {filename} not found.")

fig, axs = plt.subplots(1, len(sizes), figsize=(12, 6), sharey=True)
fig.suptitle('Box Plot of Execution Times by Raw file Size and Worker Count')

for i, size in enumerate(sizes):
    axs[i].boxplot(plot_data[size], labels=[f"{wc}w" for wc in worker_counts])
    axs[i].set_title(f"Raw file size: {size}")
    axs[i].set_xlabel('Number of Workers')
    axs[i].set_ylabel('Time (seconds)')

plt.tight_layout()
plt.show()
