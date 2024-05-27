import numpy as np
import matplotlib.pyplot as plt
import os

# Define the file structure
json_sizes = [300, 600, 900, 1800]
worker_counts = [1, 2, 4, 8]
iterations = 20

# Directory containing the data files
data_dir = './first_benchmark_results/json/step1000/'

# Prepare to collect data for plotting
plot_data = {size: [] for size in json_sizes}

# Process each file
for size in json_sizes:
    for workers in worker_counts:
        # filename = f"json_data{size}_{workers}w.txt"
        # filename = f"csv_data{size}_{workers}w.txt"
        filename = f"json_data{size}_{workers}w.txt"
        filepath = os.path.join(data_dir, filename)
        
        try:
            with open(filepath, 'r') as file:
                times = np.array([float(line.strip()) for line in file])
                
                # Calculate statistics
                mean = np.mean(times)
                std = np.std(times)
                median = np.median(times)
                minimum = np.min(times)
                maximum = np.max(times)
                percentiles = np.percentile(times, [25, 50, 75])
                
                # Print statistics
                print(f"Statistics for {filename}:")
                print(f"Mean: {mean:.3f}, Std: {std:.3f}, Median: {median:.3f}, Min: {minimum:.3f}, Max: {maximum:.3f}")
                print(f"25th percentile: {percentiles[0]:.3f}, 50th percentile: {percentiles[1]:.3f}, 75th percentile: {percentiles[2]:.3f}\n")
                
                # Append data for box plot
                plot_data[size].append(times)
        
        except FileNotFoundError:
            print(f"File {filename} not found.")

# Plotting
fig, axs = plt.subplots(1, len(json_sizes), figsize=(12, 6), sharey=True)
fig.suptitle('Box Plot of Execution Times by JSON Size and Worker Count')

for i, size in enumerate(json_sizes):
    axs[i].boxplot(plot_data[size], labels=[f"{wc}w" for wc in worker_counts])
    axs[i].set_title(f"JSON size: {size}")
    axs[i].set_xlabel('Number of Workers')
    axs[i].set_ylabel('Time (seconds)')

plt.tight_layout()
plt.show()
