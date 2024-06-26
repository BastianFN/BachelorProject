import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Define the path to the benchmark_data folder
base_path = "benchmark_data"

# Define the folder structure
data_types = ["csv", "json", "pipe"]
steps = ["step1", "step100"]
data_sizes = [300, 600, 900, 1800]
workers = ["1w", "4w"]

# Initialize a dictionary to store the results
results = {data_type: {step: {worker: [] for worker in workers} for step in steps} for data_type in data_types}

# Traverse the directory and read the data
for data_type in data_types:
    for step in steps:
        for worker in workers:
            for data_size in data_sizes:
                file_name = f"{data_type}_data{data_size}_{worker}.txt"
                file_path = os.path.join(base_path, data_type, step, file_name)
                if os.path.isfile(file_path):
                    with open(file_path, 'r') as file:
                        times = [float(line.strip()) for line in file.readlines()]
                        average_time = np.mean(times)
                        results[data_type][step][worker].append((data_size, average_time))

# Function to plot the data
def plot_combined_results(step, worker):
    plt.figure(figsize=(10, 6))
    for data_type in data_types:
        data = results[data_type][step][worker]
        data = sorted(data, key=lambda x: x[0])  # Sort by data_size
        data_sizes, times = zip(*data)
        plt.plot(data_sizes, times, marker='o', label=data_type.upper())
    
    plt.title(f'{step} - {worker}')
    plt.xlabel('Data Size')
    plt.ylabel('Average Processing Time (s)')
    plt.legend()
    plt.grid(True)
    plt.show()

# Plot combined results for each step and worker configuration
for step in steps:
    for worker in workers:
        plot_combined_results(step, worker)
