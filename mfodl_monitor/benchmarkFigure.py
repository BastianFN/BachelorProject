import os
import numpy as np
import matplotlib.pyplot as plt

def read_results(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return np.array([float(line.strip()) for line in lines])

def plot_results(data, labels, title):
    fig, ax = plt.subplots()
    ax.boxplot(data, labels=labels)
    ax.set_title(title)
    ax.set_ylabel('Time (seconds)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


base_path = 'benchmark_data'
datasets = ['csv', 'json', 'pipe']
step_sizes = ['step1', 'step100']
workers = [1, 4]
sizes = [1800]

data = []
labels = []

for dataset in datasets:
    for step_size in step_sizes:
        for worker in workers:
            file_name = f'{dataset}_data{sizes[0]}_{worker}w.txt'
            file_path = os.path.join(base_path, dataset, step_size, file_name)
            if os.path.exists(file_path):
                results = read_results(file_path)
                data.append(results)
                labels.append(f'{dataset} {step_size} {worker}w')

plot_results(data, labels, 'Benchmark Results: Largest Dataset with Various Configurations')
