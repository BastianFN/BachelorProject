#!/bin/bash

# Number of iterations
N=20

# Array of JSON data files
csv_files=("cleaned_data300.csv" "cleaned_data600.csv" "cleaned_data900.csv" "cleaned_data1800.csv")

# Array of worker counts
worker_counts=(1 2 4 8)

# Loop through all JSON files
for csv_file in "${csv_files[@]}"; do
    # Loop through all worker configurations
    for workers in "${worker_counts[@]}"; do
        # Set the output file name based on the JSON file and worker count
        csv_size=$(echo "$csv_file" | cut -d'_' -f2 | cut -d'.' -f1)
        timelymon_times_file="csv_${csv_size}_${workers}w.txt"

        # Clear the file or create it if it doesn't exist
        > "$timelymon_times_file"

        # Execute the timelymon command N times
        for i in $(seq 1 $N); do
            # Run the command, redirecting stderr to stdout to capture 'time' output
            output=$( { cat "data/$csv_file" | time target/release/timelymon "edit(user,0) AND ONCE[1,1] edit(user,0)" -w $workers -m 1 2>&1 > /dev/null; } 2>&1 )

            # Extract the total time from the output for timelymon command
            timelymon_time=$(echo "$output" | awk '/real/ {print $1}')

            # Append time to the file
            echo "$timelymon_time" >> "$timelymon_times_file"
        done
    done
done
