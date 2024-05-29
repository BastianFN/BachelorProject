#!/bin/bash

# Number of iterations
N=20

# Array of JSON data files
csv_files=("cleaned_data300.csv" "cleaned_data600.csv" "cleaned_data900.csv" "cleaned_data1800.csv")

# Array of worker counts
worker_counts=(1 2 4 8)

# Array of step sizes
step_sizes=(1 10 100 1000)

# Loop through all JSON files
for csv_file in "${csv_files[@]}"; do
    # Loop through all worker configurations
    for workers in "${worker_counts[@]}"; do
        # Loop through all step sizes
        for step_size in "${step_sizes[@]}"; do
            # Set the output file name based on the JSON file, worker count, and step size
            csv_size=$(echo "$csv_file" | cut -d'_' -f2 | cut -d'.' -f1)
            timelymon_times_file="csv_${csv_size}_${workers}w_s${step_size}.txt"

            # Clear the file or create it if it doesn't exist
            > "$timelymon_times_file"

            # Execute the timelymon command N times
            for i in $(seq 1 $N); do
                # Run the command, redirecting stderr to stdout to capture 'time' output
                output=$( { time (cat "data/$csv_file" | target/release/timelymon "edit(user,0) AND ONCE[1,1] edit(user,0)" -w $workers -m 1 -s $step_size) 2>&1 > /dev/null; } 2>&1 )

                # Extract the total time from the output for the entire chain, convert it to seconds, and format it to three decimal places
                timelymon_time=$(echo "$output" | awk '/real/ { split($2, time, /[ms]/); printf "%.3f\n", time[1] * 60 + time[2] }')

                # Append time to the file
                echo "$timelymon_time" >> "$timelymon_times_file"
            done
        done
    done
done
