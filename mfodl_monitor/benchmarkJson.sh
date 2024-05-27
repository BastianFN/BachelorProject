#!/bin/bash

# Number of iterations
N=20

# Array of JSON data files
json_files=("cleaned_data300.json" "cleaned_data600.json" "cleaned_data900.json" "cleaned_data1800.json")

# Array of worker counts
worker_counts=(1 2 4 8)

# Loop through all JSON files
for json_file in "${json_files[@]}"; do
    # Loop through all worker configurations
    for workers in "${worker_counts[@]}"; do
        # Set the output file name based on the JSON file and worker count
        json_size=$(echo "$json_file" | cut -d'_' -f2 | cut -d'.' -f1)
        timelymon_times_file="json_${json_size}_${workers}w.txt"

        # Clear the file or create it if it doesn't exist
        > "$timelymon_times_file"

        # Execute the timelymon command N times
        for i in $(seq 1 $N); do
            # Run the command, redirecting stderr to stdout to capture 'time' output
            output=$( { time (cat "data/$json_file" | target/release/timelymon "<<.user & .type = \"edit\" & .bot = false>> AND ONCE[1,1] <<.user & .type = \"edit\" & .bot = false>>" -w $workers -m 1 -f json) 2>&1 > /dev/null; } 2>&1 )

            # Extract the total time from the output for the entire chain, convert it to seconds, and format it to three decimal places
            timelymon_time=$(echo "$output" | awk '/real/ { split($2, time, /[ms]/); printf "%.3f\n", time[1] * 60 + time[2] }')

            # Append time to the file
            echo "$timelymon_time" >> "$timelymon_times_file"
        done
    done
done
