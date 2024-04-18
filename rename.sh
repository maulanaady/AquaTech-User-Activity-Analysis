#!/bin/bash

current_year=$1 #2024
current_month=$2
next_month=$3

unzip ./raw_data.zip -d ./airflow/data/
# Loop through all folders in the root path
directories=$(find ./airflow/data/raw_data -maxdepth 1 -type d)

for dir in $directories; do
    # Extract the last part of the directory path (the directory name)
    folder=$(basename "$dir")

    # Extract year, month, and day from folder name
    year=$(echo "$folder" | cut -d'-' -f1)
    month=$(echo "$folder" | cut -d'-' -f2)
    day=$(echo "$folder" | cut -d'-' -f3)

    # Rename folder if current datetime is not April or May of 2024
    if [ "$year" = "2024" ] && ([ "$month" = "04" ] || [ "$month" = "05" ]); then
        if [ "$month" = "04" ]; then
            new_month=$current_month
        else
            new_month=$next_month
        fi
        new_folder="$current_year-$new_month-$day"
        mv "./airflow/data/raw_data/$folder" "./airflow/data/raw_data/$new_folder"
        echo "Renamed $folder to $new_folder"

        # Loop through files in the renamed folder
        files=$(find "./airflow/data/raw_data/$new_folder" -maxdepth 1 -type f)
        for file in $files; do
            # Extract date and time from file name
            filename=$(basename "$file")
            date=$(echo "$filename" | cut -d'-' -f1)
            time=$(echo "$filename" | cut -d'-' -f2)

            # Extract the month and day parts
            file_month="${date:4:2}"
            file_day="${date:6:2}"
            
            # Determine the new month for the file
            if [ "$file_month" = "04" ]; then
                new_file_month=$current_month
            else
                new_file_month=$next_month
            fi

            # Construct the new file name
            new_file="$current_year$new_file_month$file_day-$time"
            mv "./airflow/data/raw_data/$new_folder/$filename" "./airflow/data/raw_data/$new_folder/$new_file"
        done
    fi
done
