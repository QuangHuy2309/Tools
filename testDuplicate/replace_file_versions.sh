#!/bin/bash

# Read the input directory path from user input
echo "Enter the input directory path:"
read -r input_directory

# Read the output directory path from user input
echo "Enter the output directory path:"
read -r output_directory

# Read the old version text from user input
echo "Enter the old version text:"
read -r old_version

# Read the new version text from user input
echo "Enter the new version text:"
read -r new_version

# Create the output directory if it doesn't exist
mkdir -p "$output_directory"

# Loop through each file in the input directory
for file in "$input_directory"/*; do
    if [[ -f "$file" ]]; then
        base_filename=$(basename "$file")
        updated_filename=$(echo "$base_filename" | sed "s/$old_version/$new_version/I; s/${old_version^^}/${new_version^^}/g")
        updated_filepath="$output_directory/$updated_filename"
        cp "$file" "$updated_filepath"
        sed -i "s/$old_version/$new_version/g; s/${old_version^^}/${new_version^^}/g" "$updated_filepath"
    fi
done

echo "Text replaced in files and updated copies created successfully in $output_directory."

