#!/bin/bash
mkdir -p output_csvs

for file in testcases/*.json; do
    echo "Running: $file"
    ./json2relcsv "$file" --print-ast -out-dir ./output_csvs
done

echo "All tests complete. Check ./output_csvs"
