for file in $1; do
    mv "$file" "`basename $file .html`.txt"
done