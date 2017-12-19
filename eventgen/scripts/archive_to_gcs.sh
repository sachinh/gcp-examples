#~/bin/sh

src_file="$1"
target_dir="/mnt/gcs_data/generated_data"
target_file_stem="impression.data"
datecmd="date +%FT%H-%M-%S"

echo "The file provided is: $src_file"
echo "---the current time is: `$datecmd`"
echo "---Target Dir: $target_dir"
echo "---Target File Stem: $target_file_stem"

cd /home/email_sachinh/src/eventgen/generated_data/
mv "$src_file" "$target_dir/$target_file_stem.`$datecmd`"
echo "File Moved!"

#/mnt/gcs_data/generated_data/impression.data.`$datecmd`
