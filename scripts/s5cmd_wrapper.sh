#!/bin/bash


# Default args
fp_rate="0.01"
filtering_threshold="0.8"
min_ngram_size="20"
max_ngram_size="20"
remove_type="both"
shard_num="0"
total_shards="1"


# Parsing the args

while [[ $# -gt 0 ]]; do
  case $1 in
    --s3-input=*) # REQUIRED
      s3_input="${1#*=}"
      shift
      ;;
    --s3-output=*) # REQUIRED
      s3_output="${1#*=}"
      shift
      ;;
    --local-dir=*) #REQUIRED 
      local_dir="${1#*=}"
      shift
      ;;
    --expected-ngram-count=*) # REQUIRED
      expected_ngram_count="${1#*=}"
      shift
      ;;
    --fp-rate=*) # default 0.01
      fp_rate="${1#*=}"
      shift
      ;;
    --min-ngram-size=*) # default 20
      min_ngram_size="${1#*=}"
      shift
      ;;
    --max-ngramsize=*) # default 20
      max_ngram_size="${1#*=}"
      shift
      ;;
    --filtering-threshold=*) # default 0.8
      filtering_threshold="${1#*=}"
      shift
      ;;
    --remove-type=*) # default both
      remove_type="${1#*=}"
      shift
      ;;
    --shard-num=*) # default 0
      shard_num="${1#*=}"
      shift
      ;;            
    --total-shards=*) # default 1
      total_shards="${1#*=}"
      shift
      ;;           
    *)
      echo "Unknown argument: $1"
      shift
      ;;
  esac
done

# Run some asserts that the up/down is specified correctly
if [[ -z $s3_input ]]; then
  echo "Error: --s3-input is required and cannot be empty."
  exit 1
fi

if [[ -z $s3_output ]]; then
  echo "Error: --s3-output is required and cannot be empty."
  exit 1
fi

if [[ -z $local_dir ]]; then
  echo "Error: --local-dir is required and cannot be empty."
  exit 1
fi


# Actually do the thing:
input_dir=${local_dir%/}/input/
output_dir=${local_dir%/}/output/

mkdir -p $input_dir
mkdir -p $output_dir

echo "Downloading S3 files to local..."
s5cmd cp --show-progress "${s3_input%/}/*" $input_dir


echo "Running BFF..."
cargo run --release bff \
--inputs $input_dir \
--output-directory $output_dir \
--expected-ngram-count $expected_ngram_count \
--fp-rate $fp_rate \
--min-ngram-size $min_ngram_size \
--max-ngram-size $max_ngram_size \
--filtering-threshold $filtering_threshold \
--remove-type $remove_type \
--shard-num $shard_num \
--total-shards $total_shards \


echo "Uploading dedup'ed data to S3..."
s5cmd cp --show-progress $output_dir $s3_output




