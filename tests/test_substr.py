import glob 
import json
import gzip
import subprocess
from pathlib import Path

"""
Testbed: (3, 5) means => ngrams of size 3, need to find substrings of 5 tokens
1. (3,5) (exact duplicate case)
    Text0: "0 1 2 3 4 5" => "0 1 2 3 4 5"
    Text1: "0 1 2 3 4 5" => "0 1 2 3 4 5"
    
2. (3, 5) (prefix/suffix get kept)
     Text0: "3 4 5 6 7" => "3 4 5 6 7"
     Text1: "0 1 2 3 4 5 6 7 8 9 10" => "0 1 2 8 9 10"
     
3. (3, 5) (prefix/suffix get trimmed)
    Text0: "0 1 2 3 4" => "0 1 2 3 4"
    Text1: "8 9 10 11 12" => "8 9 10 11 12"
    Text2: "0 1 2 3 4 5 6 7 8 9 10 11 12" => "5 6 7"
    
4. (3,5) check (i.e. multiple components all found in separate files)
    Text0: "0 1 2" => "0 1 2"
    Text1: "1 2 3" => "1 2 3"
    Text2: "2 3 4" => "2 3 4"
    Text3: "0 1 2 3 4" => ""
    
5. (3,5) check (i.e. multiple overlapping windows)
    Text0: "0 1 2" => "0 1 2"
    Text1: "1 2 3" => "1 2 3"
    Text2: "2 3 4" => "2 3 4"
    Text3: "4 5 6" => "4 5 6"
    Text4: "5 6 7" => "5 6 7"
    Text5: "6 7 8" => "6 7 8"
    Text6: "0 1 2 3 4 5 6 7 8 9 10" => "9 10"
        
6. (3, 5) check (too short)
    Text0: "0 1" => "0 1"
    Text1: "0 1" => "0 1"
"""



def get_wordstr(lo,hi, pfx=''):
    return ' '.join('%sword%02d' % (pfx,i) for i in range(lo, hi))


def make_test_files():
    d1_ranges = [(0, 6), (0, 6)]
    d2_ranges = [(3, 8), (0, 11)]
    d3_ranges = [(0, 5), (8, 13), (0, 13)]
    d4_ranges = [(0, 3), (1, 4), (2, 5), (0, 5)]
    d5_ranges = [(0, 3), (1, 4), (2, 5), (4, 7), (5, 8), (6,9), (0, 11)]
    d6_ranges = [(0,2), (0,2)]

    all_ranges = [d1_ranges, d2_ranges, d3_ranges, d4_ranges, d5_ranges, d6_ranges]
    Path("tests/substr_debug_in").mkdir(parents=True, exist_ok=True)
    Path("tests/substr_debug_out").mkdir(parents=True, exist_ok=True)

    for i, d in enumerate(all_ranges, start=1):

        filename = 'tests/substr_debug_in/test_input_%02d.jsonl.gz' % i
        dicts = [{'text': get_wordstr(*subd, pfx='group%02d_' % i), 'group_id': i, 'doc_id': j} 
                 for j,subd in enumerate(d)]
        to_write = b'\n'.join(json.dumps(_).encode('utf-8') for _ in dicts)
        with open(filename, 'wb') as f:
            f.write(gzip.compress(to_write))    


def run_bff():
    command = 'cargo run bff --inputs tests/substr_debug_in --output-directory tests/substr_debug_out --expected-ngram-count 100000 --fp-rate 0.01 --remove-type substring --max-ngram-size 3 --substr-seqlen 5'

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    # Wait for the command to complete and get the output
    stdout, stderr = process.communicate()

    # Decode the output from bytes to string
    output = stdout.decode("utf-8")
    error = stderr.decode("utf-8")

    # Print the output and error (if any)
    print("Output:")
    print(output)

    return



def read_outputs_into_dict():
    output_dict = {} # (group_id, doc_id) -> text

    for f in glob.glob('tests/substr_debug_out/*'):
        lines = gzip.decompress(open(f, 'rb').read()).splitlines()
        for line in lines:
            d = json.loads(line)
            output_dict[d['group_id'], d['doc_id']] = d['text']
    return output_dict


def get_num_suffices(s):
    return [int(_[-2:]) for _ in s.split(' ')]


def run_asserts(output_dict):

    # 
    assert (1,1) not in output_dict

    assert get_num_suffices(output_dict[2, 1]) == [0, 1, 2, 8, 9, 10]

    assert get_num_suffices(output_dict[3, 2]) == [5, 6, 7]

    assert (4,3) not in output_dict 

    assert get_num_suffices(output_dict[5, 6]) == [9, 10]

    assert get_num_suffices(output_dict[6, 1]) == [0, 1]

if __name__ == '__main__':

    make_test_files()
    run_bff()
    output_dict = read_outputs_into_dict()

    run_asserts(output_dict)

    print("All asserts passing!")

    
