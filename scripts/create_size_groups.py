"""
Simple adapter script that takes in the outputs of an 
`aws s3 ls ...` command (that's been piped to an outfile)
and a max-file-size and groups these into files
"""


import argparse



def create_groups(input_lines, group_size_in_bytes):
	groups = []
	cur_group, cur_group_size = [], 0
	
	parse_line = lambda line: line.strip().split(' ')[-2:]# outputs (size_in_bytes:string)
	for line in input_lines:
		next_line_bytes, next_line_file = parse_line(line)
		next_line_bytes = int(next_line_bytes)
		if cur_group_size + next_line_bytes > group_size_in_bytes: # make new group
			if len(cur_group) > 0:
				groups.append(cur_group)
			cur_group_size = next_line_bytes
			cur_group = [next_line_file]
		else:
			cur_group_size += next_line_bytes
			cur_group.append(next_line_bytes)

	groups.append(cur_group)
	return groups


def write_groupfile(groups, output_file):
	""" Modify this to make it easy to read in rust"""
	with open(output_file, 'w') as f:
		for group in groups:
			f.write(','.join(group) + '\n')


def main(input_file, output_file, group_size_in_bytes):
	input_lines = open(input_file, 'r').readlines()
	groups = create_groups(input_lines, group_size_in_bytes)
	write_groupfile(groups, output_file)


if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('--input', type=str, required=True)
	parser.add_argument('--output', type=str, required=True)
	parser.add_argument('--groupsize', type=int, required=True)

	args = parser.parse_args()
	main(input_file=args.input,
		 output=args.output,
		 group_size_in_bytes=args.groupsize)

