import re
import sys

def scan_file(input_file, start_line, end_line, pattern, output_file):
    try:
        with open(input_file, 'r') as file:
            lines = file.readlines()
            with open(output_file, 'w') as out_file:
                for i, line in enumerate(lines):
                    if start_line <= i <= end_line:
                        if re.search(pattern, line):
                            out_file.write(line)
    except FileNotFoundError:
        print("Input file not found.")

def show_help():
    print(f"Usage: python {sys.argv[0]} input_file start_line end_line pattern output_file")
    print("Introduction:")
    print(" This tool will scan the `input_file` in the line range [start_line,end_line] whose "
    "content satisfy the `pattern` and output it to the `output_file`")
    print("Arguments:")
    print("  input_file   : The input file to scan.")
    print("  start_line   : The starting line number to scan from.")
    print("  end_line     : The ending line number to scan until.")
    print("  pattern      : The regular expression pattern to search for.")
    print("  output_file  : The output file to write the matched lines.")

if __name__ == "__main__":
    if len(sys.argv) != 6 or sys.argv[1] in ['-h', '--help']:
        show_help()
    else:
        input_file = sys.argv[1]
        start_line = int(sys.argv[2])
        end_line = int(sys.argv[3])
        pattern = sys.argv[4]
        output_file = sys.argv[5]
        scan_file(input_file, start_line, end_line, pattern, output_file)
