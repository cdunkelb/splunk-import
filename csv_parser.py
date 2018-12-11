import csv
import json
from os import path

working_dir = ""
testfile = ""
testoutput = ""
testfile = path.join(working_dir,testfile)

def write_raw_lines():
    output_file = path.join(working_dir, "raw_output")
    with open(testfile) as csvfile, open(output_file, 'w+') as output_file:
        splunkreader = csv.DictReader(csvfile)
        for row in splunkreader:
            output_file.write(row["_raw"] + "\n")

def write_ts_line():
    with open(testfile) as csvfile, open(testoutput, 'w+') as output_file:
        splunkreader = csv.DictReader(csvfile)
        for row in splunkreader:
            output_line = row["_time"] + ": " + row["line"]
            output_file.write(output_line + "\n")

def write_file_by_source():
    sources = {}
    with open(testfile) as csvfile:
        splunkreader = csv.DictReader(csvfile)
        for row in splunkreader:
            if row["source"] not in sources.keys():
                sources[row["source"]] = []
            log_line = row["_time"] + ": " + row["line"]
            sources[row["source"]].append(log_line)
        for key in sources.keys():
            print(key)
            print("lines: {}".format(len(sources[key])))
        pass

def write_file_by(attr):
    sources = {}
    with open(testfile) as csvfile:
        splunkreader = csv.DictReader(csvfile)
        for row in splunkreader:
            if row[attr] not in sources.keys():
                sources[row[attr]] = []
            log_line = row["_time"] + ": " + row["line"] + "\n"
            sources[row[attr]].append(log_line)
        for key in sources.keys():
            print(key)
            print("lines: {}".format(len(sources[key])))
            filename="{}_{}".format(attr,key)
            output_file = path.join(working_dir,filename)
            with open(output_file,"w+") as output:
                output.writelines(sources[key])
        pass

def splunk_csv_keys():
    with open(testfile) as csvfile:
        splunkreader = csv.DictReader(csvfile)
        for row in splunkreader:
            for i in row.keys():
                if i == "_raw":
                    raw = json.loads(row[i])
                    print(raw)
                    continue
                if row[i]:
                    print("{}: {}".format(i,row[i]))
            return