import csv
import json
from os import path
from os import makedirs


# print(csv.list_dialects())

# class SplunkDialect(csv.Dialect)


class SplunkCSVParser():
    def __init__(self, workingdir, csvfile, outputdir=''):
        self.workingdir = workingdir
        self.csvfile = path.join(workingdir, csvfile)
        self.outputdir = outputdir
        self.outputfile = path.join(workingdir, outputdir, "output")
        self.tags = {}
        self.tags_extracted = False

    def write_raw_lines(self):
        output_file = path.join(self.workingdir, "raw_output")
        with open(self.csvfile) as csvfile, open(output_file, 'w+') as output_file:
            splunkreader = csv.DictReader(csvfile)
            for row in splunkreader:
                output_file.write(row["_raw"] + "\n")

    def write_ts_line(self):
        with open(self.csvfile) as csvfile, open(self.outputfile, 'w+') as output_file:
            splunkreader = csv.DictReader(csvfile)
            for row in splunkreader:
                output_line = row["_time"] + ": " + row["line"]
                output_file.write(output_line + "\n")

    def write_file_by_source(self):
        sources = {}
        with open(self.csvfile) as csvfile:
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

    def write_file_by(self, attr):
        sources = {}
        with open(self.csvfile) as csvfile:
            splunkreader = csv.DictReader(csvfile)
            for row in splunkreader:
                if row[attr] not in sources.keys():
                    sources[row[attr]] = []
                log_line = row["_time"] + ": " + row["line"] + "\n"
                sources[row[attr]].append(log_line)
            for key in sources.keys():
                print(key)
                print("lines: {}".format(len(sources[key])))
                filename = "{}_{}".format(attr, key)
                output_file = path.join(self.workingdir, filename)
                with open(output_file, "w+") as output:
                    output.writelines(sources[key])
            pass

    def splunk_csv_keys(self):
        with open(self.csvfile) as csvfile:
            splunkreader = csv.DictReader(csvfile)
            for row in splunkreader:
                for i in row.keys():
                    if i == "_raw":
                        raw = json.loads(row[i])
                        print("raw: {}".format(raw))
                        print("raw keys: {}".format(raw.keys()))
                        continue
                    if row[i]:
                        print("{}: {}".format(i, row[i]))
                return row.keys()

    def write_raw(self, write_tags=False, tag_filter=None):

        # parse all the lines into a dict indexed by tag
        self._extract_tags()

        if tag_filter:  # only write self.self.self.tags matching filter
            with open(self.outputfile + "-" + tag_filter, "w+") as output:
                for tag in self.tags:
                    if tag_filter in tag:
                        output.write("--- {}\n".format(tag))
                        for line in self.tags[tag]:
                            output.write("{}\n".format(line))
            print("output available at {}".format(self.outputfile + "-" + tag_filter))

        else:  # write all
            with open(self.outputfile, "w+") as output:
                for tag in self.tags:
                    output.write("--- {}\n".format(tag))
                    for line in self.tags[tag]:
                        output.write("{}\n".format(line))
            print("output available at {}".format(self.outputfile))

        if write_tags == True:
            self._write_tags_to_file()

    def write_raw_by_tag(self):

        # parse all the lines into a dict indexed by tag
        self._extract_tags()

        #make a directory to hold all the files
        tag_base_dir = path.join(self.workingdir, "tags")
        for tag, lines in self.tags.items():
            tag_filepath = path.join(tag_base_dir,tag)
            tag_filepath = tag_filepath.replace(':','-')
            tag_dir = path.dirname(tag_filepath)
            if not path.isdir(tag_dir):
                makedirs(tag_dir)
            print("path dirname: " + path.dirname(tag_filepath))
            with open(tag_filepath, "w+") as tf:
                for line in lines:
                    tf.write(line + "\n")

    def _write_tags_to_file(self):
        tags_fp = path.join(self.workingdir, "tags.txt")
        with open(tags_fp, "w+") as t:
            for tag in self.tags:
                t.write(tag + "\n")
        print("tags available at: {}".format(tags_fp))

    def _extract_tags(self):
        if self.tags_extracted:
            return
        with open(self.csvfile) as csvfile:
            splunkreader = csv.DictReader(csvfile)
            for row in splunkreader:
                raw = json.loads(row["_raw"])
                if raw["tag"] not in self.tags:
                    self.tags[raw["tag"]] = []
                self.tags[raw["tag"]].append(raw["line"])
        self.tags_extracted = True
