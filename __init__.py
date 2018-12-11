from SplunkCSVParser import SplunkCSVParser
import sys


def main():
    if len(sys.argv) != 4:
        print("usage: {} WORKINGDIR CSVFILE OUTPUTFILE".format(sys.argv[0]))
        return

    working_dir = sys.argv[1]
    testfile = sys.argv[2]
    testoutput = sys.argv[3]

    parser = SplunkCSVParser(working_dir, testfile)
    parser.splunk_csv_keys()
    # parser.write_raw(write_tags=True, tag_filter="etcd")
    parser.write_raw_by_tag()


if __name__ == '__main__':
    main()
