import argparse
import errno
import json
import logging
import os
import sys
import textwrap

import aodnfetcher


class writable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        try:
            os.mkdir(prospective_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        if not os.path.isdir(prospective_dir):
            raise argparse.ArgumentError(self, "writable_dir: {0} is not a valid path".format(prospective_dir))
        if os.access(prospective_dir, os.W_OK):
            setattr(namespace, self.dest, prospective_dir)
        else:
            raise argparse.ArgumentError(self, "writable_dir: {0} is not a writable dir".format(prospective_dir))


def main():
    logging.getLogger('botocore').setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(prog='aodnfetcher',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="Fetch one or more artifacts from various URL schemes.",
                                     epilog=textwrap.dedent('''\
                                     Supported URL schemes: http, https, jenkins, s3
                                     
                                     Example URLs:
                                     http://www.example.com/artifact.zip
                                     https://www.example.com/artifact.zip
                                     s3://mybucket/prefix/artifact.zip
                                     jenkins://mybucket/myjob (downloads WAR artifact from latest build of job)
                                     jenkins://mybucket/myjob?pattern=^.*\.whl$ (downloads artifact matching given pattern from latest build of job)
                                     schemabackup://mybucket/myhost/mydatabase/myschema (downloads latest backup timestamp)
                                     schemabackup://mybucket/myhost/mydatabase/myschema?timestamp=YYYY.MM.DD.hh.mm.ss (downloads the backup with the corresponding timestamp)
                                     '''))
    parser.add_argument('artifact', nargs='+', help='artifact URL to download')
    parser.add_argument('--authenticated', '-a', action='store_true',
                        help='create an authenticated boto3 client for S3 operations. '
                             'The default is to create an UNSIGNED (anonymous) client.')
    parser.add_argument('--cache_dir', '-c', action=writable_dir,
                        help='optional cache dir. If specified, the directory will be checked for previously downloaded'
                             ' files, and if unchanged, the artifact is instead fetched from the cache. Missing or'
                             ' changed files will be added to the cache.')
    parser.add_argument('--enable-logging', '-l', action='store_true',
                        help='configure a basic logger to view library log output on STDERR')
    json_group = parser.add_mutually_exclusive_group()
    json_group.add_argument('--outfile', '-o', nargs='?', type=argparse.FileType('wb'), default=sys.stdout,
                            help='optional output file for JSON document. '
                                 'If not specified, the JSON is written to STDOUT.')
    json_group.add_argument('--no-json', '-j', action='store_true',
                            help='suppress output of JSON document to STDOUT')

    args = parser.parse_args()

    if args.enable_logging:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    results = {}
    for artifact in args.artifact:
        try:
            result = aodnfetcher.download_file(artifact, authenticated=args.authenticated, cache_dir=args.cache_dir)
        except Exception as e:
            if args.outfile is not sys.stdout:
                args.outfile.close()
                os.unlink(args.outfile.name)
            sys.exit(str(e))
        results[artifact] = result

    if not args.no_json:
        json.dump(results, args.outfile, indent=4, separators=(',', ': '))


if __name__ == '__main__':
    main()
