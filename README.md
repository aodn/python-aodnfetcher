[![build](https://travis-ci.org/aodn/python-aodnfetcher.png)](https://travis-ci.org/aodn/python-aodnfetcher)
[![coverage](https://codecov.io/gh/aodn/python-aodnfetcher/branch/master/graph/badge.svg)](https://codecov.io/gh/aodn/python-aodnfetcher)

# python-aodnfetcher
Multi-protocol artifact fetcher library/utility

## Installation

Note: it is recommended to install this in a [virtualenv](https://virtualenv.pypa.io/en/stable/).

### Install directly from GitHub
```bash
$ pip install git+https://github.com/aodn/python-aodnfetcher.git@master
```

### Install in 'editable' mode from local directory
```bash
git clone https://github.com/aodn/python-aodnfetcher.git
cd python-aodnfetcher
pip install -e .
```

### Create a wheel package
```bash
git clone https://github.com/aodn/python-aodnfetcher.git
cd python-aodnfetcher
python setup.py bdist_wheel
```

## Usage

### Commandline interface

The command line exposes the fetcher functionality for use interactively, or to be called from a script.

In order to be useful for integrating with non-Python languages (e.g. Bash/Ruby), the utility outputs a JSON document
which can be parsed and used to programmatically determine the path to the file(s) that were downloaded by the call to
the program. This is required in particular by the "jenkins://" and "schemabackup://" pseudo protocols, in which case
the "real" path to the file is not known until it is resolved by the fetcher code.

```bash
$ aodnfetcher --help
usage: aodnfetcher [-h] [--authenticated] [--cache_dir CACHE_DIR]
                   [--enable-logging] [--outfile [OUTFILE] | --no-json]
                   artifact [artifact ...]

Fetch one or more artifacts from various URL schemes.

positional arguments:
  artifact              artifact URL to download

optional arguments:
  -h, --help            show this help message and exit
  --authenticated, -a   create an authenticated boto3 client for S3
                        operations. The default is to create an UNSIGNED
                        (anonymous) client.
  --cache_dir CACHE_DIR, -c CACHE_DIR
                        optional cache dir. If specified, the directory will
                        be checked for previously downloaded files, and if
                        unchanged, the artifact is instead fetched from the
                        cache. Missing or changed files will be added to the
                        cache.
  --enable-logging, -l  configure a basic logger to view library log output
  --outfile [OUTFILE], -o [OUTFILE]
                        optional output file for JSON document. If not
                        specified, the JSON is written to STDOUT.
  --no-json, -j         suppress output of JSON document to STDOUT

Supported URL schemes: http, https, jenkins, s3

Example URLs:
http://www.example.com/artifact.zip
https://www.example.com/artifact.zip
s3://mybucket/prefix/artifact.zip
s3prefix://mybucket/prefix?pattern=^.*.zip$&sortmethod=version
s3prefix://mybucket/prefix?pattern=^.*.zip$&sortmethod=newest
file://directory/artifact.zip
jenkins://mybucket/myjob (downloads WAR artifact from latest build of job)
jenkins://mybucket/myjob?pattern=^.*\.whl$ (downloads artifact matching given pattern from latest build of job)
schemabackup://mybucket/myhost/mydatabase/myschema (downloads latest backup timestamp)
schemabackup://mybucket/myhost/mydatabase/myschema?timestamp=YYYY.MM.DD.hh.mm.ss (downloads the backup with the corresponding timestamp)

```

```bash
$ aodnfetcher https://github.com/aodn/aodn-portal/archive/master.zip \
jenkins://imos-binary/portal_4_prod \
jenkins://imos-binary/cc_plugin_imos_prod?pattern=^.*\.whl$ \
s3://imos-binary/static/talend/stels_mdb_pack.zip | python -m json.tool
{
    "https://github.com/aodn/aodn-portal/archive/master.zip": {
        "local_file": "master.zip",
        "real_url": "https://github.com/aodn/aodn-portal/archive/master.zip"
    },
    "jenkins://imos-binary/cc_plugin_imos_prod?pattern=^.*.whl$": {
        "local_file": "cc_plugin_imos-1.2.1-py2-none-any.whl",
        "real_url": "s3://imos-binary/jobs/cc_plugin_imos_prod/13/cc_plugin_imos-1.2.1-py2-none-any.whl"
    },
    "jenkins://imos-binary/portal_4_prod": {
        "local_file": "aodn-portal-4.37.1-production.war",
        "real_url": "s3://imos-binary/jobs/portal_4_prod/67/aodn-portal-4.37.1-production.war"
    },
    "s3://imos-binary/static/talend/stels_mdb_pack.zip": {
        "local_file": "stels_mdb_pack.zip",
        "real_url": "s3://imos-binary/static/talend/stels_mdb_pack.zip"
    }
}


$ ls -l
total 55768
-rw-rw-r-- 1 user user 44664939 Apr 18 12:47 aodn-portal-4.37.1-production.war
-rw-rw-r-- 1 user user  9181648 Apr 18 12:47 master.zip
-rw-rw-r-- 1 user user  3255960 Apr 18 12:47 stels_mdb_pack.zip
```

### Python library interface

```python
import aodnfetcher


# fetch an artifact from a supported URL scheme
result = aodnfetcher.download_file('jenkins://imos-binary/portal_4_prod')
print(result)
{'local_file': 'aodn-portal-4.38.8-production.war', 'real_url': 's3://imos-binary/jobs/portal_4_prod/70/aodn-portal-4.38.8-production.war'}

# fetch an artifact with a custom name
result = aodnfetcher.download_file('https://github.com/aodn/aodn-portal/archive/master.zip',
                                   local_file='aodn-portal-source.zip')
print(result)
{'local_file': 'aodn-portal-source.zip', 'real_url': 'https://github.com/aodn/aodn-portal/archive/master.zip'}

# fetch an artifact via a direct or caching downloader depending on a whether cache_dir is supplied
cache_dir = None
result = aodnfetcher.download_file('s3://imos-binary/static/talend/stels_mdb_pack.zip', cache_dir=cache_dir)
print(result)
{'local_file': 'stels_mdb_pack.zip', 'real_url': 's3://imos-binary/static/talend/stels_mdb_pack.zip'}
```
