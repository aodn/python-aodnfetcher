import abc
import errno
import json
import logging
import os
import re
import urllib2
from functools import partial
from hashlib import sha256
from shutil import copyfileobj
from urlparse import ParseResult, parse_qs, urlparse, urlunparse

import boto3
import botocore.config
import botocore.exceptions
from botocore import UNSIGNED

__all__ = [
    'fetcher',
    'fetcher_downloader',
    'AuthenticationError',
    'InvalidArtifactError',
    'FetcherCachingDownloader',
    'FetcherDirectDownloader',
    'HTTPFetcher',
    'LocalFileFetcher',
    'S3Fetcher',
    'JenkinsS3Fetcher',
    'SchemaBackupS3Fetcher'
]

LOGGER = logging.getLogger('aodnfetcher')


class AuthenticationError(Exception):
    pass


class InvalidArtifactError(Exception):
    pass


class KeyResolutionError(Exception):
    def __init__(self, reason_code, message):
        self.reason_code = reason_code
        self.message = message

    def __repr__(self):
        return "{}(message=\"{}\", reason_code=\"{}\")".format(self.__class__.__name__, self.message, self.reason_code)

    __str__ = __repr__


def fetcher(artifact, authenticated=False):
    """Factory to return an appropriate AbstractFileFetcher subclass for the given artifact string, or raise an
    exception if URL scheme is unknown or invalid

    :param artifact: artifact URL string
    :param authenticated: if true, boto3 will use the environment credentials, otherwise an anonymous client is created
    :return: AbstractFileFetcher subclass
    """
    parsed_url = urlparse(artifact)

    if parsed_url.scheme == 'jenkins':
        return JenkinsS3Fetcher(parsed_url=parsed_url, authenticated=authenticated)
    elif parsed_url.scheme == 'schemabackup':
        return SchemaBackupS3Fetcher(parsed_url=parsed_url, authenticated=authenticated)
    elif parsed_url.scheme in ('http', 'https'):
        return HTTPFetcher(parsed_url=parsed_url)
    elif parsed_url.scheme == 's3':
        return S3Fetcher(parsed_url=parsed_url, authenticated=authenticated)
    elif parsed_url.scheme == 'file' or not parsed_url.scheme:
        return LocalFileFetcher(parsed_url=parsed_url)
    else:
        raise InvalidArtifactError("unable to find a fetcher for artifact '{artifact}'".format(artifact=artifact))


def fetcher_downloader(cache_dir=None):
    """Factory to return a AbstractFetcherDownloader subclass based on whether

    :param cache_dir: optional cache directory which, if set, triggers the creation of a caching downloader
    :return: AbstractFetcherDownloader subclass
    """
    return FetcherCachingDownloader(cache_dir=cache_dir) if cache_dir else FetcherDirectDownloader()


class AbstractFetcherDownloader(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(AbstractFetcherDownloader, self).__init__()
        LOGGER.info("creating FetcherDownloader of type '{t}'".format(t=self.__class__.__name__))

    @abc.abstractmethod
    def get_handle(self, file_fetcher):
        pass


class FetcherCachingDownloader(AbstractFetcherDownloader):
    """Abstracts the interactions with a cache directory comprising of a JSON index file, and file objects corresponding
    to remote files encapsulated by Fetcher instances.

    Files are stored based on the sha256 hash of their full URL ('real_url' attribute), and cache validation is
    performed using an implementation-specific Fetcher 'unique_id' attribute to identify stale files (e.g. using Etags,
    or some other way to determine a persistent ID for an unchanged file on the remote server).
    """

    def __init__(self, cache_dir, cache_index_file='cacheindex.json'):
        super(FetcherCachingDownloader, self).__init__()

        self.cache_dir = cache_dir
        self.cache_index_file = os.path.join(cache_dir, cache_index_file)

        try:
            os.mkdir(cache_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    @property
    def index(self):
        try:
            with open(self.cache_index_file, 'r') as f:
                index = json.load(f)
        except (IOError, ValueError):
            index = {}
        return index

    def file_is_current(self, file_fetcher):
        if not file_fetcher.unique_id:
            return False
        cache_path = self._get_cache_path(file_fetcher)
        return os.path.exists(cache_path) and file_fetcher.unique_id == self.get_cached_object_id(file_fetcher)

    def get_cached_object_id(self, file_fetcher):
        cache_key = self.get_cache_key(file_fetcher)
        obj = self.index.get(cache_key, {})
        return obj.get('id')

    @staticmethod
    def get_cache_key(file_fetcher):
        return sha256(file_fetcher.real_url).hexdigest()

    def get_handle(self, file_fetcher):
        cache_path = self._get_cache_path(file_fetcher)
        if self.file_is_current(file_fetcher):
            LOGGER.info("'{artifact}' is current, using cached file".format(artifact=file_fetcher.real_url))
        else:
            LOGGER.info("'{artifact}' is missing or stale, downloading".format(artifact=file_fetcher.real_url))
            self._put_file(file_fetcher)
        return open(cache_path, mode='rb')

    def _get_cache_path(self, file_fetcher):
        return os.path.join(self.cache_dir, self.get_cache_key(file_fetcher))

    def _put_file(self, file_fetcher):
        cache_path = self._get_cache_path(file_fetcher)
        with open(cache_path, 'wb') as out_file:
            copyfileobj(file_fetcher.handle, out_file)
        self._update_index(file_fetcher)

    def _update_index(self, file_fetcher):
        cache_key = self.get_cache_key(file_fetcher)
        index = dict(self.index)
        index[cache_key] = {'id': file_fetcher.unique_id, 'url': file_fetcher.real_url}
        with open(self.cache_index_file, 'w') as f:
            json.dump(index, f)


class FetcherDirectDownloader(AbstractFetcherDownloader):
    def get_handle(self, file_fetcher):
        return file_fetcher.handle


class AbstractFileFetcher(object):
    """Abstract class for Fetcher classes to define the interface used by the FetcherDownloader
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, parsed_url):
        self.parsed_url = parsed_url

        self._handle = None

    @abc.abstractproperty
    def real_url(self):
        pass

    @abc.abstractproperty
    def handle(self):
        pass

    @abc.abstractproperty
    def unique_id(self):
        pass


class DefaultErrorHandler(urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, fp, code, msg, headers):
        result = urllib2.HTTPError(req.get_full_url(), code, msg, headers, fp)
        result.status = code
        return result


class HTTPFetcher(AbstractFileFetcher):
    """Fetch from a regular HTTP URL, using Etag header (if available) to provide identifier for cache validation
    """

    def __init__(self, parsed_url):
        super(HTTPFetcher, self).__init__(parsed_url)

        self.path = parsed_url.path
        self._stream = None

    @property
    def real_url(self):
        return urlunparse(self.parsed_url)

    @property
    def stream(self):
        if self._stream is None:
            opener = urllib2.build_opener(DefaultErrorHandler())
            self._stream = opener.open(urllib2.Request(self.real_url))
        return self._stream

    @property
    def handle(self):
        if self._handle is None:
            self._handle = self.stream.fp
        return self._handle

    @property
    def unique_id(self):
        return self.stream.headers.get('ETag')


class LocalFileFetcher(AbstractFileFetcher):
    """Fetch from a local file path, using the sha256 sum of the file to provide identifier for cache validation
    """

    def __init__(self, parsed_url):
        super(LocalFileFetcher, self).__init__(parsed_url)

        if parsed_url.netloc:
            path = os.path.join(os.path.abspath(parsed_url.netloc), parsed_url.path.lstrip('/'))
        elif os.path.isabs(parsed_url.path):
            path = parsed_url.path
        else:
            path = os.path.abspath(parsed_url.path)

        self.path = path

    @property
    def real_url(self):
        return self.path

    @property
    def handle(self):
        if self._handle is None:
            self._handle = open(self.path, mode='rb')
        return self._handle

    @property
    def unique_id(self):
        hasher = sha256()
        with open(self.path, 'rb') as f:
            for block in iter(partial(f.read, 65536), b''):
                hasher.update(block)
        return hasher.hexdigest()


class S3Fetcher(AbstractFileFetcher):
    """Fetch from an S3 URL, using Etag header to provide identifier for cache validation
    """

    def __init__(self, parsed_url, authenticated=False, s3_client=None):
        super(S3Fetcher, self).__init__(parsed_url)

        self.bucket = parsed_url.netloc
        self.path = parsed_url.path.lstrip('/')

        self.s3_client = s3_client or self.get_client(authenticated=authenticated)

        self._object = None

    @property
    def real_url(self):
        return urlunparse(self.parsed_url)

    @property
    def handle(self):
        if self._handle is None:
            self._handle = self.object['Body']
        return self._handle

    @property
    def object(self):
        if self._object is None:
            self._object = self._get_object(bucket=self.bucket, path=self.path)
        return self._object

    @property
    def unique_id(self):
        return self.object['ResponseMetadata']['HTTPHeaders']['etag']

    def _get_object(self, bucket, path):
        try:
            return self.s3_client.get_object(Bucket=bucket, Key=path)
        except botocore.exceptions.ClientError as e:
            raise AuthenticationError("S3 authentication failed. {e.__class__.__name__}: {e}".format(e=e))

    @staticmethod
    def get_client(authenticated=False):
        s3_client_kwargs = {}
        if authenticated:
            LOGGER.info('creating authenticated S3 client')
        else:
            LOGGER.info('creating anonymous S3 client')
            s3_client_kwargs['config'] = botocore.config.Config(signature_version=UNSIGNED)
        return boto3.client('s3', **s3_client_kwargs)


class BaseResolvingS3Fetcher(AbstractFileFetcher):
    def __init__(self, parsed_url, authenticated=False):
        super(BaseResolvingS3Fetcher, self).__init__(parsed_url)
        self.authenticated = authenticated

        self.bucket = parsed_url.netloc

        self.s3_client = S3Fetcher.get_client(authenticated=authenticated)

        self._fetcher = None
        self._handle = None
        self._key = None
        self._real_parse_result = None

    @property
    def fetcher(self):
        """Due to the dynamic nature of this resolver, an inner S3Fetcher is used once the real object is known

        :return: S3Fetcher for the real object
        """
        if self._fetcher is None:
            self._fetcher = S3Fetcher(parsed_url=self.real_parsed_url, s3_client=self.s3_client)
        return self._fetcher

    @property
    def real_parsed_url(self):
        """The original parse_result attribute is not useful (since only the job name was known at that time), so a new
        ParseResult is constructed reflecting the dynamically resolved S3 object

        :return: ParseResult for the real object
        """
        if self._real_parse_result is None:
            self._real_parse_result = ParseResult('s3', self.bucket, self.path, '', '', '')
        return self._real_parse_result

    @property
    def real_url(self):
        return urlunparse(self.real_parsed_url)

    @property
    def handle(self):
        if self._handle is None:
            self._handle = self.fetcher.handle
        return self._handle

    @property
    def object(self):
        return self.fetcher.object

    @property
    def path(self):
        """Unlike other Fetchers, path is a lazy property, because it is not known at the time of initialisation

        :return: dynamically determined key
        """
        if self._key is None:
            self._key = self._get_key()
        return self._key

    @property
    def unique_id(self):
        return self.object['ResponseMetadata']['HTTPHeaders']['etag']

    @abc.abstractmethod
    def _get_key(self):
        pass


class JenkinsS3Fetcher(BaseResolvingS3Fetcher):
    """Fetch from a Jenkins managed S3 artifact bucket, resolving the latest artifact for the given job, and using Etag
        header to provide identifier for cache validation
    """
    key_parse_pattern = re.compile(r"^jobs/(?P<job_name>[^/]+)/(?P<build_number>[^/]+)/(?P<basename>.*)$")

    def __init__(self, parsed_url, authenticated=False):
        super(JenkinsS3Fetcher, self).__init__(parsed_url, authenticated)

        self.job_name = parsed_url.path.lstrip('/')

        try:
            self.filename_pattern = parse_qs(parsed_url.query)['pattern'][0]
        except (KeyError, IndexError):
            self.filename_pattern = r'^.*\.war$'

        self._all_builds = None

    @property
    def all_builds(self):
        if self._all_builds is None:
            self._all_builds = self.s3_client.list_objects_v2(Bucket=self.bucket,
                                                              Prefix="jobs/{}".format(self.job_name))
        return self._all_builds

    def _get_key(self):
        if not self.all_builds.get('Contents'):
            raise KeyResolutionError('NO_RESULTS',
                                     "job '{s.job_name}' was invalid or returned no builds".format(s=self))

        try:
            latest = self._get_matching_builds()[-1]
        except IndexError:
            raise KeyResolutionError('NO_MATCHING_BUILDS',
                                     "no builds found for '{s.job_name}' matching '{s.filename_pattern}'".format(
                                         s=self))
        return "jobs/{job_name}/{build_number}/{basename}".format(**latest)

    def _get_matching_builds(self):
        matching_keys = (self.key_parse_pattern.match(a['Key']).groupdict() for a in self.all_builds['Contents'] if
                         re.match(self.filename_pattern, a['Key']))
        sorted_keys = sorted(matching_keys, key=lambda p: int(p['build_number']))
        return sorted_keys


class SchemaBackupS3Fetcher(BaseResolvingS3Fetcher):
    def __init__(self, parsed_url, authenticated=True):
        super(SchemaBackupS3Fetcher, self).__init__(parsed_url, authenticated)

        components = parsed_url.path.lstrip('/').split('/')
        if len(components) != 3:
            raise ValueError('URL must be in the format: schemabackup://bucket/host/database/schema')
        self.host, self.database, self.schema = components

        try:
            self.timestamp = parse_qs(parsed_url.query)['timestamp'][0]
        except (KeyError, IndexError):
            self.timestamp = 'LATEST'

    def _get_key(self):
        host_prefix_components = ['backups', '']
        host_prefix = os.path.join(*host_prefix_components)

        host_response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=host_prefix, Delimiter='/')

        all_hosts = [os.path.relpath(c['Prefix'], host_prefix)
                     for c in host_response.get('CommonPrefixes', [])
                     if c['Prefix'].startswith(host_prefix)]

        if self.host not in all_hosts:
            raise KeyResolutionError('HOST_NOT_FOUND',
                                     "host '{h}' not found in bucket '{b}'.".format(h=self.host, b=self.bucket))

        base_prefix = os.path.join(host_prefix, self.host, 'pgsql', '')

        timestamps_response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=base_prefix, Delimiter='/')

        all_timestamps = sorted(os.path.relpath(c['Prefix'], base_prefix)
                                for c in timestamps_response.get('CommonPrefixes', [])
                                if c['Prefix'].startswith(base_prefix))

        if not all_timestamps:
            raise KeyResolutionError('NO_TIMESTAMPS',
                                     "no candidate timestamps found in bucket '{b}'.".format(b=self.bucket))

        if self.timestamp == 'LATEST':
            selected_timestamp = all_timestamps[-1]
        elif self.timestamp in all_timestamps:
            selected_timestamp = self.timestamp
        else:
            raise KeyResolutionError('TIMESTAMP_NOT_FOUND',
                                     "timestamp '{t}' not found in bucket '{b}'. Available timestamp candidates: {c}".format(
                                         t=self.timestamp,
                                         b=self.bucket,
                                         c=all_timestamps))

        key_components = [selected_timestamp, self.database, "{schema}.dump".format(schema=self.schema)]
        key_name = os.path.join(base_prefix, *key_components)

        try:
            self.s3_client.get_object(Bucket=self.bucket, Key=key_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise KeyResolutionError('SCHEMA_NOT_FOUND',
                                         "schema backup '{k}' not found in bucket under timestamp '{t}'".format(
                                             k=key_name,
                                             t=self.timestamp))
            raise

        return key_name
