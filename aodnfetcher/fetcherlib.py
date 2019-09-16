import abc
import errno
import json
import logging
import os
import posixpath
import re
import shutil
import tempfile
from functools import partial
from hashlib import sha256
from io import BytesIO

import boto3
import botocore.config
import botocore.exceptions
import requests
from fasteners import InterProcessLock
from packaging import version

try:
    from urllib.parse import ParseResult, parse_qs, urlparse
except ImportError:
    from urlparse import ParseResult, parse_qs, urlparse

try:  # pragma: no cover
    from urllib import urlencode
except ImportError:  # pragma: no cover
    from urllib.parse import urlencode

__all__ = [
    'download_file',
    'fetcher',
    'fetcher_downloader',
    'get_file_hash',
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


class InvalidCacheEntryError(Exception):
    pass


class KeyResolutionError(Exception):
    def __init__(self, reason_code, message):
        self.reason_code = reason_code
        self.message = message

    def __repr__(self):  # pragma: no cover
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

    qs = parse_qs(parsed_url.query)
    local_file = qs.pop('local_file', (None,))[0]
    parsed_url = parsed_url._replace(query=urlencode(qs, True))

    if parsed_url.scheme == 'jenkins':
        return JenkinsS3Fetcher(parsed_url=parsed_url, local_file_hint=local_file, authenticated=authenticated)
    elif parsed_url.scheme == 'schemabackup':
        return SchemaBackupS3Fetcher(parsed_url=parsed_url, local_file_hint=local_file, authenticated=authenticated)
    elif parsed_url.scheme in ('http', 'https'):
        return HTTPFetcher(parsed_url=parsed_url, local_file_hint=local_file)
    elif parsed_url.scheme == 's3':
        return S3Fetcher(parsed_url=parsed_url, local_file_hint=local_file, authenticated=authenticated)
    elif parsed_url.scheme == 's3prefix':
        return PrefixS3Fetcher(parsed_url=parsed_url, local_file_hint=local_file, authenticated=authenticated)
    elif parsed_url.scheme == 'file' or not parsed_url.scheme:
        return LocalFileFetcher(parsed_url=parsed_url, local_file_hint=local_file)
    else:
        raise InvalidArtifactError("unable to find a fetcher for artifact '{artifact}'".format(artifact=artifact))


def fetcher_downloader(cache_dir=None):
    """Factory to return a AbstractFetcherDownloader subclass based on whether

    :param cache_dir: optional cache directory which, if set, triggers the creation of a caching downloader
    :return: AbstractFetcherDownloader subclass
    """
    return FetcherCachingDownloader(cache_dir=cache_dir) if cache_dir else FetcherDirectDownloader()


def download_file(artifact, local_file=None, authenticated=False, cache_dir=None):
    """Helper function to handle the most common use case

    :param artifact: artifact URL string
    :param local_file: local file path
    :param authenticated: control whether boto3 client is anonymous or authenticated
    :param cache_dir: optional cache dir
    :return: dict containing information about the actual (resolved) URL and the local file path of the downloaded file
    """
    fetcher_ = fetcher(artifact, authenticated)
    downloader = fetcher_downloader(cache_dir)

    # the local filename will be determined using the following order of precedence:
    # local_file function parameter > local_file query string parameter from URL > basename of the remote file
    if local_file is None:
        local_file = fetcher_.local_file_hint if fetcher_.local_file_hint else os.path.basename(fetcher_.real_url)

    with open(local_file, 'wb') as f:
        shutil.copyfileobj(downloader.get_handle(fetcher_), f)

    return {
        'local_file': local_file,
        'real_url': fetcher_.real_url
    }


def get_file_hash(filepath):
    """Get the SHA256 hash value (hexdigest) of a file

    :param filepath: path to the file being hashed
    :return: SHA256 hash of the file
    """
    if os.path.getsize(filepath) == 0:
        raise ValueError("not hashing zero length file '{filepath}".format(filepath=filepath))

    hasher = sha256()
    with open(filepath, 'rb') as f:
        for block in iter(partial(f.read, 65536), b''):
            hasher.update(block)
    return hasher.hexdigest()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:  # pragma: no cover
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def _paginate(method, **kwargs):
    client = method.__self__
    paginator = client.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for item in page:
            yield item


class AbstractFetcherDownloader(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(AbstractFetcherDownloader, self).__init__()
        LOGGER.info("creating FetcherDownloader of type '{t}'".format(t=self.__class__.__name__))

    @abc.abstractmethod
    def get_handle(self, file_fetcher):  # pragma: no cover
        pass


class CachedFile(object):
    def __init__(self, url, unique_id, real_url, file_hash):
        self.url = url

        self.unique_id = unique_id
        self.real_url = real_url
        self.file_hash = file_hash

    def __repr__(self):  # pragma: no cover
        return ("{self.__class__.__name__}(url='{self.url}', unique_id='{self.unique_id}', "
                "real_url='{self.real_url}', file_hash='{self.file_hash}')").format(self=self)

    def __iter__(self):  # pragma: no cover
        for attr in ('url', 'unique_id', 'real_url', 'file_hash'):
            yield attr, getattr(self, attr)

    def __eq__(self, other):
        if not isinstance(other, CachedFile):
            return False
        return (self.url == other.url and
                self.unique_id == other.unique_id and
                self.real_url == other.real_url)

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def from_dict(cls, dict_):
        try:
            return cls(**dict_) if dict_ else None
        except TypeError:
            raise InvalidCacheEntryError("invalid cache entry dict '{dict_}'".format(dict_=dict_))

    @classmethod
    def from_fetcher(cls, fetcher_, file_hash=None):
        return cls(fetcher_.url, fetcher_.unique_id, fetcher_.real_url, file_hash)


def _rename_sync(src_path, dest_path):
    def _sync_dir(path):
        dirfd = None
        try:
            dirfd = os.open(os.path.dirname(path), os.O_RDONLY)
            os.fsync(dirfd)
        finally:
            if dirfd:
                os.close(dirfd)

    os.rename(src_path, dest_path)
    _sync_dir(dest_path)


class FetcherCachingDownloader(AbstractFetcherDownloader):
    """Abstracts the interactions with a cache directory comprising of a JSON index file, and file objects corresponding
    to remote files encapsulated by Fetcher instances.

    Files are stored based on the sha256 hash of their contents in order to deduplicate cache storage, and cache
    validation is performed using an implementation-specific Fetcher 'unique_id' attribute to identify stale files (e.g.
    using Etags, or some other way to determine a persistent ID for an unchanged file on the remote server).
    """

    def __init__(self, cache_dir, cache_index_file='cacheindex.json', cache_index_lockfile='cacheindex.lock'):
        super(FetcherCachingDownloader, self).__init__()

        self.cache_dir = cache_dir
        self.cache_blob_dir = os.path.join(cache_dir, 'blobs')
        self.cache_index_file = os.path.join(cache_dir, cache_index_file)
        self.cache_index_lockfile = os.path.join(cache_dir, cache_index_lockfile)

        if os.path.exists(self.cache_blob_dir):
            self._prune_cache()

        mkdir_p(self.cache_blob_dir)

    @property
    def index(self):
        try:
            with open(self.cache_index_file, 'r') as f:
                index = json.load(f)
        except (IOError, ValueError):
            index = {}
        return index

    def get_handle(self, file_fetcher):
        blob_path = self._ensure_cached(file_fetcher)
        return open(blob_path, mode='rb')

    def _get_cached_file(self, file_fetcher):
        entry = self.index.get(CachedFile.from_fetcher(file_fetcher).url, {})
        return CachedFile.from_dict(entry)

    def _get_blob_path(self, cached_file):
        return None if cached_file.file_hash is None else os.path.join(self.cache_blob_dir, cached_file.file_hash)

    def _ensure_cached(self, file_fetcher):
        cached_file = self._get_cached_file(file_fetcher)
        if cached_file and cached_file.unique_id and file_fetcher.unique_id == cached_file.unique_id:
            blob_path = self._get_blob_path(cached_file)
            if os.path.exists(blob_path):
                LOGGER.info("'{artifact}' is current, using cached file".format(artifact=file_fetcher.url))
                return blob_path
        elif cached_file and not cached_file.unique_id:
            LOGGER.info("'{artifact}' has no unique identifier, must re-download".format(
                artifact=file_fetcher.url))
            cached_file = None
        elif cached_file:
            LOGGER.info("'{artifact}' is stale, updating cache".format(artifact=file_fetcher.url))
            cached_file = None
        else:
            LOGGER.info("'{artifact}' is missing, adding to cache".format(artifact=file_fetcher.url))

        blob_path = self._update_cache(file_fetcher, cached_file)

        return blob_path

    def _prune_cache(self):
        LOGGER.info("pruning cache")

        # prune entries with a broken blob reference from the index
        blobs_in_use = set()
        with InterProcessLock(self.cache_index_lockfile):
            new_index = self.index.copy()
            for url, entry in self.index.items():
                try:
                    cached_file = CachedFile.from_dict(entry)
                except InvalidCacheEntryError:
                    LOGGER.info("invalid cache entry for url '{}', pruning index entry".format(url))
                    new_index.pop(url)
                    continue

                blob_path = self._get_blob_path(cached_file)
                if not os.path.exists(blob_path):
                    LOGGER.info("blob missing for url '{}', pruning index entry".format(url))
                    new_index.pop(url)
                blobs_in_use.add(blob_path)

            with open(self.cache_index_file, 'w') as f:
                json.dump(new_index, f, indent=2, sort_keys=True)

        all_blobs = {os.path.join(self.cache_blob_dir, b) for b in os.listdir(self.cache_blob_dir)}

        # prune orphaned blobs from the cache directory
        orphaned_blobs = all_blobs.difference(blobs_in_use)
        for blob in orphaned_blobs:
            LOGGER.info("index entry missing for blob '{}', pruning blob".format(blob))
            os.unlink(blob)

        # prune unknown files from cache directory
        all_toplevel_files = {os.path.join(self.cache_dir, e) for e in os.listdir(self.cache_dir)}
        expected_toplevel_files = {self.cache_blob_dir, self.cache_index_file, self.cache_index_lockfile}
        unknown_toplevel_files = all_toplevel_files.difference(expected_toplevel_files)
        for file_ in unknown_toplevel_files:
            LOGGER.info("unexpected file '{file_}' found in cache dir, deleting".format(file_=file_))
            try:
                os.unlink(file_)
            except OSError as e:
                if e.errno == errno.EISDIR:
                    shutil.rmtree(file_)

    def _update_cache(self, file_fetcher, cached_file):
        if not cached_file:
            cached_file = CachedFile.from_fetcher(file_fetcher)

        with tempfile.NamedTemporaryFile(prefix=os.path.basename(cached_file.real_url), dir=self.cache_dir,
                                         delete=False) as t:
            shutil.copyfileobj(file_fetcher.handle, t)
            t.flush()
            os.fsync(t.fileno())

            cached_file.file_hash = get_file_hash(t.name)
            blob_path = self._get_blob_path(cached_file)

            if os.path.exists(blob_path):
                LOGGER.info("blob '{}' already cached".format(blob_path))
                os.remove(t.name)
            else:
                LOGGER.info("adding blob '{}' to cache".format(blob_path))
                _rename_sync(t.name, blob_path)

            self._update_index(cached_file)

        return blob_path

    def _update_index(self, cached_file):
        with InterProcessLock(self.cache_index_lockfile):
            index = dict(self.index)
            index[cached_file.url] = dict(cached_file)
            with open(self.cache_index_file, 'w') as f:
                json.dump(index, f, indent=2, sort_keys=True)


class FetcherDirectDownloader(AbstractFetcherDownloader):
    def get_handle(self, file_fetcher):
        return file_fetcher.handle


class AbstractFileFetcher(object):
    """Abstract class for Fetcher classes to define the interface used by the FetcherDownloader
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, parsed_url, local_file_hint=None):
        self.parsed_url = parsed_url
        self.local_file_hint = local_file_hint

        self._handle = None

    def get_value_from_query_string(self, param, default=None):
        """Retrieve a value from the query string

        :param param: parameter to retrieve
        :param default: value to return if the parameter is not present
        :return: parameter value or default value
        """
        try:
            return parse_qs(self.parsed_url.query)[param][0]
        except (IndexError, KeyError):
            return default

    @property
    def url(self):
        return self.parsed_url.geturl()

    @property
    def real_url(self):
        return self.url

    @abc.abstractproperty
    def handle(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def unique_id(self):  # pragma: no cover
        pass


class HTTPFetcher(AbstractFileFetcher):
    """Fetch from a regular HTTP URL, using Etag header (if available) to provide identifier for cache validation
    """

    def __init__(self, parsed_url, local_file_hint=None):
        super(HTTPFetcher, self).__init__(parsed_url, local_file_hint)

        self.path = parsed_url.path
        self._stream = None

    @property
    def response(self):
        if self._stream is None:
            r = requests.get(self.real_url, stream=True)
            r.raise_for_status()
            self._stream = r
        return self._stream

    @property
    def handle(self):
        if self._handle is None:
            self._handle = BytesIO(self.response.content)
        return self._handle

    @property
    def unique_id(self):
        return self.response.headers.get('ETag')


class LocalFileFetcher(AbstractFileFetcher):
    """Fetch from a local file path, using the sha256 sum of the file to provide identifier for cache validation
    """

    def __init__(self, parsed_url, local_file_hint=None):
        super(LocalFileFetcher, self).__init__(parsed_url, local_file_hint)

        if parsed_url.netloc:
            path = os.path.join(os.path.abspath(parsed_url.netloc), os.path.relpath(parsed_url.path.lstrip('/')))
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
        return get_file_hash(self.path)


class S3Fetcher(AbstractFileFetcher):
    """Fetch from an S3 URL, using Etag header to provide identifier for cache validation
    """

    def __init__(self, parsed_url, local_file_hint=None, authenticated=False, s3_client=None):
        super(S3Fetcher, self).__init__(parsed_url, local_file_hint)

        self.bucket = parsed_url.netloc
        self.path = parsed_url.path.lstrip('/')

        self.s3_client = s3_client or self.get_client(authenticated=authenticated)

        self._object = None

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
        if authenticated:  # pragma: no cover
            LOGGER.info('creating authenticated S3 client')
        else:
            LOGGER.info('creating anonymous S3 client')
            s3_client_kwargs['config'] = botocore.config.Config(signature_version=botocore.UNSIGNED)
        return boto3.client('s3', **s3_client_kwargs)


class BaseResolvingS3Fetcher(AbstractFileFetcher):
    def __init__(self, parsed_url, local_file_hint=None, authenticated=False):
        super(BaseResolvingS3Fetcher, self).__init__(parsed_url, local_file_hint)
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
        return self.real_parsed_url.geturl()

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
    def _get_key(self):  # pragma: no cover
        pass


class JenkinsS3Fetcher(BaseResolvingS3Fetcher):
    """Fetch from a Jenkins managed S3 artifact bucket, resolving the latest artifact for the given job, and using Etag
        header to provide identifier for cache validation
    """
    key_parse_pattern = re.compile(r"^jobs/(?P<job_name>[^/]+)/(?P<build_number>[^/]+)/(?P<basename>.*)$")

    def __init__(self, parsed_url, local_file_hint=None, authenticated=False):
        super(JenkinsS3Fetcher, self).__init__(parsed_url, local_file_hint, authenticated)

        self.job_name = parsed_url.path.lstrip('/')

        self._all_builds = None
        self._filename_pattern = None

    @property
    def all_builds(self):
        if self._all_builds is None:
            self._all_builds = [k for k in _paginate(self.s3_client.list_objects_v2, Bucket=self.bucket,
                                                     Prefix="jobs/{}".format(self.job_name))]
        return self._all_builds

    @property
    def filename_pattern(self):
        if self._filename_pattern is None:
            self._filename_pattern = self.get_value_from_query_string('pattern', r'^.*\.war$')
        return self._filename_pattern

    def _get_key(self):
        if not self.all_builds:
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
        matching_keys = (self.key_parse_pattern.match(a['Key']).groupdict() for a in self.all_builds if
                         re.match(self.filename_pattern, a['Key']))
        sorted_keys = sorted(matching_keys, key=lambda p: int(p['build_number']))
        return sorted_keys


class PrefixS3Fetcher(BaseResolvingS3Fetcher):
    """Fetch the latest artifact under an s3 prefix, using Etag header to provide identifier for cache validation """

    def __init__(self, parsed_url, local_file_hint=None, authenticated=False):
        super(PrefixS3Fetcher, self).__init__(parsed_url, local_file_hint, authenticated)

        self.prefix = parsed_url.path.lstrip('/')

        self._all_builds = None
        self._filename_pattern = None
        self._sortmethod = None

    @property
    def all_builds(self):
        if self._all_builds is None:
            self._all_builds = [k for k in _paginate(self.s3_client.list_objects_v2, Bucket=self.bucket,
                                                     Prefix=self.prefix)]
        return self._all_builds

    @property
    def filename_pattern(self):
        if self._filename_pattern is None:
            self._filename_pattern = self.get_value_from_query_string('pattern', r'^.*\.war$')
        return self._filename_pattern

    @property
    def sortmethod(self):
        if self._sortmethod is None:
            method_string = self.get_value_from_query_string('sortmethod', 'newest')
            if method_string == 'newest':
                self._sortmethod = lambda p: p['LastModified']
            elif method_string == 'version':
                self._sortmethod = lambda p: version.parse(p['Key'])
            else:
                raise ValueError("No such sort method '{method_string}".format(method_string=method_string))
        return self._sortmethod

    def _get_key(self):
        if not self.all_builds:
            raise KeyResolutionError('NO_RESULTS',
                                     "prefix '{s.prefix}' was invalid or returned no artifacts".format(s=self))

        try:
            return self._get_latest_matching_key()
        except IndexError:
            raise KeyResolutionError('NO_MATCHING_KEYS',
                                     "no keys found for '{s.prefix}' matching '{s.filename_pattern}'".format(
                                         s=self))

    def _get_latest_matching_key(self):
        matching_keys = (a for a in self.all_builds if
                         re.match(self.filename_pattern, a['Key']))
        sorted_keys = sorted(matching_keys, key=self.sortmethod)
        return sorted_keys[-1]['Key']


class SchemaBackupS3Fetcher(BaseResolvingS3Fetcher):
    def __init__(self, parsed_url, local_file_hint=None, authenticated=True):
        super(SchemaBackupS3Fetcher, self).__init__(parsed_url, local_file_hint, authenticated)

        components = parsed_url.path.lstrip('/').split('/')
        if len(components) != 3:
            raise ValueError('URL must be in the format: schemabackup://bucket/host/database/schema')
        self.host, self.database, self.schema = components

        self._timestamp = None

    @property
    def timestamp(self):
        if self._timestamp is None:
            self._timestamp = self.get_value_from_query_string('timestamp', default='LATEST')
        return self._timestamp

    def _get_key(self):
        host_prefix_components = ['backups', '']
        host_prefix = posixpath.join(*host_prefix_components)

        host_response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=host_prefix, Delimiter='/')

        all_hosts = [posixpath.relpath(c['Prefix'], host_prefix)
                     for c in host_response.get('CommonPrefixes', [])
                     if c['Prefix'].startswith(host_prefix)]

        if self.host not in all_hosts:
            raise KeyResolutionError('HOST_NOT_FOUND',
                                     "host '{h}' not found in bucket '{b}'.".format(h=self.host, b=self.bucket))

        base_prefix = posixpath.join(host_prefix, self.host, 'pgsql', '')

        timestamps_response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=base_prefix, Delimiter='/')

        all_timestamps = sorted(posixpath.relpath(c['Prefix'], base_prefix)
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
        key_name = posixpath.join(base_prefix, *key_components)

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
