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
from urlparse import ParseResult, urlparse, urlunparse

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
    'JenkinsS3Fetcher'
]

LOGGER = logging.getLogger('aodnfetcher')


class AuthenticationError(Exception):
    pass


class InvalidArtifactError(Exception):
    pass


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
    def get_handle(self, fetcher_):
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

    def file_is_current(self, fetcher_):
        if not fetcher_.unique_id:
            return False
        cache_path = self._get_cache_path(fetcher_)
        return os.path.exists(cache_path) and fetcher_.unique_id == self.get_cached_object_id(fetcher_)

    def get_cached_object_id(self, fetcher_):
        cache_key = self.get_cache_key(fetcher_)
        obj = self.index.get(cache_key, {})
        return obj.get('id')

    @staticmethod
    def get_cache_key(fetcher_):
        return sha256(fetcher_.real_url).hexdigest()

    def get_handle(self, fetcher_):
        cache_path = self._get_cache_path(fetcher_)
        if self.file_is_current(fetcher_):
            LOGGER.info("'{artifact}' is current, using cached file".format(artifact=fetcher_.real_url))
        else:
            LOGGER.info("'{artifact}' is missing or stale, downloading".format(artifact=fetcher_.real_url))
            self._put_file(fetcher_)
        return open(cache_path, mode='rb')

    def _get_cache_path(self, fetcher_):
        return os.path.join(self.cache_dir, self.get_cache_key(fetcher_))

    def _put_file(self, fetcher_):
        cache_path = self._get_cache_path(fetcher_)
        with open(cache_path, 'wb') as out_file:
            copyfileobj(fetcher_.handle, out_file)
        self._update_index(fetcher_)

    def _update_index(self, fetcher_):
        cache_key = self.get_cache_key(fetcher_)
        index = dict(self.index)
        index[cache_key] = {'id': fetcher_.unique_id, 'url': fetcher_.real_url}
        with open(self.cache_index_file, 'w') as f:
            json.dump(index, f)


class FetcherDirectDownloader(AbstractFetcherDownloader):
    def get_handle(self, fetcher_):
        return fetcher_.handle


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


class JenkinsS3Fetcher(AbstractFileFetcher):
    """Fetch from a Jenkins managed S3 artifact bucket, resolving the latest artifact for the given job, and using Etag
        header to provide identifier for cache validation
    """
    key_parse_pattern = re.compile(r"^jobs/(?P<job_name>[^/]+)/(?P<build_number>[^/]+)/(?P<basename>.*)$")

    def __init__(self, parsed_url, filename_pattern='^.*\.war$', authenticated=False):
        super(JenkinsS3Fetcher, self).__init__(parsed_url)

        self.bucket = parsed_url.netloc
        self.job_name = parsed_url.path.lstrip('/')
        self.filename_pattern = filename_pattern

        self.s3_client = S3Fetcher.get_client(authenticated=authenticated)

        self._all_builds = None
        self._key = None
        self._object = None
        self._fetcher = None
        self._real_parse_result = None

    @property
    def all_builds(self):
        if self._all_builds is None:
            self._all_builds = self.s3_client.list_objects_v2(Bucket=self.bucket,
                                                              Prefix="jobs/{}".format(self.job_name))
        return self._all_builds

    @property
    def fetcher(self):
        """Due to the dynamic nature of this resolver, an inner S3Fetcher is used once the real object is known

        :return: S3Fetcher for the real object
        """
        if self._fetcher is None:
            self._fetcher = S3Fetcher(parsed_url=self.real_parsed_url, s3_client=self.s3_client)
        return self._fetcher

    @property
    def real_url(self):
        return urlunparse(self.real_parsed_url)

    @property
    def handle(self):
        if self._handle is None:
            self._handle = self.fetcher.handle
        return self._handle

    @property
    def path(self):
        """Unlike other Fetchers, path is a lazy property, because it is not known at the time of initialisation

        :return: dynamically determined key
        """
        if self._key is None:
            self._key = self._get_key()
        return self._key

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
    def object(self):
        return self.fetcher.object

    @property
    def unique_id(self):
        return self.object['ResponseMetadata']['HTTPHeaders']['etag']

    def _get_key(self):
        if not self.all_builds.get('Contents'):
            raise ValueError("job '{s.job_name}' was invalid or returned no builds".format(s=self))

        try:
            latest = self._get_matching_builds()[-1]
        except IndexError:
            raise ValueError("no builds found for '{s.job_name}' matching '{s.filename_pattern}'".format(s=self))
        return "jobs/{job_name}/{build_number}/{basename}".format(**latest)

    def _get_matching_builds(self):
        matching_keys = (self.key_parse_pattern.match(a['Key']).groupdict() for a in self.all_builds['Contents'] if
                         re.match(self.filename_pattern, a['Key']))
        sorted_keys = sorted(matching_keys, key=lambda p: int(p['build_number']))
        return sorted_keys
