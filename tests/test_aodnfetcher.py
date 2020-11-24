import errno
import os
import shutil
import tempfile
import unittest
from datetime import datetime

import pytest
import botocore.exceptions

try:
    import mock
except ImportError:
    from unittest import mock

import aodnfetcher
import aodnfetcher.fetcherlib


class _TemporaryDirectory(object):  # pragma: no cover
    """Context manager for :py:function:`tempfile.mkdtemp` (available in core library in v3.2+).
    """

    def __init__(self, suffix="", prefix=None, dir=None):
        self._closed = False
        self.name = None

        dir_prefix = prefix if prefix else self.__class__.__name__
        self.name = tempfile.mkdtemp(suffix=suffix, prefix=dir_prefix, dir=dir)

        self._rmtree = shutil.rmtree

    def __del__(self):
        self.cleanup()

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def __repr__(self):  # pragma: no cover
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def cleanup(self):
        try:
            self._rmtree(self.name)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass  # pragma: no cover


try:
    TemporaryDirectory = tempfile.TemporaryDirectory
except AttributeError:
    TemporaryDirectory = _TemporaryDirectory


def get_mocked_s3_fetcher(url):
    with mock.patch('aodnfetcher.fetcherlib.boto3'):
        fetcher = aodnfetcher.fetcher(url)
    return fetcher


class TestFetcherLib(unittest.TestCase):
    def setUp(self):
        self.mock_content = b'mock content'
        self.mock_file = mock.mock_open(read_data=self.mock_content)

    @mock.patch('aodnfetcher.fetcherlib.os')
    def test_caching_downloader(self, mock_os):
        mock_os.path.exists.return_value = False
        downloader = aodnfetcher.fetcher_downloader('cache_dir')
        self.assertIsInstance(downloader, aodnfetcher.fetcherlib.FetcherCachingDownloader)

    def test_direct_downloader(self):
        downloader = aodnfetcher.fetcher_downloader()
        self.assertIsInstance(downloader, aodnfetcher.fetcherlib.FetcherDirectDownloader)

    @mock.patch('aodnfetcher.fetcherlib.requests')
    def test_http_scheme(self, mock_requests):
        fetcher = aodnfetcher.fetcher('http://www.example.com')
        self.assertIsInstance(fetcher, aodnfetcher.fetcherlib.HTTPFetcher)

    @mock.patch('aodnfetcher.fetcherlib.requests')
    def test_https_scheme(self, mock_requests):
        fetcher = aodnfetcher.fetcher('https://www.example.com')
        self.assertIsInstance(fetcher, aodnfetcher.fetcherlib.HTTPFetcher)

    def test_local_scheme(self):
        expected_relative_path = os.path.join(os.getcwd(), os.path.relpath('path/to/file'))

        absolute_fetcher = aodnfetcher.fetcher('/path/to/file')
        self.assertIsInstance(absolute_fetcher, aodnfetcher.fetcherlib.LocalFileFetcher)
        self.assertEqual(absolute_fetcher.path, '/path/to/file')

        relative_fetcher = aodnfetcher.fetcher(os.path.relpath('path/to/file'))
        self.assertIsInstance(relative_fetcher, aodnfetcher.fetcherlib.LocalFileFetcher)
        self.assertEqual(relative_fetcher.path, expected_relative_path)

        scheme_fetcher = aodnfetcher.fetcher('file://path/to/file')
        self.assertIsInstance(scheme_fetcher, aodnfetcher.fetcherlib.LocalFileFetcher)
        self.assertEqual(scheme_fetcher.path, expected_relative_path)

    @mock.patch('aodnfetcher.fetcherlib.boto3')
    @mock.patch('aodnfetcher.fetcherlib.botocore')
    def test_jenkins_scheme(self, mock_botocore, mock_boto3):
        fetcher = aodnfetcher.fetcher('jenkins://bucket/job')
        self.assertIsInstance(fetcher, aodnfetcher.fetcherlib.JenkinsS3Fetcher)
        self.assertEqual(fetcher.bucket, 'bucket')
        self.assertEqual(fetcher.job_name, 'job')

    @mock.patch('aodnfetcher.fetcherlib.boto3')
    @mock.patch('aodnfetcher.fetcherlib.botocore')
    def test_schemabackup_scheme(self, mock_botocore, mock_boto3):
        fetcher = aodnfetcher.fetcher('schemabackup://bucket/host/database/schema')
        self.assertIsInstance(fetcher, aodnfetcher.fetcherlib.SchemaBackupS3Fetcher)
        self.assertEqual(fetcher.bucket, 'bucket')
        self.assertEqual(fetcher.host, 'host')
        self.assertEqual(fetcher.database, 'database')
        self.assertEqual(fetcher.schema, 'schema')

    @mock.patch('aodnfetcher.fetcherlib.boto3')
    @mock.patch('aodnfetcher.fetcherlib.botocore')
    def test_s3_scheme(self, mock_botocore, mock_boto3):
        fetcher = aodnfetcher.fetcher('s3://bucket/key/path')
        self.assertIsInstance(fetcher, aodnfetcher.fetcherlib.S3Fetcher)
        self.assertEqual(fetcher.bucket, 'bucket')
        self.assertEqual(fetcher.path, 'key/path')

    def test_invalid_scheme(self):
        with self.assertRaises(aodnfetcher.fetcherlib.InvalidArtifactError):
            _ = aodnfetcher.fetcher('invalidscheme://invalid/scheme')

    def test_download_file_with_original_name(self):
        with mock.patch('aodnfetcher.fetcherlib.open', self.mock_file) as m:
            result = aodnfetcher.download_file('file://path/to/original_name')

        m().write.assert_called_with(self.mock_content)
        self.assertEqual(result['local_file'], 'original_name')

    def test_download_file_with_alternate_name(self):
        with mock.patch('aodnfetcher.fetcherlib.open', self.mock_file) as m:
            result = aodnfetcher.download_file('file://path/to/original_name', local_file='alternate_name')

        m().write.assert_called_with(self.mock_content)
        self.assertEqual(result['local_file'], 'alternate_name')

    def test_download_file_with_alternate_name_from_url(self):
        with mock.patch('aodnfetcher.fetcherlib.open', self.mock_file) as m:
            result = aodnfetcher.download_file('file://path/to/original_name?local_file=alternate_name')

        m().write.assert_called_with(self.mock_content)
        self.assertEqual(result['local_file'], 'alternate_name')

    def test_download_file_cache_same_filesystem(self):
        old_wd = os.getcwd()
        with TemporaryDirectory() as d:
            os.chdir(d)
            try:
                cache_dir = os.path.join(d, 'cache')
                source_file = os.path.join(d, 'source.txt')
                os.mkdir(cache_dir)
                with open(source_file, 'w') as f:
                    f.write('dummy_content')

                _ = aodnfetcher.download_file(source_file, local_file='dest.txt', cache_dir=cache_dir)

                cached_file_path = aodnfetcher.fetcher_downloader(
                    cache_dir=cache_dir).get_cache_path(aodnfetcher.fetcher(source_file))

                source_file_inode = os.stat(source_file).st_ino
                cached_file_inode = os.stat(cached_file_path).st_ino
                dest_file_inode = os.stat('dest.txt').st_ino

                self.assertEqual(cached_file_inode, dest_file_inode)  # file *is* a hard link to the dest file
                self.assertNotEqual(source_file_inode, dest_file_inode)
            finally:
                os.chdir(old_wd)

    def test_download_file_cache_different_filesystem(self):
        old_wd = os.getcwd()
        with TemporaryDirectory() as d, TemporaryDirectory(dir='/dev/shm') as e:
            os.chdir(d)
            try:
                cache_dir = os.path.join(e, 'cache')
                source_file = os.path.join(d, 'source.txt')
                os.mkdir(cache_dir)
                with open(source_file, 'w') as f:
                    f.write('dummy_content')

                _ = aodnfetcher.download_file(source_file, local_file='dest.txt', cache_dir=cache_dir)

                cached_file_path = aodnfetcher.fetcher_downloader(
                    cache_dir=cache_dir).get_cache_path(aodnfetcher.fetcher(source_file))

                source_file_inode = os.stat(source_file).st_ino
                cached_file_inode = os.stat(cached_file_path).st_ino
                dest_file_inode = os.stat('dest.txt').st_ino

                self.assertNotEqual(cached_file_inode, dest_file_inode)  # file is *not* a hard link to the dest file
                self.assertNotEqual(source_file_inode, dest_file_inode)
            finally:
                os.chdir(old_wd)


class TestCachedFile(unittest.TestCase):
    def test_equality_equal(self):
        object1 = aodnfetcher.fetcherlib.CachedFile('file://test/file', None, 'file://test/file', None)
        object2 = aodnfetcher.fetcherlib.CachedFile('file://test/file', None, 'file://test/file', None)

        self.assertIsNot(object1, object2)
        self.assertEqual(object1, object2)

    def test_equality_not_equal(self):
        object1 = aodnfetcher.fetcherlib.CachedFile('http://www.example.com', None, 'http://www.example.com', None)
        object2 = aodnfetcher.fetcherlib.CachedFile('file://test/file', None, 'file://test/file', None)

        self.assertIsNot(object1, object2)
        self.assertNotEqual(object1, object2)

    def test_equality_other_type(self):
        object1 = aodnfetcher.fetcherlib.CachedFile('http://www.example.com', None, 'http://www.example.com', None)
        object2 = 'DIFFERENT_TYPE'

        self.assertIsNot(object1, object2)
        self.assertNotEqual(object1, object2)

    def test_from_dict_empty(self):
        from_none = aodnfetcher.fetcherlib.CachedFile.from_dict({})
        self.assertIsNone(from_none)

    def test_from_dict_valid(self):
        input_dict = {
            'url': 'file://test/file',
            'unique_id': None,
            'real_url': 'file://test/file',
            'file_hash': None
        }

        expected_object = aodnfetcher.fetcherlib.CachedFile('file://test/file', None,
                                                            'file://test/file', None)
        from_dict = aodnfetcher.fetcherlib.CachedFile.from_dict(input_dict)

        self.assertEqual(expected_object, from_dict)

    def test_from_dict_invalid(self):
        input_dict = {'invalid_key': ''}
        with self.assertRaises(aodnfetcher.fetcherlib.InvalidCacheEntryError):
            _ = aodnfetcher.fetcherlib.CachedFile.from_dict(input_dict)

    def test_from_fetcher(self):
        mock_file = mock.mock_open(read_data=b'mock content')

        fetcher = aodnfetcher.fetcherlib.fetcher('file:///tmp/test/file')
        with mock.patch('aodnfetcher.fetcherlib.open', mock_file), mock.patch('os.path.getsize', lambda p: 1):
            cached_file = aodnfetcher.fetcherlib.CachedFile.from_fetcher(fetcher)

        expected_object = aodnfetcher.fetcherlib.CachedFile('file:///tmp/test/file',
                                                            '05db393b05821f1a536ec7e7a4094abc67c6293b6489db31d70defcfa60f6a8a',
                                                            '/tmp/test/file',
                                                            None)

        self.assertEqual(cached_file, expected_object)


# TODO: write more tests for FetcherCachingDownloader
class TestFetcherCachingDownloader(unittest.TestCase):
    def test_get_cache_path(self):
        with TemporaryDirectory() as d:
            cache_dir = os.path.join(d, 'cache')
            source_file = os.path.join(d, 'source.txt')
            os.mkdir(cache_dir)
            with open(source_file, 'w') as f:
                f.write('dummy_content')

            fetcher = aodnfetcher.fetcher(source_file)
            downloader = aodnfetcher.fetcher_downloader(cache_dir=cache_dir)
            actual = downloader.get_cache_path(fetcher)
            self.assertEqual(
                os.path.join(d, 'cache/blobs/807ac90e2ae393e32b4562a81d158a190eb4b26dd021713b82b31b1b457f3d59'), actual)

    def test_get_handle(self):
        with TemporaryDirectory() as d:
            cache_dir = os.path.join(d, 'cache')
            source_file = os.path.join(d, 'source.txt')
            os.mkdir(cache_dir)
            with open(source_file, 'wb') as f:
                f.write(b'dummy_content')

            direct_fetcher = aodnfetcher.fetcher(source_file)

            try:
                direct_content = direct_fetcher.handle.read()
            finally:
                direct_fetcher.handle.close()

            cached_fetcher = aodnfetcher.fetcher(source_file)
            downloader = aodnfetcher.fetcher_downloader(cache_dir=cache_dir)

            with pytest.deprecated_call():
                cached_handle = downloader.get_handle(cached_fetcher)

            try:
                cached_content = cached_handle.read()
            finally:
                cached_handle.close()

            self.assertEqual(direct_content, cached_content)


class TestFetcherDirectDownloader(unittest.TestCase):
    def test_get_handle(self):
        fetcher = aodnfetcher.fetcher('path/to/file')
        downloader = aodnfetcher.fetcher_downloader()
        with mock.patch('aodnfetcher.fetcherlib.open', mock.mock_open()), pytest.deprecated_call():
            self.assertEqual(fetcher.handle, downloader.get_handle(fetcher))

    def test_open(self):
        downloader = aodnfetcher.fetcher_downloader()

        with TemporaryDirectory() as d:
            temp_file = os.path.join(d, 'source.txt')
            fetcher = aodnfetcher.fetcher(temp_file)

            with open(temp_file, 'wb') as f:
                f.write(b'dummy content')

            with downloader.open(fetcher) as f:
                self.assertFalse(f.closed)
                self.assertEqual(b'dummy content', f.read())
            self.assertTrue(f.closed)


class TestHTTPFetcher(unittest.TestCase):
    def setUp(self):
        self.url = 'http://www.example.com'
        self.fetcher = aodnfetcher.fetcherlib.HTTPFetcher(aodnfetcher.fetcherlib.urlparse(self.url))
        self.mock_content = b'mock content'
        self.mock_etag = 'abc123'

    @mock.patch('aodnfetcher.fetcherlib.requests')
    def test_handle(self, mock_requests):
        mock_requests.get().content = self.mock_content
        content = self.fetcher.handle.read()
        self.assertEqual(content, self.mock_content)

    def test_real_url(self):
        self.assertEqual(self.fetcher.real_url, self.url)

    @mock.patch('aodnfetcher.fetcherlib.requests')
    def test_unique_id(self, mock_requests):
        mock_requests.get().headers = {'ETag': self.mock_etag}
        unique_id = self.fetcher.unique_id
        self.assertEqual(unique_id, self.mock_etag)


class TestLocalFileFetcher(unittest.TestCase):
    def setUp(self):
        self.url = 'file://test/file'
        self.fetcher = aodnfetcher.fetcherlib.LocalFileFetcher(aodnfetcher.fetcherlib.urlparse(self.url))
        self.mock_content = b'mock content'
        self.mock_file = mock.mock_open(read_data=self.mock_content)

    def test_handle(self):
        with mock.patch('aodnfetcher.fetcherlib.open', self.mock_file):
            handle = self.fetcher.handle
        content = handle.read()
        self.assertEqual(content, self.mock_content)

    def test_real_url(self):
        self.assertEqual(self.fetcher.real_url, self.fetcher.path)

    def test_unique_id(self):
        mock_content_checksum = '05db393b05821f1a536ec7e7a4094abc67c6293b6489db31d70defcfa60f6a8a'

        with mock.patch('aodnfetcher.fetcherlib.open', self.mock_file), mock.patch('os.path.getsize', lambda p: 1):
            unique_id = self.fetcher.unique_id
        self.assertEqual(unique_id, mock_content_checksum)


class TestS3Fetcher(unittest.TestCase):
    def setUp(self):
        self.url = 's3://bucket/key/path'
        self.fetcher = get_mocked_s3_fetcher(self.url)

        self.mock_content = b'mock content'
        self.mock_etag = 'abc123'
        mock_body = mock.MagicMock()
        mock_body.read.return_value = self.mock_content
        self.fetcher.s3_client.get_object.return_value = {
            'Body': mock_body,
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'etag': self.mock_etag
                }
            }
        }

    def test_handle(self):
        content = self.fetcher.handle.read()
        self.assertEqual(content, self.mock_content)

    def test_real_url(self):
        self.assertEqual(self.fetcher.real_url, self.url)

    def test_unique_id(self):
        unique_id = self.fetcher.unique_id
        self.assertEqual(unique_id, self.mock_etag)

    def test_auth_failure(self):
        self.fetcher.s3_client.get_object.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'AuthorizationHeaderMalformed'}}, 'GetObject')
        with self.assertRaises(aodnfetcher.fetcherlib.AuthenticationError):
            _ = self.fetcher.object


class TestJenkinsS3Fetcher(unittest.TestCase):
    def setUp(self):
        self.url = 'jenkins://bucket/job'
        self.fetcher = get_mocked_s3_fetcher(self.url)

        self.mock_content = b'mock content'
        self.mock_etag = 'abc123'
        mock_body = mock.MagicMock()
        mock_body.read.return_value = self.mock_content

        self.fetcher.s3_client.get_object.return_value = {
            'Body': mock_body,
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'etag': self.mock_etag
                }
            }
        }

        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [{'Key': 'jobs/job/1/path1.war'}, {'Key': 'jobs/job/2/path2.war'}],
            [{'Key': 'jobs/job/3/path1.war'}, {'Key': 'jobs/job/4/path2.war'}]
        ]

        self.fetcher.s3_client.list_objects_v2.__self__ = self.fetcher.s3_client
        self.fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

    def test_handle(self):
        content = self.fetcher.handle.read()
        self.assertEqual(content, self.mock_content)

    def test_real_url(self):
        self.assertEqual(self.fetcher.real_url, 's3://bucket/jobs/job/4/path2.war')

    def test_unique_id(self):
        unique_id = self.fetcher.unique_id
        self.assertEqual(unique_id, self.mock_etag)

    def test_auth_failure(self):
        self.fetcher.s3_client.get_object.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'AuthorizationHeaderMalformed'}}, 'GetObject')
        with self.assertRaises(aodnfetcher.fetcherlib.AuthenticationError):
            _ = self.fetcher.object

    def test_no_builds(self):
        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = []

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = self.fetcher.object
        self.assertEqual(cm.exception.reason_code, 'NO_RESULTS')

    def test_no_matching_builds(self):
        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [{'Key': 'jobs/job/3/path3.txt'}]
        ]

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = self.fetcher.object
        self.assertEqual(cm.exception.reason_code, 'NO_MATCHING_BUILDS')

    def test_custom_jenkins_pattern(self):
        url = r'jenkins://bucket/job?pattern=^.*\.whl$'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'jobs/job/1/path1.war'},
                {'Key': 'jobs/job/2/path2.whl'}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/jobs/job/2/path2.whl')

    def test_custom_jenkins_pattern_to_local_file(self):
        url = r'jenkins://bucket/job?pattern=^.*\.whl$&local_file=custom_path.whl'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'jobs/job/1/path1.war'},
                {'Key': 'jobs/job/2/path2.whl'}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/jobs/job/2/path2.whl')
        self.assertEqual(fetcher.local_file_hint, 'custom_path.whl')


class TestPrefixS3Fetcher(unittest.TestCase):
    def setUp(self):
        self.url = 's3prefix://bucket/prefix_part_1/prefix_part_2'
        self.fetcher = get_mocked_s3_fetcher(self.url)

        self.mock_content = b'mock content'
        self.mock_etag = 'abc123'
        mock_body = mock.MagicMock()
        mock_body.read.return_value = self.mock_content

        self.fetcher.s3_client.get_object.return_value = {
            'Body': mock_body,
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'etag': self.mock_etag
                }
            }
        }

        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [{'Key': 'prefix/1/path1.war', 'LastModified': datetime(2011, 7, 29, 5, 41, 27)},
             {'Key': 'prefix/2/path2.war', 'LastModified': datetime(2012, 7, 29, 5, 41, 27)}
            ],
            [{'Key': 'prefix/3/path1.war', 'LastModified': datetime(2013, 7, 29, 5, 41, 27)},
             {'Key': 'prefix/4/path2.war', 'LastModified': datetime(2014, 7, 29, 5, 41, 27)}
            ]
        ]

        self.fetcher.s3_client.list_objects_v2.__self__ = self.fetcher.s3_client
        self.fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

    def test_handle(self):
        content = self.fetcher.handle.read()
        self.assertEqual(content, self.mock_content)

    def test_real_url(self):
        self.assertEqual(self.fetcher.real_url, 's3://bucket/prefix/4/path2.war')

    def test_unique_id(self):
        unique_id = self.fetcher.unique_id
        self.assertEqual(unique_id, self.mock_etag)

    def test_auth_failure(self):
        self.fetcher.s3_client.get_object.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'AuthorizationHeaderMalformed'}}, 'GetObject')
        with self.assertRaises(aodnfetcher.fetcherlib.AuthenticationError):
            _ = self.fetcher.object

    def test_no_builds(self):
        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = []

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = self.fetcher.object
        self.assertEqual(cm.exception.reason_code, 'NO_RESULTS')

    def test_no_matching_keys(self):
        self.fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [{'Key': 'prefix/3/path3.txt'}]
        ]

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = self.fetcher.object
        self.assertEqual(cm.exception.reason_code, 'NO_MATCHING_KEYS')

    def test_custom_filename_pattern(self):
        url = r's3prefix://bucket/prefix?pattern=^.*\.whl$'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'prefix/1/path1.war', 'LastModified': datetime(2020, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/2/path2.whl', 'LastModified': datetime(2019, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/3/path3.whl', 'LastModified': datetime(2018, 7, 29, 5, 41, 27)}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/prefix/2/path2.whl')

    def test_custom_filename_pattern_to_local_file(self):
        url = r's3prefix://bucket/job?pattern=^.*\.whl$&local_file=custom_path.whl'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'prefix/2/path2.whl', 'LastModified': datetime(2020, 7, 29, 5, 41, 27)}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/prefix/2/path2.whl')
        self.assertEqual(fetcher.local_file_hint, 'custom_path.whl')

    def test_version_sortmethod(self):
        url = 's3prefix://bucket/prefix?sortmethod=version'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'prefix/1/version1.war', 'LastModified': datetime(2020, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/2/version2.war', 'LastModified': datetime(2019, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/3/version3.war', 'LastModified': datetime(2018, 7, 29, 5, 41, 27)}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/prefix/3/version3.war')

    def test_newest_sortmethod(self):
        url = 's3prefix://bucket/prefix'
        fetcher = get_mocked_s3_fetcher(url)
        fetcher.s3_client.list_objects_v2.__self__ = fetcher.s3_client
        fetcher.s3_client.list_objects_v2.__name__ = 'list_objects_v2'

        fetcher.s3_client.get_paginator().paginate().result_key_iters.return_value = [
            [
                {'Key': 'prefix/1/version1.war', 'LastModified': datetime(2020, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/2/version2.war', 'LastModified': datetime(2019, 7, 29, 5, 41, 27)},
                {'Key': 'prefix/3/version3.war', 'LastModified': datetime(2018, 7, 29, 5, 41, 27)}
            ]
        ]

        self.assertEqual(fetcher.real_url, 's3://bucket/prefix/1/version1.war')


class TestSchemaBackupS3Fetcher(unittest.TestCase):
    def setUp(self):
        self.list_objects_side_effect = [
            # host query
            {
                'CommonPrefixes': [{'Prefix': 'backups/test-host/'},
                                   {'Prefix': 'backups/test-host-2/'}]
            },
            # timestamp query
            {
                'CommonPrefixes': [{'Prefix': 'backups/test-host/pgsql/2018.07.31.04.22.11/'},
                                   {'Prefix': 'backups/test-host/pgsql/2018.07.20.04.30.30/'},
                                   {'Prefix': 'backups/test-host/pgsql/2018.07.30.05.23.45/'}]
            }
        ]

    def test_latest_dump_explicit(self):
        url = 'schemabackup://test-bucket/test-host/test-database/test-schema?timestamp=LATEST'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        expected_url = 's3://test-bucket/backups/test-host/pgsql/2018.07.31.04.22.11/test-database/test-schema.dump'
        self.assertEqual(fetcher.real_url, expected_url)

    def test_latest_dump_implicit(self):
        url = 'schemabackup://test-bucket/test-host/test-database/test-schema'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        expected_url = 's3://test-bucket/backups/test-host/pgsql/2018.07.31.04.22.11/test-database/test-schema.dump'
        self.assertEqual(fetcher.real_url, expected_url)

    def test_with_invalid_host(self):
        url = 'schemabackup://test-bucket/invalid-test-host/test-database/test-schema?timestamp=2011.01.01.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = fetcher.object
        self.assertEqual(cm.exception.reason_code, 'HOST_NOT_FOUND')

    def test_with_timestamp(self):
        url = 'schemabackup://test-bucket/test-host/test-database/test-schema?timestamp=2018.07.20.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        expected_url = 's3://test-bucket/backups/test-host/pgsql/2018.07.20.04.30.30/test-database/test-schema.dump'
        self.assertEqual(fetcher.real_url, expected_url)

    def test_with_invalid_timestamp(self):
        url = 'schemabackup://test-bucket/test-host/test-database/test-schema?timestamp=2011.01.01.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = fetcher.object
        self.assertEqual(cm.exception.reason_code, 'TIMESTAMP_NOT_FOUND')

    def test_with_no_timestamps(self):
        url = 'schemabackup://test-bucket/test-host-2/test-database/test-schema?timestamp=2011.01.01.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        self.list_objects_side_effect[1] = {
            'CommonPrefixes': []
        }

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = fetcher.object
        self.assertEqual(cm.exception.reason_code, 'NO_TIMESTAMPS')

    def test_with_timestamp_missing_schema(self):
        url = 'schemabackup://test-bucket/test-host/test-database/dummy_schema?timestamp=2018.07.20.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect
        dummy_error = botocore.exceptions.ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        fetcher.s3_client.get_object.side_effect = dummy_error

        with self.assertRaises(aodnfetcher.fetcherlib.KeyResolutionError) as cm:
            _ = fetcher.object
        self.assertEqual(cm.exception.reason_code, 'SCHEMA_NOT_FOUND')

    def test_unhandled_botocore_error(self):
        url = 'schemabackup://test-bucket/test-host/test-database/dummy_schema?timestamp=2018.07.20.04.30.30'
        fetcher = get_mocked_s3_fetcher(url)

        fetcher.s3_client.list_objects_v2.side_effect = self.list_objects_side_effect
        dummy_error = botocore.exceptions.ClientError({'Error': {'Code': 'UnexpectedError'}}, 'GetObject')
        fetcher.s3_client.get_object.side_effect = dummy_error

        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            _ = fetcher.object
        self.assertIs(dummy_error, cm.exception)

    def test_invalid_url(self):
        url = 'schemabackup://test-bucket/test-schema?timestamp=2018.07.20.04.30.30'
        with self.assertRaises(ValueError):
            _ = get_mocked_s3_fetcher(url)
