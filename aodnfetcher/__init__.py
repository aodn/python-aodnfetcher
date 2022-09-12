from .fetcherlib import download_file, fetcher, fetcher_downloader

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"
