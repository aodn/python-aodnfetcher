from setuptools import setup

setup(
    name='aodnfetcher',
    version='0.2',
    packages=['aodnfetcher'],
    url='https://github.com/aodn',
    license='GPLv3',
    author='AODN',
    author_email='developers@emii.org.au',
    description='AODN artifact fetcher',
    zip_safe=False,
    install_requires=['boto3'],
    tests_require=['mock', 'codecov', 'coverage'],
    entry_points={'console_scripts': ['aodnfetcher=aodnfetcher.cli:main']}
)
