from setuptools import setup

INSTALL_REQUIRES = [
        'boto3>=1.9.161',
        'fasteners==0.14.1',
        'requests>=2.22.0',
        'packaging'
    ]

TESTS_REQUIRE = [
    'mock',
    'pytest'
]

setup(
    name='aodnfetcher',
    version='0.6.8',
    packages=['aodnfetcher'],
    url='https://github.com/aodn',
    license='GPLv3',
    author='AODN',
    author_email='developers@emii.org.au',
    description='AODN artifact fetcher',
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require={'testing': TESTS_REQUIRE},
    entry_points={'console_scripts': ['aodnfetcher=aodnfetcher.cli:main']}
)
