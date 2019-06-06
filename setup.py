from setuptools import setup

INSTALL_REQUIRES = [
        'boto3>=1.9.156',
        'fasteners==0.14.1',
        'requests>=2.12.1,<2.20.0'
    ]

TESTS_REQUIRE = [
    'mock'
]

setup(
    name='aodnfetcher',
    version='0.5',
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
