from setuptools import setup

INSTALL_REQUIRES = [
        'boto3>=1.9.161',
        'fasteners==0.14.1',
        'requests>=2.22.0',
        'packaging'
    ]

TESTS_REQUIRE = [
    'mock',
    'pytest',
    'setuptools_scm',
]

setup(
    name='aodnfetcher',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
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
