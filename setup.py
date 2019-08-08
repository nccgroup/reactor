import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='reactor',
    version='0.1.1',
    author='Peter Scopes',
    author_email='peter.scopes@nccgroup.com',
    description='Runs custom filters on Elasticsearch and alerts on matches',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://nccgroup.com/',
    setup_requires='setuptools',
    packages=setuptools.find_packages(),
    license='Copyright 2019 NCC Group',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    entry_points={
        'console_scripts': ['reactor=reactor.__main__']},
    package_data={'reactor': ['schemas/*.yaml', 'mappings/**/*.json']},
    install_requires=[
        'apscheduler>=3.6.0',
        'croniter>=0.3.30',
        'elasticsearch>=6.0.0<7.0.0',
        'jsonschema>=3.0.0',
        'python-dateutil~=2.8.0',
        'PyYAML>=5.1.1',
        'requests>=2.0.0',
    ]
)
