import os

from setuptools import find_packages
from setuptools import setup

base_dir = os.path.dirname(__file__)

setup(
    name='Reactor',
    version='0.1.1',
    description='',
    author='Peter Scopes',
    author_email='peter.scopes@nccgroup.com',
    setup_requires='setuptools',
    license='Copyright 2019 NCC Group',
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    packages=find_packages(),
    package_data={'reactor': ['schemas/config.yaml', 'schemas/ruletype.yaml']},
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
