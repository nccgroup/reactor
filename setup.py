import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as fh:
    install_requirements = fh.read().split()
setuptools.setup(
    name='reactor',
    version='1.0.5',
    author='Peter Scopes',
    author_email='peter.scopes@nccgroup.com',
    maintainer='Peter Scopes',
    maintainer_email='peter.scopes@nccgroup.com',
    description='Runs custom filters on Elasticsearch and alerts on matches',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['alerting', 'alerts', 'elasticsearch', 'SIEM', 'scalable', 'reliable', 'modular'],
    url='https://nccgroup.com/',
    setup_requires='setuptools',
    packages=['reactor'],
    exclude_package_data={},
    license='Copyright 2019 NCC Group',
    platforms=['OS Independent'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Environment :: Console :: Curses',
        'Intended Audience :: Information Technology',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Security',
    ],
    entry_points={
        'console_scripts': ['reactor=reactor.__main__:main']},
    package_data={'reactor': ['schemas/*.yaml', 'mappings/**/*.json']},
    python_requires='>=3.6',
    install_requires=install_requirements,
)
