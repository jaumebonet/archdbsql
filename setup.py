from setuptools import setup, find_packages

setup(
    name='archdbsql',
    version='v0.1',

    description='READ ONLY access to the ArchDB14 database',
    long_description='archdbsql allows to perform pre-stablished '
                     'queries to ArchDB14. Some of these queries can only '
                     'be performed on the actual database (they are those) '
                     'including external databases info. Those related with '
                     'Smotif to PDB relationships can be performed with the '
                     'minimized SQL dump accessible through ArchDB14 downloads.',

    # The project's main homepage.
    url='https://github.com/jaumebonet/archdbsql',

    # Author details
    author='Jaume Bonet',
    author_email='jaume.bonet@gmail.com',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],

    project_urls={
        'Source': 'https://github.com/jaumebonet/archdbsql',
        'Tracker': 'https://github.com/jaumebonet/archdbsql/issues',
    },

    platforms='UNIX',
    keywords='development',

    install_requires=[x.strip() for x in open('REQUIREMENTS').readlines()],

    packages=find_packages(exclude=['docs', 'demo', 'sphinx-docs']),
    include_package_data=True,
    package_data={
        'archdbsql': ['REQUIREMENTS'],
    },

    zip_safe=False,
)
