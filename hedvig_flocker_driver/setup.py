
from setuptools import setup, find_packages
from os import path

setup(
    name='hedvig_flocker_driver',
    version='1.0',
    description='Hedvig Backend Plugin for ClusterHQ/Flocker ',
    license='Apache 2.0',

    classifiers=[
    # Python versions supported 
    'Programming Language :: Python :: 2.7',
    ],

    keywords='backend, plugin, flocker, docker, python',
    packages=find_packages(exclude=['']),
)
