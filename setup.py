#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-googleanalytics",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_googleanalytics"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
        "oauth2client==4.1.3",
        "google-api-python-client==1.7.7"
    ],
    entry_points="""
    [console_scripts]
    tap-googleanalytics=tap_googleanalytics:main
    """,
    packages=["tap_googleanalytics"],
    package_data = {
        "schemas": ["tap_googleanalytics/schemas/*.json"]
    },
    include_package_data=True,
)
