# coding: utf-8

from setuptools import setup, find_packages
from messagebus import __VERSION__

NAME = "kafka-message-bus"
VERSION = __VERSION__
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["confluent-kafka[avro]"]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=NAME,
    version=VERSION,
    description="Python Kafka Message Bus Library",
    author="Kata.ai Dev Team",
    author_email="dev@kata.ai",
    url="https://github.com/kata-ai/messagebus-kafka-python/",
    keywords=["Kafka", "Kafka Message Bus"],
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
                  "Programming Language :: Python :: 3",
                  "License :: OSI Approved :: MIT License",
                  "Operating System :: OS Independent",
              ],
    python_requires='>=3.6'
)
