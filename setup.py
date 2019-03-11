"""A setuptools based setup module.
See:
https://packaging.python.org/tutorials/packaging-projects/
"""

from codecs import open

from setuptools import setup, find_packages

from cloudstorageio import __name__ as package_name
from cloudstorageio import __version__ as package_version

# Get the long description from the README file
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=package_name,
    version=package_version,
    author="Vahagn Ghazaryan",
    author_email="vahagn.ghazayan@gmail.com",
    description="Tool working with storage interfaces",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/VahagnGhaz/cloudstorageio",
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'venv']),
    classifiers=[
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['boto3', 'google-cloud-storage'],
)
