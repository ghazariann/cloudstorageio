"""A setuptools based setup module.
See:
https://packaging.python.org/tutorials/packaging-projects/
"""
import os

from codecs import open
from setuptools import setup, find_packages

# Detect version
here = os.path.abspath(os.path.dirname(__file__))
module_path = os.path.join(here, 'cloudstorageio', '__init__.py')
version_line = [line for line in open(module_path)
                if line.startswith('__version__')][0]

__version__ = version_line.split('__version__ = ')[-1][1:][:-2]

# Get the long description from the README file
with open("README.md", "r") as fh:
    long_description = fh.read()

# Get all installed packages from requirements.txt
with open('/home/vahagn/dev/workspace/cognaize/cloudstorageio/requirements.txt', 'r') as f:
    requirements = f.read().splitlines()


setup(
    name="cloudstorageio",
    version=__version__,
    author="Vahagn Ghazaryan",
    author_email="vahagn.ghazayan@gmail.com",
    description="Tool working with storage interfaces",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/VahagnGhaz/cloudstorageio",
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'venv']),
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements
)

