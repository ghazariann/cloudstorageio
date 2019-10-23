"""A setuptools based setup module.
See:
https://packaging.python.org/tutorials/packaging-projects/
"""

from codecs import open

from setuptools import setup, find_packages

from cloudstorageio import __name__ as package_name
from cloudstorageio import __version__ as package_version

# Get the long description from the README file
with open("READMe.md", "r") as fh:
    long_description = fh.read()
with open('requirements.txt', 'r') as rf:
    requirements = rf.read().split('\n')

setup(
    name=package_name,
    version=package_version,
    author="Vahagn Ghazaryan",
    author_email="vahagn.ghazayan@gmail.com",
    description="Cloud storage IO for humans",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/VahagnGhaz/cloudstorageio",
    packages=find_packages(exclude=['contrib', 'docs', 'cloudstorageio.log', 'venv', 'cloudstorageio.tests',
                                    'cloudstorageio.tests.resources', 'cloudstorageio.tests.interface',
                                    'cloudstorageio.log']),
    classifiers=[
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # install_requires=['boto3', 'google-cloud-storage', 'dropbox'],
    install_requires=requirements,
    setup_requires=['wheel']

)
