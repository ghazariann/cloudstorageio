### Packaging
A step by step series of work that tell you how to make a package of validator
     
 ```bash  
 # This will install or update all necessary packages 
 pip install --user --upgrade setuptools wheel twine
 ```
 ```bash
 cd /path/to/folder/cloudstorageio
 
 # This will create dist/cloudstorageio-<version>-py3-*.whl and dist/cloudstorageio-<version>-py3-*.tar.gz
 python3 setup.py sdist bdist_wheel
 # Upload package to PYPI
  python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
 ```
 
[click here](https://packaging.python.org/tutorials/packaging-projects/) for more info about python packaging
[click here](https://www.techcoil.com/blog/how-to-set-environment-variables-for-your-python-application-from-pycharm/) for setting env variables in PyCharm 
