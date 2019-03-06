# cloudstorageio
Tool working with S3 and Google Storage interfaces

_Developed and tested on python 3.7+_

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for usage purpose.

      
### Prerequisites
```bash
sudo apt install python3.6
sudo apt install python3-pip
```
* #### set S3 Configs  
    ```bash 
    pip install awscli --upgrade --user 
    ```
    
    set your aws configs 
    ```bash
    $ aws configure
    AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
    AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    Default region name [None]: us-west-2
    Default output format [None]: json
     ``` 
   [click here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation) for more info about configuration and installation 

* #### set Cloud Storage Configs 
   Provide authentication credentials to your application code by setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.
   Replace [FILE_NAME] with the file of the JSON file that contains your service account key
  
    ```bash 
   GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json" 
    ```
      
   [click here](https://cloud.google.com/storage/docs/reference/libraries) for more info about configuration and installation
   
   [click here](https://www.techcoil.com/blog/how-to-set-environment-variables-for-your-python-application-from-pycharm/) for setting env variables in PyCharm 


### Installing
A step by step series of examples that tell you how to get a validator in ready
```bash 
pip install --upgrade google-cloud-storage boto3`
```

### Packaging
A step by step series of work that tell you how to make a package of validator
     
 ```bash  
 cd /path/to/folder/cloudstorageio
 # This will create dist/cloudstorageio-<version>-py3-*.whl
 python3 setup.py sdist bdist_wheel
 ```
 ```bash
 # Uninstall previous version if it's exist
 pip3 uninstall cloudstorageio-<version>-py3-none-any.whl
 # and this will install the package/tool in your computer
 pip3 install dist/cloudstorageio-<version>-py3-none-any.whl
 ```
 
### Usage 
 * ##### Write text to Google cloud storage file 
     ```python
   from cloudstorageio.service.cloud_interface import CloudInterface
    
   google_file_path = "gs://bucket-name/path-to-file/sample-file.txt"
     
   ci = CloudInterface()
    
   with ci.open(google_file_path, 'w') as f:
       f.write("Lorem Ipsum is simply dummy text of the printing and typesetting industry. ")
        
     ```
 * ##### Read picture from S3 storage 
    ```python
   from cloudstorageio.service.cloud_interface import CloudInterface

   s3_file_path = 's3://bucket-name/path-to-file/sample-pic.jpg'
   ci = CloudInterface()

   with ci.open(s3_file_path, 'rb') as f:
        output = f.read()
   print(output) # Prints binary content of picture 
    
    ```

 * ##### Read text locally 
     ```python
   from cloudstorageio.service.cloud_interface import CloudInterface
    
   local_file_path = 'path-to-file/sample-text-file.txt'
   ci = CloudInterface()
    
   with ci.open(local_file_path, 'r') as f:
       output = f.read()
   print(output) # Prints string content of text file 
        
    ```