# cloudstorageio
Storage agnostic IO interface for humans

_Developed and tested on python 3.7+_

[GitHub](https://github.com/VahagnGhaz/cloudstorageio)

[PyPi](https://pypi.org/project/cloudstorageio)

## Getting Started
These instructions will get you a copy of the project up and running on your local machine.

* #### S3 configs  
    ```bash 
    pip install awscli --upgrade --user 
    ```
    set your aws configs 
    ```bash
     sudo apt install awscli
    ```
    
    ```bash
    $ aws configure
    AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
    AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    Default region name [None]: us-west-2
    Default output format [None]: json
     ``` 
   [click here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation) for more info about configuration and installation 

* #### Google cloud storage configs 
   Provide authentication credentials to your application code by setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.
   Replace [FILE_NAME] with the file of the JSON file that contains your service account key
  
    ```bash 
   GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json" 
    ```
      
   [click here](https://cloud.google.com/storage/docs/reference/libraries) for more info about configuration and installation

### Installation
```
pip install cloudstorageio
```
 
### Usage 

* Write and read file 
```python
from cloudstorageio import CloudInterface

google_file_path = 'gs://bucket-name/path-to-file/sample-file.txt'
s3_file_path = 's3://bucket-name/path-to-file/sample-pic.jpg'

ci = CloudInterface()

# Write text to Google cloud storage file 
with ci.open(google_file_path, 'w') as f:
   f.write("Lorem Ipsum is simply dummy text")
   
# Read picture from S3 storage 
with ci.open(s3_file_path, 'rb') as f:
    s3_output = f.read()  # binary content of picture 
```

* Remove, list, and check folder/file  
```python
# List folder content
from cloudstorageio import CloudInterface

google_folder_path = 'gs://bucket-name/folder'
s3_file_path = 's3://bucket-name/path-to-file/sample-pic.jpg'
ci = CloudInterface()

ci.isfile(s3_file_path) # returns True 
ci.isdir(google_folder_path) # returns True
ci.remove(s3_file_path) # removes file 
ci.listdir(google_folder_path) # list folder content 

```
