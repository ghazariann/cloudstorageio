# cloudstorageio
Tool to handle with storage interface 

_Developed and tested on python 3.7+_

## Getting Started

### Prerequisites
* #### S3 Configs  
        pip install awscli --upgrade --user
    
    set your aws configs 

        $ aws configure
        AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
        AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
        Default region name [None]: us-west-2
        Default output format [None]: json
   [click here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation) for more info about configuration and installation 

* #### Cloud Storage Configs 
   Provide authentication credentials to your application code by setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.
   Replace [FILE_NAME] with the file of the JSON file that contains your service account key
  
      GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json"
      
   [click here](https://cloud.google.com/storage/docs/reference/libraries) for more info about configuration and installation
   
   [click here](https://www.techcoil.com/blog/how-to-set-environment-variables-for-your-python-application-from-pycharm/) for setting env variables in PyCharm 

### Installations 


`pip install --upgrade google-cloud-storage`

`pip install boto3`
### Running the tests
cloudstorageio -> service -> s3_interface.py -> main()
