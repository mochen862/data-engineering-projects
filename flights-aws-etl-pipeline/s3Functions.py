"""
Functions for using AWS S3 services
"""

def checkS3bucket(s3Client=None, bucketName=None, clientError=None):
    """
    The check S3 bucket funtion takes a bucket name and searches to see if that bucket exists or not. It returns a Yes or No answer.
    Parameters:
        s3Client:  The client instance from boto3 to interact with S3 services 
        bucketName: The name of the bucket being searched for 
        clientError: The clientError instance from botocore.client 
    Returns:
        a 'Yes' or 'No' value indicating if the bucket exists or not 
    """
    try:
        s3Client.head_bucket(Bucket=bucketName) 
        return 'Yes'
    except clientError:
        print("The bucket does not exist")
        return 'No'



def buildS3bucket(s3Client=None, bucketName=None, clientError=None, awsRegion=None):
    """
    The build S3 bucket funtion takes a bucket name and creates an empty bucket in the AWS region chosen 
    Parameters:
        s3Client:  The client instance from boto3 to interact with S3 services 
        bucketName: The name of the bucket being searched for 
        clientError: The clientError instance from botocore.client
        awsRegion: The region in which the bucket will be located in 
    Returns:
        log: Contains statement indicating if bucket has been built, or error occured and failed 
    """
    try:
        location = {'LocationConstraint': awsRegion} 
        s3Client.create_bucket(Bucket=bucketName,
                                CreateBucketConfiguration=location)
        log = 'S3 bucket successfully created' 
        return log 

    except clientError as e:
        log = 'ERROR - S3 bucket has NOT been created'
        return log 



def loadToS3(s3Client=None, bucketName=None, clientError=None, bucketKey=None, loadFile=None, fileName=None):
    """
    This function takes a file passed to it, and loads it to a specified S3 bucket on AWS 
    Parameters:
        s3Client:  The client instance from boto3 to interact with S3 services 
        bucketName: The name of the bucket being searched for 
        clientError: The clientError instance from botocore.client
        bucketKey:  The path, if any, for the file to sit at in the S3 bucket
        loadFile:   The file that is to be loaded to the S3 bucket
        fileName:   The name you want to give to the file when on the S3 bucket
    Returns:
        S3loadLog:  Contains a statement indicating if the file has successfully loaded or not 
    """
    upload_bucket = bucketName 
    upload_bucket_key = str(bucketKey) + str(fileName) 
    try:
        s3Client.upload_file(loadFile, upload_bucket, upload_bucket_key)
        S3loadLog = "Successfully loaded " + str(fileName) + " to " + str(upload_bucket) 
        return S3loadLog 

    except clientError as e:
        S3loadLog = "ERROR - File " + str(fileName) + " has NOT loaded to " + str(upload_bucket)
        return S3loadLog 