import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import * 
from pyspark.sql.functions import *
import boto3
import base64


from awsglue.dynamicframe import DynamicFrame
import hashlib

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

finding_macie_database="PREFIX"
finding_macie_tables="PREFIX_glue_AWS_REGION_ACCOUNT_ID"
bucket_athena="s3://dcp-athena-AWS_REGION-ACCOUNT_ID".replace("_", "-")


#inputs to KMS
key_id = "KEY_NAME"
region_name = "us_east_1".replace("_", "-")


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

macie_findings = glueContext.create_dynamic_frame.from_catalog(database = finding_macie_database, table_name = finding_macie_tables)

macie_findings_df = macie_findings.toDF()

macie_findings.printSchema()

columns_to_be_masked_and_encrypted = []

tables_list = []


def get_tables_to_be_masked_and_encrypted(df):
    df = df.select('key').collect()
    try:
        for i in df:
            e = i['key'].split("/")[1] 
            if e not in tables_list:
                tables_list.append(e)

    except:
        print ("DEBUG:",sys.exc_info())
    

    return tables_list

def get_columns_to_be_masked_and_encrypted(df):
    df = df.select('jsonPath').collect()
    try:
        for i in df:
            columns_to_be_masked_and_encrypted.append(i['jsonPath'].split(".")[1])    
    except:
        print ("DEBUG:",sys.exc_info())
    

    return columns_to_be_masked_and_encrypted

if 'detail' in macie_findings_df.columns:
    #Working with json in Spark
    try:
        #sensitiveData
        #detail.classificationDetails.result.sensitiveData.detections
        macie_finding_sensitiveData = macie_findings_df.select("detail.resourcesAffected.s3Object.key", "detail.classificationDetails.result.sensitiveData.detections").select("key", explode("detections").alias("new_detections")).select("key","new_detections.occurrences.records").select("key", explode("records").alias("new_records")).select("key","new_records.jsonPath").select("key", explode("jsonPath").alias("jsonPath")).drop_duplicates()


    #     macie_finding_sensitiveData.printSchema();
    #     macie_finding_sensitiveData.head(20)

        get_tables_to_be_masked_and_encrypted(macie_finding_sensitiveData);
        get_columns_to_be_masked_and_encrypted(macie_finding_sensitiveData);
    except:
        print ("DEBUG:",sys.exc_info())

if 'detail' in macie_findings_df.columns:

    try:    
        #customDataIdentifiers
        macie_finding_custome_data_identifiers = macie_findings_df.select("detail.resourcesAffected.s3Object.key", "detail.classificationDetails.result.customDataIdentifiers.detections").select("key", explode("detections").alias("new_detections")).select("key","new_detections.occurrences.records").select("key", explode("records").alias("new_records")).select("key","new_records.jsonPath").select("key", "jsonPath").drop_duplicates()
        
        #macie_findings_custome_data_identifiers_df_7.printSchema();
        #macie_findings_custome_data_identifiers_df_7.head(20)
        
        get_tables_to_be_masked_and_encrypted(macie_finding_custome_data_identifiers);
        get_columns_to_be_masked_and_encrypted(macie_finding_custome_data_identifiers);
    except:
        print ("DEBUG:",sys.exc_info())
    

def get_secret():

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region_name)


    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        
    return secret 


def masked_rows(r):
    try:
        for entity in columns_to_be_masked_and_encrypted:
            if entity in table_columns:
                r[entity + '_masked'] = "##################"
                del r[entity]          
    except:
        print ("DEBUG:",sys.exc_info())

    return r

def get_kms_encryption(row):
    # Create a KMS client
    session = boto3.session.Session()
    client = session.client(service_name='kms',region_name=region_name)
    
    try:
        encryption_result = client.encrypt(KeyId=key_id, Plaintext=row)
        blob = encryption_result['CiphertextBlob']
        encrypted_row = base64.b64encode(blob)
        print('encrypted_row________', encrypted_row)
        string_teste = encrypted_row.decode("utf-8")
        
        #decrypt
        decrypted = client.decrypt(CiphertextBlob=base64.b64decode(string_teste))
        print('decrypted 999999999999999',decrypted['Plaintext'])
        
        return string_teste
        
    except:
        print('Erro')

    return '' 


def encrypt_rows(r):
    #retrieve the secret in Secrets Manager
    salted_string = get_secret()
    print(salted_string)
    encrypted_entities = columns_to_be_masked_and_encrypted
    # print ("encrypt_rows", salted_string, encrypted_entities)
    try:
        for entity in encrypted_entities:
            if entity in table_columns:
                encrypted_entity = get_kms_encryption(r[entity])
                r[entity + '_encrypted'] = encrypted_entity
                del r[entity]
    except:
        print ("DEBUG:",sys.exc_info())
    return r


for table in tables_list:
    dataset_table_to_mask_and_encrypt = glueContext.create_dynamic_frame.from_catalog(database = "dataset", table_name = table)

    # Get columns names
    table_columns = dataset_table_to_mask_and_encrypt.toDF().columns

    # Apply mask to the identified fields
    df_masked_completed = Map.apply(frame = dataset_table_to_mask_and_encrypt, f = masked_rows)
    masked_path = bucket_athena +  "/masked/" + table
    # output to s3 in parquet format
    data_masked = glueContext.write_dynamic_frame.from_options(frame = df_masked_completed, connection_type = "s3", connection_options = {"path": masked_path}, format = "parquet", transformation_ctx = "datasink5")
    
    
    # Apply encryption to the identified fields
    df_encrypted_completed = Map.apply(frame = dataset_table_to_mask_and_encrypt, f = encrypt_rows)
    encrypted_path = bucket_athena + "/encrypted/"  + table   
     # output to s3 in parquet format
    data_encrypted = glueContext.write_dynamic_frame.from_options(frame = df_encrypted_completed, connection_type = "s3", connection_options = {"path": encrypted_path}, format = "parquet", transformation_ctx = "datasink5")


job.commit()




