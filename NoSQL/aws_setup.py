import secret_keys
import boto3

s3 = boto3.resource('s3', aws_access_key_id=secret_keys.aws_access_key_id, aws_secret_access_key=secret_keys.aws_secret_access_key)

bucket_name = 'experimentdata342343'
table_name = 'DataTable'

s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
    'LocationConstraint': 'us-east-2'})
bucket = s3.Bucket(bucket_name)
bucket.Acl().put(ACL='public-read')

s3.Object(bucket_name, 'experiments.csv').put(
    Body=open('experiments.csv', 'rb'))

dyndb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id=secret_keys.aws_access_key_id, aws_secret_access_key=secret_keys.aws_secret_access_key)

table = dyndb.create_table(
    TableName=table_name,
    KeySchema=[
        {'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
        {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
    ],
    AttributeDefinitions=[
        {'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
        {'AttributeName': 'RowKey', 'AttributeType': 'S'}
    ],
    ProvisionedThroughput={
            'ReadCapacityUnits': 40,
            'WriteCapacityUnits': 40
        }
)

import csv

table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
print("Done setting up table")

urlbase = "https://s3.us-east-2.amazonaws.com/" + bucket_name + "/"
with open('experiments.csv') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        body = open(item[6], 'rb')
        s3.Object(bucket_name, item[6]).put(Body=body)
        md = s3.Object(bucket_name, item[6]).Acl().put(ACL='public-read')
        url=urlbase + item[6]
        metadata_item={'PartitionKey': item[0], 'RowKey': item[1], 'date': item[2], 'Temp': item[3], 'Conductivity' : item[4], 'Concentration' : item[5],  'url' : url}
        table.put_item(Item=metadata_item)

response = table.get_item(
    Key={
    'PartitionKey': 'experiment1',
    'RowKey': '1'
    }
)
print_item = response['Item']
print(print_item)


