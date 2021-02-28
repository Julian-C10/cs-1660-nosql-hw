import boto3
import csv

s3 = boto3.resource('s3',
 aws_access_key_id='my-access-key',
 aws_secret_access_key='my-secret'
)

try:
    s3.create_bucket(Bucket='datacont-name2', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

bucket = s3.Bucket("datacont-name2")
bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
body = open('myFile.txt', 'rb')

o = s3.Object('datacont-name2', 'test').put(Body=body )

s3.Object('datacont-name2', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
 region_name='us-west-2',
 aws_access_key_id='my-access-key',
 aws_secret_access_key='my-secret'
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")


table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)


with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        if item[0] == "partition":
            continue
        print(item)
        body = open('c:/users/julian/mine/classes/cs-1660/assignments/nosql hw/'+item[4], 'rb')
        s3.Object('datacont-name2', item[4]).put(Body=body )
        md = s3.Object('datacont-name2', item[4]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/datacont-name2/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
 'description' : item[3], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
 Key={
 'PartitionKey': 'experiment2',
 'RowKey': 'data2'
 }
)
item = response['Item']
print(item)