import base64
import json
import boto3
from botocore.config import Config
import os
import datetime
import urllib.parse

session = boto3.Session()

# Recommended Timestream write client SDK configuration:
#  - Set SDK retry count to 10.
#  - Use SDK DEFAULT_BACKOFF_STRATEGY
#  - Set RequestTimeout to 20 seconds .
#  - Set max connections to 5000 or higher.
# timestream_write = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

# TABLE_NAME from CloudFormation is set in format of DATABASE|TABLE, so we need to split it
# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-timestream-table.html
# TABLE_NAME_CF= os.environ['TABLE_NAME']
# fields = TABLE_NAME_CF.split('|')
# DATABASE_NAME = fields[0]
# TABLE_NAME = fields[1]

# Loads a json configuration object that defines the data type mappings for each of the valid values of Realtime Log Fields
FIELD_DATA_MAPPINGS = {}
with open('./config/cf_realtime_log_field_mappings.json') as f:
    data = json.load(f)
    FIELD_DATA_MAPPINGS = data['cf_realtime_log_fields']
    # Debug
    #print('Configured field data type mappings: ', json.dumps(FIELD_DATA_MAPPINGS))


payload_sample = {}
with open('./test.json') as f:
    data = json.load(f)
    payload_sample = data
    
# Utility function for parsing the header fields
def parse_headers(headers, header_type):
    supported_types = ['cs-headers', 'cs-header-names']
    output = []
    if header_type not in supported_types:
        print('Could not parse header, invalid type: {}'.format(header_type))

    if header_type == 'cs-headers':
        output = {header_type+'_'+header["Name"]: header["Value"] for header in headers}
        # header_list = list(filter(None, urllib.parse.unquote(headers).split('\n'))) # filter out empty strings
        # for header in header_list:
        #     kv_pair = header.split(':', 1)
        #     if len(kv_pair) > 1:
        #         for i in range(0, len(kv_pair), 2):
        #             output.append({
        #                 'Name': kv_pair[i],
        #                 'Value': kv_pair[i + 1]
        #             })
    if header_type == 'cs-header-names':
        # output = list(filter(None, urllib.parse.unquote(headers).split('\n')))
        output = {header_type + '_' + name: name for name in headers}
    return output

def write_batch_timestream(records, record_counter):
    try:
        # result = timestream_write.write_records(DatabaseName = DATABASE_NAME, TableName = TABLE_NAME, Records = records, CommonAttributes = {})
        # print('Processed [%d] records. WriteRecords Status: [%s]' % (record_counter, result['ResponseMetadata']['HTTPStatusCode']))
        print(records)
    except Exception as e:
        print(records)
        raise Exception('There was an error writing records to Amazon Timestream when inserting records')

def lambda_handler(event, context):
    records = {}
    record_counter = 0
 
    for record in event['Records']:
    
        # Extracting the record data in bytes and base64 decoding it
        payload_in_bytes = base64.b64decode(record['kinesis']['data'])

        # Converting the bytes payload to string
        payload = "".join(map(chr, payload_in_bytes))
 
        # dictionary where all the field and record value pairing will end up
        payload_dict = {}
        
        # counter to iterate over the record fields
        counter = 0
        
        # generate list from the tab-delimited log entry
        # payload_list = payload.strip().split('\t')
        # payload_list = json.loads(json.dumps(payload_sample))
        # payload_list = json.dumps(payload_sample)
        # payload_list = list(payload_sample.values())
        payload_list = payload_sample
        payload_dict = payload_list
        # print(payload_list)
        # Use field mappings configuration to perform data type conversion as needed
        # for field, data_type in FIELD_DATA_MAPPINGS.items():
        #     print(payload_list[counter]);
        #     if(data_type == "str" and payload_list[counter].strip() == '-'):
        #         data_type = "str"
        #     if(data_type == "int"):
        #         payload_dict[field] = int(payload_list[counter].strip())
        #     elif(data_type == "float"):
        #         payload_dict[field] = float(payload_list[counter])
        #     else:
        #         payload_dict[field] = payload_list[counter].strip()
        #     counter = counter + 1
        
        # Parse the headers and return as lists. This is useful if you want to log the header information as well
        if('cs-headers' in payload_dict.keys()):
            # del payload_dict['cs-headers'] # remove this line and uncomment below to include cs-headers as a list in the record
            # payload_dict['cs-headers'] = parse_headers(payload_dict['cs-headers'], 'cs-headers')
            collapsed_header = parse_headers(payload_dict['cs-headers'], 'cs-headers')
            payload_dict.update(collapsed_header)
            del payload_dict['cs-headers']
        if('cs-header-names' in payload_dict.keys()):
            # del payload_dict['cs-header-names'] # remove this line and uncomment below to include cs-header-names as a list
            #payload_dict['cs-header-names'] = parse_headers(payload_dict['cs-header-names'], 'cs-header-names')
            collapsed_header_names = parse_headers(payload_dict['cs-header-names'], 'cs-header-names')
            payload_dict.update(collapsed_header_names)
            del payload_dict['cs-header-names']

        # dimensions_list = []
        # for field, value in payload_dict.items():
        #     field_name = field.replace('-','_') # replace dashes in field names with underscore to adhere to Timsestream requirements
        #     dimensions_list.append(
        #         { 'Name': field_name, 'Value': str(value) }
        #     )

        # record = {
        #     'Dimensions': dimensions_list,
        #     'MeasureName': 'sc_bytes',
        #     'MeasureValue': str(payload_dict['sc-bytes']),
        #     'MeasureValueType': 'BIGINT',
        #     'Time': str(int(payload_dict['timestamp'])),
        #     'TimeUnit': 'SECONDS'
        # }
        records = payload_dict
        record_counter = record_counter + 1

        # if(len(records) == 100):
        #     write_batch_timestream(records, record_counter)
        #     records = []

    if(len(records) != 0):
        write_batch_timestream(records, record_counter)

    print('Successfully processed {} records.'.format(len(event['Records'])))

# event = {
#   "Records": [
#     {
#       "kinesis": {
#         "partitionKey": "1",
#         "kinesisSchemaVersion": "1.0",
#         "data": "eyJ0aW1lc3RhbXAiOiAiMjAyNC0wMS0yNlQwMDowMDowMC4wMDBaIiwgImNzLWhlYWRlcnMiOiAiY2hlY2tvdW5kLWluZm8sY2hlY2tvdW5kLW5hbWVzIiwgInNjLWJ5dGVzIjogInN0YXJ0LXN0b3JlLHN0YXJ0LXJlcXVlc3QiLCAic2MtZmxhZyI6IDAsICJzYy1jdWlkIjogMTAwMCwgInNjLWhlYWRlci1uYW1lcyI6ICJjYXNlLWhlYWRlci1uYW1lcyIsICJzYy1oZWFkZXItbmFtZXMiOiAiY2FzZS1oZWFkZXItbmFtZXMifQ==",
#         "sequenceNumber": "49545115243490985018280067714973144582180062593244200961",
#         "approximateArrivalTimestamp": 1614807024.0
#       },
#       "eventSource": "aws:kinesis",
#       "eventVersion": "1.0",
#       "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
#       "eventName": "aws:kinesis:record",
#       "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
#       "awsRegion": "us-west-2",
#       "eventSourceARN": "arn:aws:kinesis:us-west-2:123456789012:stream/example-stream"
#     }
#   ]
# }

event = {
  "Records": [
    {
      "kinesis": {
        "data": "ewogICAgInRpbWVzdGFtcCI6IDE2MDI2NDY3MzguMTQ1LAogICAgImMtaXAiOiAiMS4yLjMuNCIsCiAgICAidGltZS10by1maXJzdC1ieXRlIjogMC4wMDIsCiAgICAic2Mtc3RhdHVzIjogMjAwLAogICAgInNjLWJ5dGVzIjogMTY2NTMsCiAgICAiY3MtbWV0aG9kIjogIkdFVCIsCiAgICAiY3MtcHJvdG9jb2wiOiAiaHR0cHMiLAogICAgImNzLWhvc3QiOiAic29tZWhvc3QxMjMuY2xvdWRmcm9udC5uZXQiLAogICAgImNzLXVyaS1zdGVtIjogIi9pbWFnZS5qcGciLAogICAgImNzLWJ5dGVzIjogNTksCiAgICAieC1lZGdlLWxvY2F0aW9uIjogIklBRDY2LUMxIiwKICAgICJ4LWVkZ2UtcmVxdWVzdC1pZCI6ICJib05iMWFsN0I1MEc1VDdqWERPR2kyemxZQUYyVldyYmEyZm5aV2Z1Y3NvbWV0aGluZzEyMzQ1X1VBPT0iLAogICAgIngtaG9zdC1oZWFkZXIiOiAic29tZWhvc3QxMjMuY2xvdWRmcm9udC5uZXQiLAogICAgInRpbWUtdGFrZW4iOiAwLjAwMiwKICAgICJjcy1wcm90b2NvbC12ZXJzaW9uIjogIkhUVFAvMi4wIiwKICAgICJjLWlwLXZlcnNpb24iOiAiSVB2NCIsCiAgICAiY3MtdXNlci1hZ2VudCI6ICJjdXJsLzcuNTMuMSIsCiAgICAiY3MtcmVmZXJlciI6ICItIiwKICAgICJjcy1jb29raWUiOiAiLSIsCiAgICAiY3MtdXJpLXF1ZXJ5IjogIi0iLAogICAgIngtZWRnZS1yZXNwb25zZS1yZXN1bHQtdHlwZSI6ICJIaXQiLAogICAgIngtZm9yd2FyZGVkLWZvciI6ICItIiwKICAgICJzc2wtcHJvdG9jb2wiOiAiVExTdjEuMiIsCiAgICAic3NsLWNpcGhlciI6ICJFQ0RIRS1SU0EtQUVTMTI4LUdDTS1TSEEyNTYiLAogICAgIngtZWRnZS1yZXN1bHQtdHlwZSI6ICJIaXQiLAogICAgImZsZS1lbmNyeXB0ZWQtZmllbGRzIjogIi0iLAogICAgImZsZS1zdGF0dXMiOiAiLSIsCiAgICAic2MtY29udGVudC10eXBlIjogImltYWdlL2pwZWciLAogICAgInNjLWNvbnRlbnQtbGVuIjogMTYzMzUsCiAgICAic2MtcmFuZ2Utc3RhcnQiOiAiLSIsCiAgICAic2MtcmFuZ2UtZW5kIjogIi0iLAogICAgImMtcG9ydCI6IDM2MjQyLAogICAgIngtZWRnZS1kZXRhaWxlZC1yZXN1bHQtdHlwZSI6ICJIaXQiLAogICAgImMtY291bnRyeSI6ICJVUyIsCiAgICAiY3MtYWNjZXB0LWVuY29kaW5nIjogIi0iLAogICAgImNzLWFjY2VwdCI6ICIqLyoiLAogICAgImNhY2hlLWJlaGF2aW9yLXBhdGgtcGF0dGVybiI6ICIqIiwKICAgICJjcy1oZWFkZXJzIjogWwogICAgICAgIHsKICAgICAgICAgICAgIk5hbWUiOiAiaG9zdCIsCiAgICAgICAgICAgICJWYWx1ZSI6ICJzb21laG9zdDEyMy5jbG91ZGZyb250Lm5ldCIKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIk5hbWUiOiAidXNlci1hZ2VudCIsCiAgICAgICAgICAgICJWYWx1ZSI6ICJjdXJsLzcuNTMuMSIKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIk5hbWUiOiAiYWNjZXB0IiwKICAgICAgICAgICAgIlZhbHVlIjogIiovKiIKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIk5hbWUiOiAiQ2xvdWRGcm9udC1Jcy1Nb2JpbGUtVmlld2VyIiwKICAgICAgICAgICAgIlZhbHVlIjogImZhbHNlIgogICAgICAgIH0sCiAgICAgICAgewogICAgICAgICAgICAiTmFtZSI6ICJDbG91ZEZyb250LUlzLVRhYmxldC1WaWV3ZXIiLAogICAgICAgICAgICAiVmFsdWUiOiAiZmFsc2UiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJOYW1lIjogIkNsb3VkRnJvbnQtSXMtU21hcnRUVi1WaWV3ZXIiLAogICAgICAgICAgICAiVmFsdWUiOiAiZmFsc2UiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJOYW1lIjogIkNsb3VkRnJvbnQtSXMtRGVza3RvcC1WaWV3ZXIiLAogICAgICAgICAgICAiVmFsdWUiOiAidHJ1ZSIKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIk5hbWUiOiAiQ2xvdWRGcm9udC1WaWV3ZXItQ291bnRyeSIsCiAgICAgICAgICAgICJWYWx1ZSI6ICJVUyIKICAgICAgICB9CiAgICBdLAogICAgImNzLWhlYWRlci1uYW1lcyI6IFsKICAgICAgICAiaG9zdCIsCiAgICAgICAgInVzZXItYWdlbnQiLAogICAgICAgICJhY2NlcHQiLAogICAgICAgICJDbG91ZEZyb250LUlzLU1vYmlsZS1WaWV3ZXIiLAogICAgICAgICJDbG91ZEZyb250LUlzLVRhYmxldC1WaWV3ZXIiLAogICAgICAgICJDbG91ZEZyb250LUlzLVNtYXJ0VFYtVmlld2VyIiwKICAgICAgICAiQ2xvdWRGcm9udC1Jcy1EZXNrdG9wLVZpZXdlciIsCiAgICAgICAgIkNsb3VkRnJvbnQtVmlld2VyLUNvdW50cnkiCiAgICBdLAogICAgImNzLWhlYWRlcnMtY291bnQiOiA4LAogICAgInRpbWVzdGFtcF91dGMiOiAiMjAyMC0xMC0xNFQwMzozODo1OCIKfQ=="
      }
    }
  ]
}

lambda_handler(event, {})