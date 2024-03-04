import base64
import json
import boto3
from botocore.config import Config
import os
import datetime
import urllib.parse
import datetime
import os
from config import constants

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
with open(constants.LOG_FIELD_MAPPING_FILE_PATH) as f:
    data = json.load(f)
    FIELD_DATA_MAPPINGS = data[constants.CF_REALTIME_LOG_FIELDS_LABEL]


payload_sample = {}
with open(constants.SAMPLE_DATA_FILE_PATH) as f:
    data = json.load(f)
    payload_sample = data

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for idx, item in enumerate(v):
                if isinstance(item, dict):
                    for sub_k, sub_v in item.items():
                        sub_new_key = new_key + sep + sub_k + sep + sub_v
                        items.append((sub_new_key, sub_v))
                else:
                    sub_new_key = new_key + sep + str(idx)
                    items.append((sub_new_key, item))
        else:
            items.append((new_key, v))
    return dict(items)

# Utility function for parsing the header fields
def parse_headers(headers, header_type):
    supported_types = [constants.CS_HEADER_LABEL, constants.CS_HEADER_NAME_LABEL]
    output = []
    if header_type not in supported_types:
        print('Could not parse header, invalid type: {}'.format(header_type))

    if header_type == constants.CS_HEADER_LABEL:
        output = {header_type+'_'+header["Name"]: header["Value"] for header in headers}
        
    if header_type == constants.CS_HEADER_NAME_LABEL:
        output = {header_type + '_' + name: name for name in headers}
    return output

def write_batch_file(records):
    try:
        cloudfront_logs_direcory = os.path.join(constants.SAVE_TO_DRIVE, constants.LOGS_DIRECTORY_NAME)
        if not os.path.exists(cloudfront_logs_direcory):
            os.makedirs((cloudfront_logs_direcory))
            
        today = datetime.datetime.utcnow().strftime("%d-%m-%Y")
        log_directory = os.path.join(cloudfront_logs_direcory, today)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        
        current_hour = datetime.datetime.utcnow().strftime('%H')
        log_file_name = f"{constants.LOG_FILE_NAME_PREFIX}{current_hour}{constants.LOG_FILE_EXTENSION}"
        log_file_path = os.path.join(log_directory, log_file_name)
        
        mapped_fields = constants.MAPPED_FIELDS
        
        log_line = ""
        for data in records:
            timestamp_utc = data.get("timestamp_utc", "")
            date, time = "-", "-"
            if "-" in timestamp_utc and "T" in timestamp_utc:
                date, time = timestamp_utc.split("T")
            log_line = "\t".join(str(data.get(mapped_fields[field], "-")) for field in 
                                 [constants.CS_IP, constants.CS_METHOD, constants.CS_URI, constants.STATUS, 
                                  constants.SC_BYTES, constants.TIME_TAKEN, constants.CS_REFERER, constants.CS_USER_AGENT, 
                                  constants.CS_COOKIE, constants.X_DISID, constants.X_HEADERSIZE, 
                                  constants.CACHED, constants.CS_RANGE, constants.X_TCWAIT, constants.X_TCPINFO_RTT, 
                                  constants.X_TCPINFO_RTTVAR, constants.X_TCPINFO_SND_CWND, 
                                  constants.X_TCPINFO_RCV_SPACE, constants.X_TDWAIT, 
                                  constants.SC_CONTENT_TYPE, constants.CS_VERSION, constants.C_CITY, 
                                  constants.C_STATE, constants.C_COUNTRY])

            log_line = f"{date}\t{time}\t{log_line}"
        
        header_content = constants.LOG_FILE_HEADER_CONTENT
        
        if not os.path.exists(log_file_path):
            with open(log_file_path, 'w') as f:
                f.write(header_content + log_line + "\n")
        else:
            with open(log_file_path, 'a') as f:
                f.write(log_line + "\n")
                
    except Exception as e:
        print(e)     
    
    
    

def lambda_handler(event, context):
    
    records = {}
    record_counter = 0
 
    for record in event['Records']:
    
        # Extracting the record data in bytes and base64 decoding it
        payload_in_bytes = base64.b64decode(record['kinesis']['data'])

        # Converting the bytes payload to string
        payload = "".join(map(chr, payload_in_bytes))
 
        payload_dict = {}
        
        payload_list = payload_sample
        payload_dict = flatten_dict(payload_list)
        
        if(constants.CS_HEADER_LABEL in payload_dict.keys()):
            collapsed_header = parse_headers(payload_dict[constants.CS_HEADER_LABEL], constants.CS_HEADER_LABEL)
            payload_dict.update(collapsed_header)
            del payload_dict[constants.CS_HEADER_LABEL]
        if(constants.CS_HEADER_NAME_LABEL in payload_dict.keys()):
            collapsed_header_names = parse_headers(payload_dict[constants.CS_HEADER_NAME_LABEL], constants.CS_HEADER_NAME_LABEL)
            payload_dict.update(collapsed_header_names)
            del payload_dict[constants.CS_HEADER_NAME_LABEL]
            
        records = payload_dict
        record_counter = record_counter + 1


    if(len(records) != 0):
        write_batch_file([records])

    print('Successfully processed {} records.'.format(len(event['Records'])))

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