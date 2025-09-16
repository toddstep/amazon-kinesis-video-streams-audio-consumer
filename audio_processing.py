# Copyright Todd Stephenson. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

'''
Class to analyze an audio file with a Lambda function and to store the
results in a DynamoDB table.

Note: python-ffmpeg is dependent on ffmpeg binaries (https://ffmpeg.org/download.html).

Changelog:
9/16/2025, Todd Stephenson: Initial version
 '''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Todd Stephenson. All Rights Reserved."
__author__ = "Todd Stephenson <https://www.linkedin.com/in/todd-stephenson-91a5a58/>"

import base64
import datetime
import json
import logging
import os
from ffmpeg import FFmpeg

log = logging.getLogger(__name__)

class AudioProcessing:
    '''
    Analyze audio and store scores
    '''
    def __init__(self, session, function_name, table_name):
        self.session = session
        self.lambda_client = self.session.client("lambda")
        self.dynamodb_client = self.session.client("dynamodb")
        self.function_name = function_name
        self.table_name = table_name
        log.info(f"setup function {self.function_name} and table {self.table_name}")

    def __call__(self, mkv_file, fragment_tags):
        audio_file = os.path.splitext(mkv_file)[0] + '.ogg'
        AudioProcessing.mkv2ogg(mkv_file, audio_file)
        lambda_response = self.invoke_lambda(audio_file)
        if lambda_response['code'] == 200:
            self.put_dynamodb(lambda_response, fragment_tags)
        else:
            log.warn(f"Function {self.function_name} response: {lambda_response['code']}. "
                     f"Not writing anything to {self.table_name}. "
                     f"Timestamp: {fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP']}.")
        os.remove(audio_file)

    @staticmethod
    def mkv2ogg(mkv_file, audio_file):
        output_options = {
            "q": "10",
            "af": "highpass=f=80,pan=mono|c0=FL",
            "ar": "22050"
        }
        ffmpeg = FFmpeg().option("y").input(mkv_file).output(audio_file, output_options)
        ffmpeg.execute()

    def invoke_lambda(self, audio_file):
        with open(audio_file, 'rb') as f:
            payload_body = base64.b64encode(f.read()).decode('utf-8')
        lambda_response = self.lambda_client.invoke(
            FunctionName=self.function_name,
            Payload=json.dumps({'body': payload_body, 'threshold': 0.0}),
        )
        lambda_response = json.loads(lambda_response['Payload'].read())
        return lambda_response

    def put_dynamodb(self, lambda_response, fragment_tags):
        producer_timestamp_sec, _ = fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP'].split('.')
        request_items = [{'PutRequest': { 'Item': {
                       'species': {'S': species[1]},
                       'time': {'N': producer_timestamp_sec},
                       'score': {'N': str(round(score, 4))},
        }}} for idx, species, score in lambda_response['top_results']]
        # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
        batch_size = 25
        for start_idx in range(0, len(request_items), batch_size):
            rc = self.dynamodb_client.batch_write_item(RequestItems={self.table_name: request_items[start_idx:start_idx+batch_size]},
                                                       ReturnConsumedCapacity='TOTAL')
            print(f"Batch start {start_idx}:", rc)

