# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
# https://www.javadoc.io/static/org.apache.nifi/nifi-api/1.7.0/index.html?org/apache/nifi/flowfile/FlowFile.html
# https://github.com/apache/thrift/blob/master/lib/py/src/transport/THttpClient.py
# https://docs.python.org/3/library/http.client.html

import unittest
import sys
import http.client

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

sys.path.append('gen-py')

from org.apache.nifi.processors.ttypes import InvalidOperationException, FlowFileRequest, FlowFileReply,ThriftFlowFile

class PythonClient(unittest.TestCase):
    def test_http(self):
        
        transport = TTransport.TMemoryBuffer()
        protocol  = TBinaryProtocol.TBinaryProtocol(transport)
        
        attributes = {'foo' : "barzz"}
        id         = 5
        content    = b'0123456789' * 10
        flow_file = ThriftFlowFile(
                                content    =  content,
                                attributes =  attributes)

        ffr_a = FlowFileRequest(id         = id,
                               flowFile    = flow_file)
        ffr_a.write(protocol)

        headers = {"Content-type" : "application/x-thrift",
                   "Accept"       : "application/x-thrift"}
        # Create a client to use the protocol encoder
        conn = http.client.HTTPConnection('localhost', 9090)
        conn.request("POST", "", transport.getvalue(), headers)
        response = conn.getresponse()

        transport = TTransport.TMemoryBuffer(response.read())
        protocol = TBinaryProtocol.TBinaryProtocol(transport)  
        reply = FlowFileReply()
        reply.read(protocol)
        
        self.assertEqual(ffr_a.id,reply.id)
        self.assertEqual(flow_file.content,   reply.flowFile.content)
        self.assertEqual(flow_file.attributes,reply.flowFile.attributes)

        conn.close()
    def test_http_bytes_io(self):
        
        transport = TTransport.TMemoryBuffer()
        protocol  = TBinaryProtocol.TBinaryProtocol(transport)
        
        attributes = {'foo' : "barzz"}
        id         = 5
        content    = b'0123456789' * 10

        flow_file = ThriftFlowFile(
                                content    =  content,
                                attributes =  attributes)

        ffr_a = FlowFileRequest(id         = id,
                               flowFile    = flow_file)
        ffr_a.write(protocol)

        headers = {"Content-type" : "application/x-thrift",
                   "Accept"       : "application/x-thrift"}
        # Create a client to use the protocol encoder
        conn = http.client.HTTPConnection('localhost', 9090)
        conn.request("POST", "", transport.getvalue(), headers)
        response = conn.getresponse()

        transport.cstringio_buf.seek(0)
        transport.cstringio_buf.write(response.read())
        transport.cstringio_buf.seek(0)

        reply = FlowFileReply()
        reply.read(protocol)
        
        self.assertEqual(ffr_a.id,reply.id)
        self.assertEqual(flow_file.content,reply.flowFile.content)
        self.assertEqual(flow_file.attributes,reply.flowFile.attributes)

        conn.close()

        # self.__wbuf = BytesIO()


if __name__ == '__main__':
    unittest.main()
