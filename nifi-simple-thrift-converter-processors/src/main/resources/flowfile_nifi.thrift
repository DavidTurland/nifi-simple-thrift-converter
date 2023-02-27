namespace java org.apache.nifi.processors.thrift
namespace perl Org.Apache.Nifi.Processors.Thrift
namespace py   org.apache.nifi.processors.thrift

/*
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to
You under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

exception InvalidOperationException {
    1: i32 code,
    2: string description
}

enum ResponseCode {
  ERROR = 0,
  SUCCESS = 1,
  RETRY = 2,
}

struct ThriftFlowFile{
   3: map<string,string> attributes,
   15: binary content,
}

struct FlowFileRequest{
   1: i64 id,
   3: ThriftFlowFile flowFile,
}

/*
struct FlowFileRequest{
   1: i64 id,
   2: FlowFile flowFile,
}
*/

/* https://jar-download.com/artifacts/org.apache.nifi/nifi-standard-processors/0.2.1/source-code/org/apache/nifi/processors/standard/HandleHttpResponse.java
*/

struct FlowFileReply{
   1: ResponseCode responseCode,
   2: i64 id,
   3: ThriftFlowFile flowFile,
}

struct RecordSet{
   1: list<ThriftFlowFile> flowFiles,
}
