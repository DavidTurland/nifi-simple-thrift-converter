namespace java org.apache.nifi.processors

exception InvalidOperationException {
    1: i32 code,
    2: string description
}

enum ResponseCode {
  ERROR = 0,
  SUCCESS = 1,
  RETRY = 2,
}

struct FlowFileRequest{
   1: i64 id,
   2: map<string,string> attributes,
   15: binary content,
}

/* https://jar-download.com/artifacts/org.apache.nifi/nifi-standard-processors/0.2.1/source-code/org/apache/nifi/processors/standard/HandleHttpResponse.java
*/
struct FlowFileReply{
   1: ResponseCode responseCode,
   2: i64 id,
   3: map<string,string> attributes,
   15: binary content,
}

