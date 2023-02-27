/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *
 *
 */
package org.apache.nifi.processors.thrift;

import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Date;


import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
//import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AllThriftProcessorsTest {

    private TestRunner testRunnerFrom;
    private TestRunner testRunnerTo;

    @Before
    public void init() {
        testRunnerFrom = TestRunners.newTestRunner(FromThriftProcessor.class);
        testRunnerTo   = TestRunners.newTestRunner(ToThriftProcessor.class);
    }

    /**
     *
     */
    @Test
    public void testAllProcessor() {
        List<String> factories = Stream
                .of(AbstractThriftProcessor.ProtocolJSON,
                    AbstractThriftProcessor.ProtocolBinary,
                    AbstractThriftProcessor.ProtocolCompact)
                .collect(Collectors.toList());
        for (String factoryName : factories) {
            TProtocolFactory factory = FromThriftProcessor.getFactory(factoryName);
            long thrift_id = new Date().getTime();
            FlowFileRequest ffr = new FlowFileRequest(thrift_id,new ThriftFlowFile());
            String mime = "I am a some mime";
            ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
            int byte_size = mime.getBytes().length;
            ffr.getFlowFile().setContent(mime.getBytes());
            assertTrue("bytes size > 1", 1 < byte_size);



            testRunnerFrom.setProperty(FromThriftProcessor.CONVERSION_SCOPE, "All");
            testRunnerFrom.setProperty(AbstractThriftProcessor.THRIFT_PROTOCOL,
                    factoryName);
            testRunnerTo.setProperty(AbstractThriftProcessor.THRIFT_PROTOCOL,
                     factoryName);
            try {
                TSerializer serializer = new TSerializer(factory);
                InputStream content = new ByteArrayInputStream(serializer.serialize(ffr));
                // Add the content to the runner
                assertTrue("enqued content has a size", 1 < content.available());
                testRunnerFrom.enqueue(content);
            }catch (TTransportException tte){
                fail(tte.getMessage());
            }catch (TException | IOException e) {
                fail(e.getMessage());
            }

            // Run the enqueued content, it also takes an int = number of contents queued
            testRunnerFrom.run(1);

            // All results were processed with out failure
            testRunnerFrom.assertQueueEmpty();

            List<MockFlowFile> results = testRunnerFrom.getFlowFilesForRelationship(FromThriftProcessor.SUCCESS);
            assertTrue("1 match", results.size() >= 1);
            MockFlowFile result = results.get(results.size()-1);
            result.getAttributes().forEach((k, v) -> {
                System.out.println(k + "[" + v + "]");
            });

            // Test attributes and content
            try {
                result.assertContentEquals(mime.getBytes());
            }catch (IOException e) {
                fail(e.getMessage());
            }
            String contentString = new String(testRunnerFrom.getContentAsByteArray(result));
            assertEquals("", mime, contentString);

            // Test attributes and content
            result.assertAttributeEquals("thrift.id", Long.toString(thrift_id));


            testRunnerTo.enqueue(result);
            // Run the enqueued content, it also takes an int = number of contents queued
            testRunnerTo.run(1);

            // All results were processed with out failure
            testRunnerTo.assertQueueEmpty();
            // If you need to read or do additional tests on results you can access the
            // content
            List<MockFlowFile> resultsTo = testRunnerTo.getFlowFilesForRelationship(ToThriftProcessor.SUCCESS);
            assertTrue("1 match", resultsTo.size() >= 1);
            MockFlowFile resultTo = resultsTo.get(results.size()-1);

            FlowFileReply flowFileReply = new FlowFileReply();
            try {
                TDeserializer deserializer = new TDeserializer(factory);
                deserializer.deserialize(flowFileReply, testRunnerTo.getContentAsByteArray(resultTo));
                assertEquals("id member is correct", thrift_id, flowFileReply.getId());
            }catch (TTransportException tte){
                fail(tte.getMessage());
            }catch (Exception e) {
                fail(e.getMessage());

            }
            assertEquals(flowFileReply.getId() ,thrift_id);

        }
    }

}
