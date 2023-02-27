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
//import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Date;
import java.io.ByteArrayInputStream;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

//import org.apache.thrift.TDeserializer;
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


public class FromThriftProcessorTest {

    private TestRunner testRunnerFrom;

    @Before
    public void init() {
        testRunnerFrom = TestRunners.newTestRunner(FromThriftProcessor.class);
    }

    /**
     * Tests the FromThriftProcessor
     * using all the supported Thrift protocols
     * A Thrift FlowFileRequest object is constructed with known data ...
     * ... and serialised with the appropriate protocol
     * The serialised bytes are enqueued
     * We check the FlowFile result from the SUCCESS relationship:
     *  - the attributes match those in the Thrift object's attribute member
     *  - the content matches that in the Thrift object's content member
     *  - the 'thrift.id' attribute matches the Thrift object's id member
     */
    @Test
    public void testAllProcessorProtocols() {
        FlowFileRequest.metaDataMap.forEach((key, value) -> System.err.println(key + " : " + value));
        List<String> factories = Stream
                .of(AbstractThriftProcessor.ProtocolJSON,
                    AbstractThriftProcessor.ProtocolBinary,
                    AbstractThriftProcessor.ProtocolCompact)
                .collect(Collectors.toList());
        for (String factoryName : factories) {
            TProtocolFactory factory = FromThriftProcessor.getFactory(factoryName);
            long thrift_id = new Date().getTime();
            FlowFileRequest ffr = new FlowFileRequest(thrift_id,new ThriftFlowFile());

            String contentString = "I am some content";
            ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
            int byte_size = contentString.getBytes().length;
            ffr.getFlowFile().setContent(contentString.getBytes());
            assertTrue("bytes size > 1", 1 < byte_size);

            testRunnerFrom.setProperty(FromThriftProcessor.CONVERSION_SCOPE, "All");
            testRunnerFrom.setProperty(AbstractThriftProcessor.THRIFT_PROTOCOL,
                    factoryName);
            try {
                TSerializer serializer = new TSerializer(factory);
                InputStream content = new ByteArrayInputStream(serializer.serialize(ffr));
                // Add the content to the runner
                assertTrue("enqueued content has size", 0 < content.available());
                testRunnerFrom.enqueue(content);
            } catch (TTransportException tte){
                fail(tte.getMessage());
            } catch (TException | IOException e) {
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
                result.assertContentEquals(contentString.getBytes());
            } catch (IOException e) {
                fail(e.getMessage());
            }

            String resultContentString = new String(testRunnerFrom.getContentAsByteArray(result));
            assertEquals(contentString, resultContentString);

            // Test attributes and content
            result.assertAttributeEquals(AbstractThriftProcessor.AttrPrefix + "wibble_key", "wibble_value");
        }
    }

    /**
     * Tests the FromThriftProcessor using the JSON protocol (any might do)
     * A Thrift object is not constructed, we just create some hopefully invalid
     *  serialised Thrift object bytes
     * These 'serialised bytes' are enqueued
     * We check there is a FlowFile result from the FAILURE relationship:
     *  - there is a error attribute in the FlowFile ...
     *  - ... and that is has the expected value
     */
    @Test
    public void testNifiInvalidThrift() {
        String invalid_thrift = "I am probably not valid thrift";
        testRunnerFrom.setProperty(FromThriftProcessor.CONVERSION_SCOPE, "All");
        testRunnerFrom.setProperty(AbstractThriftProcessor.THRIFT_PROTOCOL,
                                   AbstractThriftProcessor.ProtocolJSON);

        InputStream content = new ByteArrayInputStream(invalid_thrift.getBytes());
        try {
            assertTrue("enqueued content has a size", 1 < content.available());
            testRunnerFrom.enqueue(content);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        // Run the enqueued content, it also takes an int = number of contents queued
        testRunnerFrom.run(1);

        // All results were processed without failure
        testRunnerFrom.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the
        // content
        List<MockFlowFile> results = testRunnerFrom.getFlowFilesForRelationship(FromThriftProcessor.FAILURE);
        assertTrue("1 results match", results.size() == 1);
        MockFlowFile result = results.get(0);

        String error = result.getAttribute("error");
        assertTrue(error, error.contains("Unexpected character:"));
    }

}
