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
 * mvn enforcer:display-info
 * mvn help:active-profiles
 *
 */
package org.apache.nifi.processors.thrift;


import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ToThriftProcessorTest {

    private TestRunner testRunnerTo;

    @Before
    public void init() {
        testRunnerTo = TestRunners.newTestRunner(ToThriftProcessor.class);
    }

    /**
     * Tests the ToThriftProcessor
     * using all the supported Thrift protocols
     * The TestRunner enqueues known content and attributes
     * With the FlowFile result from the SUCCESS relationship:
     * It's content is deserialised using the appropriate protocol to a ThriftFlowFileReply object
     *     - Thrift object's attribute member match the enqueued attributes
     *  - Thrift object's content member matches the enqueued content
     *  - Thrift object's id member matches the 'thrift.id' attribute
     */
    @Test
    public void testAllProcessorProtocols() {
        List<String> factories = Stream
                .of(AbstractThriftProcessor.ProtocolJSON,
                    AbstractThriftProcessor.ProtocolBinary,
                    AbstractThriftProcessor.ProtocolCompact)
                .collect(Collectors.toList());
        for (String factoryName : factories) {
            TProtocolFactory factory = AbstractThriftProcessor.getFactory(factoryName);
            long thrift_id = new Date().getTime();
            Map<String, String> attrs = new HashMap<>();
            attrs.put("wibble_key", "wibble_value");
            attrs.put("thrift.id", Long.toString(thrift_id));
            String contentString = "I am some content";

            testRunnerTo.setProperty(AbstractThriftProcessor.THRIFT_PROTOCOL,
                                     factoryName);
            testRunnerTo.enqueue(contentString.getBytes(), attrs);

            // Run the enqueued content, it also takes an int = number of contents queued
            testRunnerTo.run(1);

            // All results were processed with out failure
            testRunnerTo.assertQueueEmpty();
            // If you need to read or do additional tests on results you can access the
            // content
            List<MockFlowFile> results = testRunnerTo.getFlowFilesForRelationship(ToThriftProcessor.SUCCESS);
            assertTrue("1 match", results.size() >= 1);
            MockFlowFile result = results.get(results.size()-1);

            FlowFileReply flowFileReply = new FlowFileReply();
            try {
                TDeserializer deserializer = new TDeserializer(factory);
                deserializer.deserialize(flowFileReply, testRunnerTo.getContentAsByteArray(result));
                assertEquals("id member is correct", thrift_id, flowFileReply.getId());
            }catch (TTransportException tte){
                fail(tte.getMessage());
            }catch (Exception e) {
                fail(e.getMessage());

            }
        }
    }
}
