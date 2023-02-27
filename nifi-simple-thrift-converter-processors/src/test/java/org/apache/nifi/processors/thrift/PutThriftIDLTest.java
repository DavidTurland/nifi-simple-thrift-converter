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
 */
package org.apache.nifi.processors.thrift;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class PutThriftIDLTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutThriftIDL.class);
    }

    /**
     * Tests the PutThriftIDL processor
     * The TestRunner enqueues known, and irrelevant, content and attributes
     * With the FlowFile result from the SUCCESS relationship:
     * It's content is morphed to a String
     *     - The attributes match the enqueued attributes
     *  - The string somewhat matches the Thrift File contents
     *  - the attribute PutThriftIDL.THRIFT_ATTR (maybe "thrift.filename") exists
     */
    @Test
    public void testProcessor() {
        Map<String, String> ignoredAttrs = new HashMap<>();
        ignoredAttrs.put("wibble_key", "wibble_value");
        String ignoredContentString = "I am some ignored content";

        testRunner.enqueue(ignoredContentString.getBytes(), ignoredAttrs);
        int iterations = 1;
        boolean stopOnFinish = true;
        boolean initialize = true;
        // Run the enqueued content, it also takes an int = number of contents queued
        testRunner.run(iterations, stopOnFinish, initialize);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();
        // If you need to read or do additional tests on results you can access the
        // content
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PutThriftIDL.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        testRunner.getLogger().info(result.toString());
        result.assertAttributeEquals("wibble_key", "wibble_value");
        result.assertAttributeExists(PutThriftIDL.THRIFT_ATTR);
        testRunner.getLogger().info("thrift filename " + result.getAttribute(PutThriftIDL.THRIFT_ATTR));
        String contents = new String(result.toByteArray());
        String expectedLine = "namespace java org.apache.nifi.processors";
        assertEquals("thriftfile",expectedLine,contents.substring(0, expectedLine.length()));

        testRunner.getLogger().info("contents" + contents);

    }

}
