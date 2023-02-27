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


import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.net.HttpURLConnection;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

@Tags({ "thrift", "converter" })
@CapabilityDescription("Creates a thrift ThriftFlowFile object, wrapped in a FlowFileReply Object, from the flowfile. " +
                       "flowfile attributes , eg thrift.attr.xxx, are copied to ThriftFlowFile.attributes('xxx')." +
                       "flowfile content is copied to ThriftFlowFile.content" +
                       "FlowFileReply.id will contain the maintained thrift.id attr ")
@SeeAlso({PutThriftIDL.class,FromThriftProcessor.class})
@ReadsAttributes({
    @ReadsAttribute(attribute   = "thrift.id",
                    description = "The Thrift id of the Thrift object"),
    @ReadsAttribute(attribute   = "thrift.attr.*",
                    description = "Each attribute 'thrift.attr.xxx' is copied to the Thrift object's 'xxx' attribute"),
    @ReadsAttribute(attribute   = "thrift.protocol",
                    description = "the thrift protocol to be used for serialisation (ignored unless DynamicProtocol)")
                })
@WritesAttributes({
        @WritesAttribute(attribute   = "thrift.http_status_attr",
                         description = "HTTP Status code to be passed back by, eg, HandleHttpResponse")
                  })

public class ToThriftProcessor extends AbstractThriftProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Serialized Thrift object in flowfile")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed to create serialized Thrift object in flowfile")
            .build();

    private static final String HttpStatusAttr = "thrift.http_status_attr";

    public ToThriftProcessor() {
        super();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // nifi/nifi-nar-bundles/nifi-jolt-record-bundle/nifi-jolt-record-processors/src/main/java/org/apache/nifi/processors/jolt/record/JoltTransformRecord.java
        final StopWatch stopWatch = new StopWatch(true);
        if (perRequestProtcol) {
            try{
                String thrift_protocol = flowFile.getAttribute("thrift.protocol");
                if (null != thrift_protocol) {
                    setProtocol(thrift_protocol);
                }else{
                    getLogger().error("perRequestProtocol but no attr, 'thrift.protocol', set");
                    throw new ProcessException("perRequestProtocol but no attr, 'thrift.protocol', set");
                }
            }catch (Exception ex) {
                // ex.printStackTrace();
                getLogger().error("Failed to process perrequest protocol " + ex.getMessage());
                session.transfer(flowFile, FAILURE);
                return;
            }
        }


        ThriftFlowFile thriftFlowFile = new ThriftFlowFile();
        FlowFileReply flowFileReply = new FlowFileReply();
        FlowFile newflowFile = session.create(flowFile);
        try {
            flowFileReply.setFlowFile(thriftFlowFile);
            session.read(flowFile, (in) -> {
                    try {
                        byte[] bytes = new byte[in.available()];
                        in.read(bytes);
                        thriftFlowFile.setContent(bytes);
                    } catch (Exception ex) {
                        getLogger().error("Failed to read flowFileRequest");
                        throw new ProcessException("Failed to read flowFileRequest, ",ex);
                    }
            });

            if (getLogger().isDebugEnabled()) {
                String flowFileReplyContent = new String(thriftFlowFile.getContent());
                getLogger().debug("flowFileReplyContent " + flowFileReplyContent);
            }
            int prefixlength = AttrPrefix.length();
            for (Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                if (entry.getKey().startsWith(AttrPrefix)) {
                    thriftFlowFile.putToAttributes(entry.getKey().substring(prefixlength), entry.getValue());
                }
                getLogger().debug("ToThriftProcessor: attribute " + entry.getKey() + " : " + entry.getValue());
            }
            /**
             * If we have the attribute "thrift.id" (set in FromThriftProcessor then use it
             * to populate the id member
             */
            String thrift_id = flowFile.getAttribute("thrift.id");
            if (null != thrift_id) {
                flowFileReply.setId(Long.parseLong(thrift_id));
            }

            // this attribute can be interpreted by HTTPResponse
            session.putAttribute(newflowFile, HttpStatusAttr,
                                String.valueOf(HttpURLConnection.HTTP_OK));
            flowFileReply.setResponseCode(ResponseCode.SUCCESS);

            byte[] serializedFlowFileReply = serializer.serialize(flowFileReply);
            // https://www.nifi.rocks/developing-a-custom-apache-nifi-processor-json/
            // To write the results back out to flow file
            newflowFile = session.write(newflowFile, (out) -> {
                    out.write(serializedFlowFileReply);
            });
        } catch (Exception ex) {
            // ex.printStackTrace();
            getLogger().error("Failed to write flowFileReply, " + ex.getMessage());
            if (newflowFile != null) {
                session.remove(newflowFile);
            }
            session.transfer(flowFile, FAILURE);
            return;
        }
        // check for no change?
        session.getProvenanceReporter().modifyAttributes(newflowFile, "Modified With serialised thrift attributes");
        session.getProvenanceReporter().modifyContent(newflowFile, "Modified With serialised thrift content", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.remove(flowFile);
        session.transfer(newflowFile, SUCCESS);
    }

}
