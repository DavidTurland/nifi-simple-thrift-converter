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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.util.StopWatch;


@Tags({ "thrift", "converter" })
@CapabilityDescription("Creates a new flowfile by deserializing the FlowFileRequest contained in the flowfile.content." +
                       "Each attribute 'xxx' from the FlowFileRequest.ThriftFlowFile.attribute object " +
                          "is copied to the flowfile attribute 'thrift.attr.xxx'" +
                       "FlowFileRequest.ThriftFlowFile.contents is copied to flowfile.content")
@SeeAlso({ToThriftProcessor.class,PutThriftIDL.class})

@WritesAttributes({
    @WritesAttribute(attribute   = "thrift.id",
                     description = "The Thrift id of the Thrift object"),
    @WritesAttribute(attribute   = "thrift.attr.*",
                     description = "Each attribute 'xxx' from the Thrift object " +
                                     "is copied to the flowfile attribute 'thrift.attr.xxx'")
                  })
public class FromThriftProcessor extends AbstractThriftProcessor {

    private static final String ConversionScopeAll     = "All";
    private static final String ConversionScopeAttr    = "Attr";
    private static final String ConversionScopeContent = "Content";
    private              String conversionScope ;
    private              boolean convertingAttr;
    private              boolean convertingContent;
    public static final PropertyDescriptor CONVERSION_SCOPE = new PropertyDescriptor.Builder()
            .name("ConversionScope")
            .description("What Thrift object members (Attributes,Content,All) are converted to FlowFile")
            .required(true)
            .allowableValues(new AllowableValue(ConversionScopeAll),
                            new AllowableValue(ConversionScopeAttr),
                            new AllowableValue(ConversionScopeContent))
            .defaultValue(ConversionScopeAll)
            .dynamic(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Flowfile extracted from Thrift")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed Thrift to flowfile conversion")
            .build();

    public FromThriftProcessor() {
        super();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
        addPropertyDescriptor(CONVERSION_SCOPE);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        setConversionScope(CONVERSION_SCOPE.getDefaultValue());
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.getName().equals("ConversionScope")) {
            setConversionScope(newValue);
        }
        super.onPropertyModified(descriptor, oldValue, newValue);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        // https://javadoc.io/static/org.apache.nifi/nifi-api/1.1.0/index.html?org/apache/nifi/flowfile/FlowFile.html
        final FlowFileRequest flowFileRequest = new FlowFileRequest();
        try {
            session.read(flowFile, (in) -> {
                    byte[] bytes = new byte[in.available()];
                    in.read(bytes);
                    try {
                        deserializer.deserialize(flowFileRequest, bytes);
                    } catch (Exception ex) {
                        throw new IOException(ex);
                    }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to read flowFileRequest");
            flowFile = session.putAttribute(flowFile, "error", ex.getMessage());
            session.transfer(flowFile, FAILURE);
            return;
        }
        ThriftFlowFile thriftFlowFile = flowFileRequest.getFlowFile();
        if (getLogger().isDebugEnabled()) {
            String flowFileRequestContent = new String(thriftFlowFile.getContent());
            getLogger().debug("flowFileRequestContent " + flowFileRequestContent);
        }

        FlowFile newflowFile = session.create(flowFile);
        if(convertingAttr){
            if (null != thriftFlowFile.attributes) {
                for (Map.Entry<String, String> entry : thriftFlowFile.attributes.entrySet()) {
                    newflowFile = session.putAttribute(newflowFile, AttrPrefix + entry.getKey(), entry.getValue());
                }
            }
        }

        /**
         * capture the 'id' member in the attribute thrift.id which needs to survive the flow
         * if we want to return it back the client in ToThriftProcessor
         */
        session.putAttribute(newflowFile,"thrift.id",Long.toString(flowFileRequest.getId()));
        if(convertingContent){
            try {
                // https://www.nifi.rocks/developing-a-custom-apache-nifi-processor-json/
                newflowFile = session.write(newflowFile, (out) -> {
                        if (thriftFlowFile.content.hasArray()) {
                            out.write(thriftFlowFile.getContent());
                        } else {
                            byte[] arr = new byte[thriftFlowFile.content.remaining()];
                            thriftFlowFile.content.get(arr);
                            out.write(arr);
                        }
                });
            } catch (Exception ex) {
                // ex.printStackTrace();
                getLogger().error("Failed to write thrift flowfile content, " + ex.getMessage());
                if (newflowFile != null) {
                    session.remove(newflowFile);
                }
                session.transfer(flowFile, FAILURE);
            }
        }
        session.getProvenanceReporter().modifyAttributes(newflowFile, "Modified With deserialised thrift attributes");
        session.getProvenanceReporter().modifyContent(newflowFile, "Modified With deserialised thrift content", stopWatch.getElapsed(TimeUnit.MILLISECONDS));

        session.remove(flowFile);
        session.transfer(newflowFile, SUCCESS);
    }

    /**
     * @param conversionScope the conversionScope
     * @throws IllegalArgumentException if an invalid protocol
     */
    protected void setConversionScope(String conversionScope) throws IllegalArgumentException{
        if (this.conversionScope != null && this.conversionScope.equals(conversionScope)) {
            return;
        }
        if (conversionScope == null) {
            throw new IllegalArgumentException("null conversion scope");
        }

        switch (conversionScope) {
            case ConversionScopeAll:
                convertingAttr    = true;
                convertingContent = true;
                break;
            case ConversionScopeAttr:
                convertingAttr = true;
                break;
            case ConversionScopeContent:
                convertingContent = true;
                break;
            default:
                throw new IllegalArgumentException("setConversionScope Invalid conversionScope" + conversionScope);
        }

    }


}
