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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * @author davidt
 *
 */
public abstract class AbstractThriftProcessor extends AbstractProcessor {

    public static final String ProtocolJSON = "JSON";
    public static final String ProtocolBinary = "BINARY";
    public static final String ProtocolCompact = "COMPACT";
    protected static final String defaultPerRequestProtcol = "false";
    public static final String AttrPrefix = "thrift.attr.";
    private String currentProtocol;
    protected boolean perRequestProtcol;
    protected TDeserializer deserializer;
    protected TSerializer serializer;
    protected Set<Relationship> relationships;
    protected List<PropertyDescriptor> descriptors;
    public static final PropertyDescriptor DYNAMIC_PROTOCOL = new PropertyDescriptor.Builder()
            .name("DynamicProtocol")
            .description("Allow per request thrift protocol")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue(defaultPerRequestProtcol)
            .dynamic(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor THRIFT_PROTOCOL = new PropertyDescriptor.Builder()
            .name("ThriftProtocol")
            .description("What Thrift protocol is used")
            .required(true)
            .allowableValues(new AllowableValue(ProtocolJSON), new AllowableValue(ProtocolBinary),new AllowableValue(ProtocolCompact))
            .defaultValue(ProtocolBinary)
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public AbstractThriftProcessor() {
        super();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DYNAMIC_PROTOCOL);
        descriptors.add(THRIFT_PROTOCOL);
        this.descriptors = Collections.unmodifiableList(descriptors);
        try{
            setProtocol(THRIFT_PROTOCOL.getDefaultValue());
        }catch (TTransportException tte){
            throw new RuntimeException("setProtocol",tte);
        }
        setPerRequestProtcol(defaultPerRequestProtcol);
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.getName().equals("ThriftProtocol")) {
            try{
                setProtocol(newValue);
            }catch (TTransportException tte){
                throw new RuntimeException("setProtocol",tte);
            }
        }
        if (descriptor.getName().equals("DynamicProtocol")) {
            setPerRequestProtcol(newValue);
        }
        super.onPropertyModified(descriptor, oldValue, newValue);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * @param descriptor the PropertyDescriptor
     */
    protected void addPropertyDescriptor(PropertyDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("null descriptor");
        }
        List<PropertyDescriptor> descriptors = new ArrayList<>(this.descriptors);
        descriptors.add(descriptor);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    /**
     * @param option the per request protocol
     */
    protected void setPerRequestProtcol(String option) {
        if (option == null) {
            throw new IllegalArgumentException("null option");
        }
        perRequestProtcol = option.equals("true");
    }

    /**
     * @param protocol the protocol
     * @return TProtocolFactory
     * @throws IllegalArgumentException if an invalid protocol
     */
    public static TProtocolFactory getFactory(String protocol) throws IllegalArgumentException {
        if (protocol == null) {
            throw new IllegalArgumentException("null protocol");
        }
        switch (protocol) {
        case ProtocolJSON:
            return new TJSONProtocol.Factory();
        case ProtocolBinary:
            return new TBinaryProtocol.Factory();
        case ProtocolCompact:
            return new TCompactProtocol.Factory();
        default:
            throw new IllegalArgumentException("getFactory Invalid protocol -" + protocol + "-");
        }
    }

    /**
     * @param protocol the protocol
     * @throws IllegalArgumentException if an invalid protocol
     */
    protected void setProtocol(String protocol) throws IllegalArgumentException,TTransportException{
        if (currentProtocol != null && currentProtocol.equals(protocol)) {
            return;
        }
        if (protocol == null) {
            throw new IllegalArgumentException("null protocol");
        }
        switch (protocol) {
        case ProtocolJSON:
            deserializer = new TDeserializer(new TJSONProtocol.Factory());
            serializer = new TSerializer(new TJSONProtocol.Factory());
            break;
        case ProtocolBinary:
            deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            serializer = new TSerializer(new TBinaryProtocol.Factory());
            break;
        case ProtocolCompact:
            deserializer = new TDeserializer(new TCompactProtocol.Factory());
            serializer = new TSerializer(new TCompactProtocol.Factory());
            break;
        default:
            throw new IllegalArgumentException("setProtocol Invalid protocol " + protocol);
        }

    }

}
