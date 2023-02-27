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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.MissingResourceException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;


@Tags({ "thrift", "converter" })
@CapabilityDescription("returns the thiftfile, as a string, in the flowfile content")
@SeeAlso({ToThriftProcessor.class,FromThriftProcessor.class})
@WritesAttributes({
    @WritesAttribute(attribute   = "thrift.filename",
                     description = "Filename of the thrift IDL specification"),
                  })

public class PutThriftIDL extends AbstractProcessor {
    public static final String THRIFT_FILENAME = "flowfile_nifi.thrift";
    public static final String THRIFT_ATTR     = "thrift.filename";
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Source of Thrift IDL").build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed to create content from thrift IDL specification")
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private byte[] idlBytes;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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
     *
     * loads the Thrift IDL resource once
     */
    @OnAdded
    public void loadIDL() {
        try {
            this.idlBytes = getBytesFromResource(THRIFT_FILENAME);

        } catch (Exception ex) {
            getLogger().error("Failed to process thrift file, error " + ex.getMessage());
            throw new MissingResourceException("loadIDL: Failed to read IDL resource file into byte[]",
                                                File.class.getName(),
                                                THRIFT_FILENAME);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        try {
            flowFile = session.putAttribute(flowFile, THRIFT_ATTR, THRIFT_FILENAME);
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(idlBytes);
                }
            });
        } catch (Exception ex) {
            getLogger().error("Failed to process thrift file, error " + ex.getMessage());
            session.transfer(flowFile, FAILURE);
            return;
        }
        session.transfer(flowFile, SUCCESS);
    }

    /**
     * @param fileName 'filename' of resource
     * @return byte[]
     * @throws IOException if the inputStream fails
     * @throws IllegalArgumentException if the resource isn't found
     *
     */
    private byte[] getBytesFromResource(String fileName) throws IOException{
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IllegalArgumentException("thrift file not found in resources");
        }
        BufferedInputStream bis = new BufferedInputStream(inputStream);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for (int result = bis.read(); result != -1; result = bis.read()) {
            buf.write((byte) result);
        }
        return buf.toByteArray();
    }
}
