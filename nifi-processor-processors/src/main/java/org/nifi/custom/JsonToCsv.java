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
package org.nifi.custom;


import com.jsontocsv.parser.JsonFlattener;
import com.jsontocsv.writer.CSVWriter;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"JSON", "NIFI ROCKS"})
@CapabilityDescription("Fetch value from json path.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class JsonToCsv extends AbstractProcessor {


    private List<PropertyDescriptor> properties;

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final String MATCH_ATTR = "match";


    private JsonFlattener parser;
    private CSVWriter writer;


    public static final AllowableValue HEADER_ONLY = new AllowableValue("Header Only", "Header Only",
            "Only header record will be outputted ");
    public static final AllowableValue BODY_ONLY = new AllowableValue("Body Only", "Body Only",
            "Only body records will be outputted.  The header record will not be emitted");
    public static final AllowableValue HEADER_AND_BODY = new AllowableValue("Header and Body", "Header and Body",
            "Output will contain header record and all body record(s)");



    public static final PropertyDescriptor OUTPUTHANDLER = new PropertyDescriptor.Builder()
            .name("Type of Output")
            .description("This setting will determine if header, body, or header with body is outputted from processor")
            .allowableValues(HEADER_ONLY, BODY_ONLY, HEADER_AND_BODY)
            .defaultValue(BODY_ONLY.getValue())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("Json Path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();




    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(OUTPUTHANDLER);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);


        parser = new JsonFlattener();
        writer = new CSVWriter();

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog log = getLogger();
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{

                    String outputHandler = context.getProperty(OUTPUTHANDLER).getValue();


                    String json = IOUtils.toString(in);
                    System.out.println(json);


                    //String result = writer.writeAsCSV(parser.parseJson(json));
                    String result = writer.writeAsCSV(parser.parseJson(json), context);


                    //String result = JsonPath.read(json, context.getProperty(JSON_PATH).getValue());
                    value.set(result);
                }catch(Exception ex){
                    ex.printStackTrace();
                    log.error("Failed to read json string.");
                }
            }
        });

        // Write the results to an attribute
        String results = value.get();
        if(results != null && !results.isEmpty()){
            flowfile = session.putAttribute(flowfile, "match", results);
        }

        // To write the results back out ot flow file
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowfile, SUCCESS);
    }
}
