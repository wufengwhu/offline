/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.jpush.util.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

public class DemoProducer {

    public static void main(String[] args) {
        Properties props = new Properties();


        String topic = args[0];
        int iters = Integer.parseInt(args[1]);
        props.put("metadata.broker.list", args[2]);
        props.put("request.required.acks", "-1");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "sync");

        BaseProducer demo = new BaseProducer(props);

        Schema schema = new Schema.Parser().parse("{\n" +
                "\t\"type\":\"record\",\n" +
                "\t\"name\":\"test_schema_1\",\n" +
                "\t\"fields\" : [ {\n" +
                "\t\t\"name\":\"a\",\n" +
                "\t\t\"type\":\"int\"\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\"name\":\"b\",\n" +
                "\t\t\"type\":\"string\"\n" +
                "\t\t}\n" +
                "\t\t]\n" +
                "}");


        GenericRecord event = new GenericData.Record(schema);

        for (int i = 1; i < iters; i++) {
            event.put("a", i);
            event.put("b", "static string");
            demo.publish(event, topic, schema);
        }
        demo.close();
    }
}
