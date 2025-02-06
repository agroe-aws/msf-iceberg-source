/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.msf;

import com.amazonaws.services.msf.iceberg.IcebergSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.msf.avro.AvroSchemaUtils;
import com.amazonaws.services.msf.datagen.AvroGenericStockTradeGeneratorFunction;
import com.amazonaws.services.msf.iceberg.IcebergSinkBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    // Name of the local JSON resource with the application properties in the same format as they are received from the Amazon Managed Service for Apache Flink runtime
    private final static Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    // private static final String DEFAULT_GROUP_ID = "my-group";
    // private static final String DEFAULT_SOURCE_TOPIC = "InputStream";
    // private static final String DEFAULT_SINK_TOPIC = "OutputStream";
    // private static final OffsetsInitializer DEFAULT_OFFSETS_INITIALIZER = OffsetsInitializer.earliest();

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }
    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime or from a local resource, when the environment is local
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    DataStreamJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    // Generate data
    // Data Generator source generating random trades as AVRO GenericRecords
    private static DataGeneratorSource<GenericRecord> createDataGenerator(Properties generatorProperties, Schema avroSchema) {
        double recordsPerSecond = Double.parseDouble(generatorProperties.getProperty("records.per.sec", "10.0"));
        Preconditions.checkArgument(recordsPerSecond > 0, "Generator records per sec must be > 0");

        LOG.info("Data generator: {} record/sec", recordsPerSecond);
        return new DataGeneratorSource<>(
                new AvroGenericStockTradeGeneratorFunction(avroSchema),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                new GenericRecordAvroTypeInfo(avroSchema)
        );
    }


    // Sink data from MSK topic to Iceberg
    // MSK Source
    // private static KafkaSource<String> createKafkaSource(Properties inputProperties) {
    //     OffsetsInitializer startingOffsetsInitializer = inputProperties.containsKey("startTimestamp") ? OffsetsInitializer.timestamp(
    //             Long.parseLong(inputProperties.getProperty("startTimestamp"))) : DEFAULT_OFFSETS_INITIALIZER;

    //     return KafkaSource.<String>builder()
    //             .setBootstrapServers(inputProperties.getProperty("bootstrap.servers"))
    //             .setTopics(inputProperties.getProperty("topic", DEFAULT_SOURCE_TOPIC))
    //             .setGroupId(inputProperties.getProperty("group.id", DEFAULT_GROUP_ID))
    //             .setStartingOffsets(startingOffsetsInitializer) // Used when the application starts with no state
    //             .setValueOnlyDeserializer(new SimpleStringSchema())
    //             .setProperties(inputProperties)
    //             .build();
    // }

    // private static FlinkKinesisConsumer<String> createSource(Properties inputProperties) {
    //     String inputStreamName = inputProperties.getProperty("stream.name0");
    //     return new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties);
    // }

    // private static KinesisStreamsSink<String> createSink(Properties outputProperties) {
    //     String outputStreamName = outputProperties.getProperty("stream.name1");
    //     return KinesisStreamsSink.<String>builder()
    //             .setKinesisClientProperties(outputProperties)
    //             .setSerializationSchema(new SimpleStringSchema())
    //             .setStreamName(outputStreamName)
    //             .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
    //             .build();
    // }

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationProperties = loadApplicationProperties(env);


        // Get AVRO Schema from the definition bundled with the application
        // Note that the application must "knows" the AVRO schema upfront, i.e. the schema must be either embedded
        // with the application or fetched at start time.
        // If the schema of the records received by the source changes, all changes must be FORWARD compatible.
        // This way, the application will be able to write the data with the new schema into the old schema, but schema
        // changes are not propagated to the Iceberg table.
        Schema avroSchema = AvroSchemaUtils.loadSchema();

        // Create Generic Record TypeInfo from schema.
        GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);

        // Local dev specific settings
        if (isLocal(env)) {
            org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//            env.disableOperatorChaining();
            // Checkpointing and parallelism are set by Amazon Managed Service for Apache Flink when running on AWS
            env.enableCheckpointing(10000);
            env.setParallelism(2);
        }



		// Sink to KDS/MSK for downstream Redshift
		// input = change to whatever datastream instance we want to use to sink to MSK out. need to make a new datastream prob
		// how to use datastream for this join -- 
        // Sink<String> sink = createSink(applicationParameters.get("OutputStream0"));
        // input.sinkTo(sink);


        // Data Generator Source.
        // Simulates an external source that receives AVRO Generic Records
        Properties dataGeneratorProperties = applicationProperties.get("DataGen");
        DataStream<GenericRecord> genericRecordDataStream = env.fromSource(
                createDataGenerator(dataGeneratorProperties, avroSchema),
                WatermarkStrategy.noWatermarks(),
                "DataGen");


        //Sink back to Iceberg/S3
        // Flink Sink Builder
        Properties icebergProperties = applicationProperties.get("Iceberg");
        FlinkSink.Builder icebergSinkBuilder = IcebergSinkBuilder.createBuilder(
                icebergProperties,
                genericRecordDataStream, avroSchema);
        // Sink to Iceberg Table
        icebergSinkBuilder.append();

        // Read from Iceberg table
        DataStreamSource<GenericRecord> stream = IcebergSourceBuilder
                .builder(icebergProperties, avroSchema)
                .withMonitorInterval(Duration.ofSeconds(15))
                .buildSource(env);


        stream.print();

        // Submit and execute this streaming read job.
        env.execute("Test Flink Iceberg Datastream Integration");
    }

}
