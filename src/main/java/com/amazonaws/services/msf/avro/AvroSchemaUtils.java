package com.amazonaws.services.msf.avro;

import com.amazonaws.services.msf.DataStreamJob;
import org.apache.avro.Schema;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class AvroSchemaUtils implements Serializable {

    private static final String AVRO_SCHEMA_RESOURCE = "price.avsc";

    /**
     * Load the AVRO Schema from the resources folder
     */
    public static Schema loadSchema() throws IOException {
        // Get AVRO Schema from the definition bundled with the application
        InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream(AVRO_SCHEMA_RESOURCE);
        return new org.apache.avro.Schema.Parser().parse(inputStream);
    }
}
