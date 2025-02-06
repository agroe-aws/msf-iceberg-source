package com.amazonaws.services.msf.iceberg;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.AvroGenericRecordReaderFunction;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;

import java.time.Duration;
import java.io.IOException;
import java.util.*;

public class IcebergSourceBuilder {

    private static final String DEFAULT_GLUE_DB = "default_database";
    private static final String DEFAULT_ICEBERG_TABLE_NAME = "prices_iceberg";
    private static final String DEFAULT_ICEBERG_SORT_ORDER_FIELD = "accountNr";
    private static final String DEFAULT_ICEBERG_PARTITION_FIELDS = "symbol";
    private static final String DEFAULT_ICEBERG_OPERATION = "append";
    private static final String DEFAULT_ICEBERG_UPSERT_FIELDS = "symbol";

    private final CatalogLoader catalogLoader;
    private final TableLoader tableLoader;
    private final Schema avroSchema;
    private Duration monitorInterval = Duration.ofSeconds(60);


    private IcebergSourceBuilder(CatalogLoader catalogLoader, TableLoader tableLoader, Schema avroSchema) {
        this.catalogLoader = catalogLoader;
        this.tableLoader = tableLoader;
        this.avroSchema = avroSchema;
    }

    // Iceberg Flink Source Builder
    public static IcebergSourceBuilder builder(Properties icebergProperties, org.apache.avro.Schema avroSchema) throws IOException {
        // Catalog properties for using Glue Data Catalog
        Map<String, String> catalogProperties = new HashMap<>();
        // Retrieve configuration from application parameters
        String s3BucketPrefix = Preconditions.checkNotNull(
                icebergProperties.getProperty("bucket.prefix"),
                "Iceberg S3 bucket prefix not defined"
        );


        catalogProperties.put("type", "iceberg");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("warehouse", s3BucketPrefix);

        String glueDatabase = icebergProperties.getProperty("catalog.db", DEFAULT_GLUE_DB);
        String glueTable = icebergProperties.getProperty("catalog.table", DEFAULT_ICEBERG_TABLE_NAME);

        // Load Glue Data Catalog
        CatalogLoader glueCatalogLoader = CatalogLoader.custom(
                "glue",
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );


        // Table Object that represents the table in the Glue Data Catalog
        TableIdentifier inputTable = TableIdentifier.of(glueDatabase, glueTable);

        TableLoader tableLoader = TableLoader.fromCatalog(glueCatalogLoader, inputTable);

        // Load created Iceberg Catalog to perform table operations
//        Catalog catalog = glueCatalogLoader.loadCatalog();


        return new IcebergSourceBuilder(glueCatalogLoader, tableLoader, avroSchema);
    }

    public IcebergSourceBuilder withMonitorInterval(Duration interval) {
        this.monitorInterval = interval;
        return this;
    }

    public DataStreamSource<GenericRecord> buildSource(StreamExecutionEnvironment env) throws IOException {
        Table table;
        try (TableLoader loader = tableLoader) {
            loader.open();
            table = loader.loadTable();
        } catch (IOException e) {
            throw new IOException("Failed to load Iceberg table", e);
        }

        AvroGenericRecordReaderFunction readerFunction = AvroGenericRecordReaderFunction.fromTable(table);

        IcebergSource<GenericRecord> source = IcebergSource.<GenericRecord>builder()
                .tableLoader(tableLoader)
                .readerFunction(readerFunction)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .monitorInterval(monitorInterval)
                .streaming(true)
                .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
//                .startSnapshotId(table.currentSnapshot().snapshotId())
                .build();

        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Iceberg Source as Avro GenericRecord",
                new GenericRecordAvroTypeInfo(avroSchema)
        );
    }
}
