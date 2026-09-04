package sh.oso.salesforce.source;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.salesforce.pubsub.ChangeEventUtils;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.schema.AvroToConnect;
import sh.oso.salesforce.schema.CsvValueConverter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds Kafka Connect SourceRecords from:
 *
 * 1. Salesforce Pub/Sub CDC events
 * 2. Salesforce Bulk CSV rows
 *
 * Production considerations:
 *
 * - CDC and Bulk schemas are cached independently.
 * - CDC fields are nullable because Salesforce CDC only sends
 *   changed fields.
 * - Nested Salesforce records are converted to Kafka Connect STRUCTs.
 * - Kafka Connect values are always validated against their schema
 *   before being inserted into Struct.
 * - CDC metadata is kept in Kafka Connect headers.
 * - _ObjectType and _EventType remain in the record value for
 *   downstream compatibility.
 */
final class SourceRecordFactory {

    static final String EVENT_TYPE_FIELD = "_EventType";
    static final String OBJECT_TYPE_FIELD = "_ObjectType";

    private final String sobject;
    private final String topic;
    private final Map<String, Object> partition;

    /*
     * IMPORTANT:
     *
     * Do NOT combine these into Map<Object, Schema>.
     *
     * CDC uses Avro Schema as the key.
     * Bulk uses Kafka Connect Schema as the key.
     *
     * Strongly typed caches avoid computeIfAbsent() type inference
     * problems and prevent accidental cache collisions.
     */
    private final Map<org.apache.avro.Schema, Schema> eventSchemaCache =
            new ConcurrentHashMap<>();

    private final Map<Schema, Schema> bulkSchemaCache =
            new ConcurrentHashMap<>();

    SourceRecordFactory(String sobject, String topic) {
        this.sobject = sobject;
        this.topic = topic;
        this.partition = SObjectOffset.partition(sobject);
    }

    /**
     * Converts Salesforce CDC event to Kafka Connect SourceRecord.
     *
     * The returned record is the actual data record.
     * DELETE tombstones are created separately using tombstone().
     */
    SourceRecord fromChangeEvent(
            DecodedEvent event,
            GenericRecord header,
            String changeType,
            String recordId,
            Map<String, Object> offset) {

        if (event == null) {
            throw new IllegalArgumentException(
                    "Salesforce CDC event cannot be null"
            );
        }

        if (event.payload() == null) {
            throw new IllegalArgumentException(
                    "Salesforce CDC event payload cannot be null"
            );
        }

        org.apache.avro.Schema avroSchema =
                event.payload().getSchema();

        /*
         * Strongly typed cache.
         *
         * This fixes:
         *
         * incompatible types:
         * Object cannot be converted to org.apache.avro.Schema
         */
        Schema valueSchema =
                eventSchemaCache.computeIfAbsent(
                        avroSchema,
                        this::buildEventValueSchema
                );

        Struct value =
                new Struct(valueSchema);

        /*
         * Populate every field defined by the Connect schema.
         *
         * This is important because Salesforce CDC events may contain
         * only changed fields.
         */
        for (Field field : valueSchema.fields()) {

            String fieldName = field.name();

            switch (fieldName) {

                case EVENT_TYPE_FIELD -> {

                    value.put(
                            field,
                            eventTypeFor(changeType)
                    );
                }

                case OBJECT_TYPE_FIELD -> {

                    value.put(
                            field,
                            sobject
                    );
                }

                case "Id" -> {

                    /*
                     * Id is supplied separately by the CDC pipeline.
                     */
                    value.put(
                            field,
                            recordId
                    );
                }

                default -> {

                    org.apache.avro.Schema.Field avroField =
                            avroSchema.getField(fieldName);

                    /*
                     * Field may not exist in a particular schema.
                     */
                    if (avroField == null) {
                        continue;
                    }

                    Object avroValue =
                            event.payload().get(fieldName);

                    /*
                     * CRITICAL:
                     *
                     * Always convert the Avro runtime value to a Java
                     * value compatible with the Kafka Connect schema.
                     *
                     * This prevents:
                     *
                     * Invalid Java object for schema with type STRING:
                     * GenericData$Record
                     */
                    Object connectValue =
                            AvroToConnect.toConnectValue(
                                    field.schema(),
                                    avroValue
                            );

                    /*
                     * Salesforce CDC can omit fields or explicitly
                     * send null.
                     */
                    if (connectValue == null) {

                        if (field.schema().isOptional()) {
                            value.put(field, null);
                        }

                        continue;
                    }

                    value.put(
                            field,
                            connectValue
                    );
                }
            }
        }

        ConnectHeaders headers =
                cdcHeaders(
                        event,
                        header,
                        changeType
                );

        return new SourceRecord(
                partition,
                offset,
                topic,
                null,

                /*
                 * Kafka message key
                 */
                Schema.STRING_SCHEMA,
                recordId,

                /*
                 * Kafka message value
                 */
                valueSchema,
                value,

                null,
                headers
        );
    }

    /**
     * Creates a Kafka tombstone for DELETE events.
     */
    SourceRecord tombstone(
            String recordId,
            Map<String, Object> offset) {

        return new SourceRecord(
                partition,
                offset,
                topic,
                null,

                Schema.STRING_SCHEMA,
                recordId,

                null,
                null
        );
    }

    /**
     * Converts Bulk CSV row to Kafka Connect SourceRecord.
     */
    SourceRecord fromBulkRow(
            Schema describeSchema,
            Map<String, String> row,
            String eventType,
            Map<String, Object> offset) {

        if (describeSchema == null) {
            throw new IllegalArgumentException(
                    "Bulk describe schema cannot be null"
            );
        }

        if (row == null) {
            throw new IllegalArgumentException(
                    "Bulk CSV row cannot be null"
            );
        }

        /*
         * Strongly typed Bulk schema cache.
         *
         * This fixes:
         *
         * incompatible types:
         * Object cannot be converted to
         * org.apache.kafka.connect.data.Schema
         */
        Schema valueSchema =
                bulkSchemaCache.computeIfAbsent(
                        describeSchema,
                        this::buildBulkValueSchema
                );

        Struct value =
                new Struct(valueSchema);

        for (Map.Entry<String, String> entry :
                row.entrySet()) {

            String fieldName =
                    entry.getKey();

            Field field =
                    valueSchema.field(fieldName);

            /*
             * Ignore CSV columns not represented in the schema.
             */
            if (field == null) {
                continue;
            }

            Object convertedValue =
                    CsvValueConverter.convert(
                            field.schema(),
                            fieldName,
                            entry.getValue()
                    );

            /*
             * Kafka Connect Struct accepts null only when the
             * schema is optional.
             */
            if (convertedValue == null) {

                if (field.schema().isOptional()) {
                    value.put(field, null);
                }

                continue;
            }

            value.put(
                    field,
                    convertedValue
            );
        }

        value.put(
                EVENT_TYPE_FIELD,
                eventType
        );

        value.put(
                OBJECT_TYPE_FIELD,
                sobject
        );

        String recordId =
                row.get("Id");

        return new SourceRecord(
                partition,
                offset,
                topic,
                null,

                Schema.STRING_SCHEMA,
                recordId,

                valueSchema,
                value
        );
    }

    /**
     * Builds Kafka Connect value schema for Salesforce CDC.
     */
    private Schema buildEventValueSchema(
            org.apache.avro.Schema avroSchema) {

        SchemaBuilder builder =
                SchemaBuilder
                        .struct()
                        .name(sobject);

        /*
         * Id is always represented as String.
         */
        builder.field(
                "Id",
                Schema.STRING_SCHEMA
        );

        for (org.apache.avro.Schema.Field field :
                avroSchema.getFields()) {

            /*
             * ChangeEventHeader is transported through Kafka
             * Connect headers and should not be duplicated in
             * the record value.
             */
            if (ChangeEventUtils.HEADER_FIELD.equals(
                    field.name())) {

                continue;
            }

            /*
             * Salesforce CDC sends only changed fields.
             *
             * Therefore every CDC field must be optional.
             */
            Schema fieldSchema =
                    AvroToConnect.toConnectSchema(
                            nullable(field.schema())
                    );

            builder.field(
                    field.name(),
                    fieldSchema
            );
        }

        /*
         * Compatibility fields required by downstream consumers.
         */
        builder.field(
                EVENT_TYPE_FIELD,
                Schema.OPTIONAL_STRING_SCHEMA
        );

        builder.field(
                OBJECT_TYPE_FIELD,
                Schema.OPTIONAL_STRING_SCHEMA
        );

        return builder.build();
    }

    /**
     * Builds Kafka Connect value schema for Bulk data.
     */
    private Schema buildBulkValueSchema(
            Schema describeSchema) {

        SchemaBuilder builder =
                SchemaBuilder
                        .struct()
                        .name(sobject);

        for (Field field :
                describeSchema.fields()) {

            builder.field(
                    field.name(),
                    field.schema()
            );
        }

        builder.field(
                EVENT_TYPE_FIELD,
                Schema.OPTIONAL_STRING_SCHEMA
        );

        builder.field(
                OBJECT_TYPE_FIELD,
                Schema.OPTIONAL_STRING_SCHEMA
        );

        return builder.build();
    }

    /**
     * Makes an Avro field nullable for Salesforce CDC.
     *
     * Example:
     *
     * String
     *
     * becomes:
     *
     * [null, string]
     *
     * Salesforce CDC can omit unchanged fields, so the
     * corresponding Kafka Connect field must be optional.
     */
    private static org.apache.avro.Schema nullable(
            org.apache.avro.Schema schema) {

        if (schema == null) {
            throw new IllegalArgumentException(
                    "Avro schema cannot be null"
            );
        }

        if (schema.getType() ==
                org.apache.avro.Schema.Type.UNION) {

            return schema;
        }

        return org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.NULL
                ),
                schema
        );
    }

    /**
     * Converts Salesforce CDC metadata into Kafka Connect headers.
     */
    private ConnectHeaders cdcHeaders(
            DecodedEvent event,
            GenericRecord header,
            String changeType) {

        ConnectHeaders headers =
                new ConnectHeaders();

        if (header == null) {
            return headers;
        }

        headers.addString(
                "sf.change.type",
                changeType
        );

        headers.addString(
                "sf.entity",
                string(header.get("entityName"))
        );

        Object commitTimestamp =
                header.get("commitTimestamp");

        if (commitTimestamp instanceof Number) {

            headers.addLong(
                    "sf.commit.timestamp",
                    ((Number) commitTimestamp).longValue()
            );
        }

        Object commitNumber =
                header.get("commitNumber");

        if (commitNumber instanceof Number) {

            headers.addLong(
                    "sf.commit.number",
                    ((Number) commitNumber).longValue()
            );
        }

        headers.addString(
                "sf.transaction.key",
                string(header.get("transactionKey"))
        );

        Object changedFieldsObject =
                header.get("changedFields");

        if (changedFieldsObject instanceof List<?>) {

            List<String> changed =
                    ChangeEventUtils.expandBitmapFields(
                            event.payload().getSchema(),
                            (List<?>) changedFieldsObject
                    );

            headers.addString(
                    "sf.changed.fields",
                    String.join(",", changed)
            );
        }

        Object nulledFieldsObject =
                header.get("nulledFields");

        if (nulledFieldsObject instanceof List<?>) {

            List<String> nulled =
                    ChangeEventUtils.expandBitmapFields(
                            event.payload().getSchema(),
                            (List<?>) nulledFieldsObject
                    );

            headers.addString(
                    "sf.nulled.fields",
                    String.join(",", nulled)
            );
        }

        return headers;
    }

    /**
     * Converts Salesforce ChangeEvent change type into the
     * event type expected by downstream consumers.
     */
    static String eventTypeFor(
            String changeType) {

        if (changeType == null) {
            return null;
        }

        return switch (changeType) {

            case "CREATE", "UNDELETE" ->
                    "created";

            case "UPDATE" ->
                    "updated";

            case "DELETE" ->
                    "deleted";

            default ->
                    changeType.toLowerCase();
        };
    }

    /**
     * Safe Object -> String conversion.
     */
    private static String string(
            Object value) {

        return value == null
                ? null
                : value.toString();
    }
}