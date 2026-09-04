package sh.oso.salesforce.schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import sh.oso.salesforce.common.SalesforceException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Salesforce Pub/Sub Avro schemas and runtime values
 * into Kafka Connect schemas and runtime values.
 *
 * Production considerations:
 *
 * - Avro RECORD       -> Kafka Connect STRUCT
 * - Avro STRING       -> Kafka Connect STRING
 * - Avro ENUM         -> Kafka Connect STRING
 * - Avro ARRAY        -> Kafka Connect ARRAY
 * - Avro MAP          -> Kafka Connect MAP
 * - Avro BYTES/FIXED  -> Kafka Connect BYTES
 * - Avro INT          -> Kafka Connect INT32
 * - Avro LONG         -> Kafka Connect INT64
 * - Avro FLOAT        -> Kafka Connect FLOAT32
 * - Avro DOUBLE       -> Kafka Connect FLOAT64
 * - Avro BOOLEAN      -> Kafka Connect BOOLEAN
 * - nullable unions   -> optional Connect schema
 *
 * Kafka Connect does not natively support arbitrary Avro unions.
 * Unexpected multi-type unions are therefore represented as
 * OPTIONAL STRING, with runtime values safely converted to String.
 */
public final class AvroToConnect {

    private AvroToConnect() {
    }

    /**
     * Converts an Avro schema into a Kafka Connect schema.
     */
    public static Schema toConnectSchema(
            org.apache.avro.Schema avro) {

        if (avro == null) {
            throw new SalesforceException(
                    "Cannot convert null Avro schema"
            );
        }

        return toConnectSchema(avro, false);
    }

    /**
     * Converts Avro schema recursively.
     */
    private static Schema toConnectSchema(
            org.apache.avro.Schema avro,
            boolean optional) {

        if (avro == null) {
            throw new SalesforceException(
                    "Cannot convert null Avro schema"
            );
        }

        return switch (avro.getType()) {

            /*
             * ---------------------------------------------------------
             * RECORD
             * ---------------------------------------------------------
             */
            case RECORD -> {

                SchemaBuilder builder =
                        SchemaBuilder
                                .struct()
                                .name(avro.getFullName());

                if (optional) {
                    builder.optional();
                }

                for (org.apache.avro.Schema.Field field :
                        avro.getFields()) {

                    if (field == null) {
                        continue;
                    }

                    Schema fieldSchema =
                            toConnectSchema(
                                    field.schema(),
                                    false
                            );

                    builder.field(
                            field.name(),
                            fieldSchema
                    );
                }

                yield builder.build();
            }

            /*
             * ---------------------------------------------------------
             * UNION
             * ---------------------------------------------------------
             */
            case UNION -> unionToConnect(avro);

            /*
             * ---------------------------------------------------------
             * ARRAY
             * ---------------------------------------------------------
             */
            case ARRAY -> {

                Schema elementSchema =
                        toConnectSchema(
                                avro.getElementType(),
                                false
                        );

                SchemaBuilder builder =
                        SchemaBuilder.array(
                                elementSchema
                        );

                if (optional) {
                    builder.optional();
                }

                yield builder.build();
            }

            /*
             * ---------------------------------------------------------
             * MAP
             * ---------------------------------------------------------
             */
            case MAP -> {

                Schema valueSchema =
                        toConnectSchema(
                                avro.getValueType(),
                                false
                        );

                SchemaBuilder builder =
                        SchemaBuilder.map(
                                Schema.STRING_SCHEMA,
                                valueSchema
                        );

                if (optional) {
                    builder.optional();
                }

                yield builder.build();
            }

            /*
             * ---------------------------------------------------------
             * STRING / ENUM
             * ---------------------------------------------------------
             */
            case STRING, ENUM -> optional
                    ? Schema.OPTIONAL_STRING_SCHEMA
                    : Schema.STRING_SCHEMA;

            /*
             * ---------------------------------------------------------
             * BYTES / FIXED
             * ---------------------------------------------------------
             */
            case BYTES, FIXED -> optional
                    ? Schema.OPTIONAL_BYTES_SCHEMA
                    : Schema.BYTES_SCHEMA;

            /*
             * ---------------------------------------------------------
             * INTEGER
             * ---------------------------------------------------------
             */
            case INT -> optional
                    ? Schema.OPTIONAL_INT32_SCHEMA
                    : Schema.INT32_SCHEMA;

            /*
             * ---------------------------------------------------------
             * LONG
             * ---------------------------------------------------------
             */
            case LONG -> optional
                    ? Schema.OPTIONAL_INT64_SCHEMA
                    : Schema.INT64_SCHEMA;

            /*
             * ---------------------------------------------------------
             * FLOAT
             * ---------------------------------------------------------
             */
            case FLOAT -> optional
                    ? Schema.OPTIONAL_FLOAT32_SCHEMA
                    : Schema.FLOAT32_SCHEMA;

            /*
             * ---------------------------------------------------------
             * DOUBLE
             * ---------------------------------------------------------
             */
            case DOUBLE -> optional
                    ? Schema.OPTIONAL_FLOAT64_SCHEMA
                    : Schema.FLOAT64_SCHEMA;

            /*
             * ---------------------------------------------------------
             * BOOLEAN
             * ---------------------------------------------------------
             */
            case BOOLEAN -> optional
                    ? Schema.OPTIONAL_BOOLEAN_SCHEMA
                    : Schema.BOOLEAN_SCHEMA;

            /*
             * Standalone NULL is not a useful Connect field schema.
             */
            case NULL -> throw new SalesforceException(
                    "Standalone Avro NULL schema is not supported"
            );
        };
    }

    /**
     * Converts Avro UNION into Kafka Connect schema.
     *
     * Normal Salesforce nullable field:
     *
     *     ["null", "string"]
     *
     * becomes:
     *
     *     OPTIONAL STRING
     *
     * Normal Salesforce nested nullable record:
     *
     *     ["null", "record"]
     *
     * becomes:
     *
     *     OPTIONAL STRUCT
     *
     * Arbitrary multi-type unions cannot be represented natively
     * by Kafka Connect. Those are safely represented as STRING.
     */
    private static Schema unionToConnect(
            org.apache.avro.Schema union) {

        List<org.apache.avro.Schema> nonNull =
                new ArrayList<>();

        for (org.apache.avro.Schema type :
                union.getTypes()) {

            if (type == null) {
                continue;
            }

            if (type.getType() !=
                    org.apache.avro.Schema.Type.NULL) {

                nonNull.add(type);
            }
        }

        /*
         * ["null"]
         */
        if (nonNull.isEmpty()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        /*
         * ["null", X]
         */
        if (nonNull.size() == 1) {

            return toConnectSchema(
                    nonNull.get(0),
                    true
            );
        }

        /*
         * Multi-type union.
         *
         * Kafka Connect has no native equivalent.
         *
         * IMPORTANT:
         *
         * Runtime conversion below MUST also convert the actual
         * GenericRecord/List/Map into String.
         *
         * This prevents:
         *
         * Connect schema = STRING
         * Java object    = GenericData.Record
         *
         * which caused the original production failure.
         */
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    /**
     * Converts an Avro runtime value into a Java object compatible
     * with the supplied Kafka Connect schema.
     */
    public static Object toConnectValue(
            Schema connectSchema,
            Object avroValue) {

        if (connectSchema == null) {
            throw new SalesforceException(
                    "Kafka Connect schema cannot be null"
            );
        }

        if (avroValue == null) {
            return null;
        }

        try {

            return switch (connectSchema.type()) {

                case STRUCT ->
                        toStruct(
                                connectSchema,
                                avroValue
                        );

                case ARRAY ->
                        toArray(
                                connectSchema,
                                avroValue
                        );

                case MAP ->
                        toMap(
                                connectSchema,
                                avroValue
                        );

                case STRING ->
                        toStringValue(
                                avroValue
                        );

                case BYTES ->
                        toBytesValue(
                                avroValue
                        );

                case INT32 ->
                        toIntValue(
                                connectSchema,
                                avroValue
                        );

                case INT64 ->
                        toLongValue(
                                connectSchema,
                                avroValue
                        );

                case FLOAT32 ->
                        toFloatValue(
                                connectSchema,
                                avroValue
                        );

                case FLOAT64 ->
                        toDoubleValue(
                                connectSchema,
                                avroValue
                        );

                case BOOLEAN ->
                        toBooleanValue(
                                connectSchema,
                                avroValue
                        );

                /*
                 * The Salesforce converter currently does not
                 * explicitly create INT8 or INT16 schemas.
                 *
                 * Keep them here so this class remains safe if
                 * another caller supplies such a Connect schema.
                 */
                case INT8 ->
                        toNumberValue(
                                connectSchema,
                                avroValue
                        );

                case INT16 ->
                        toNumberValue(
                                connectSchema,
                                avroValue
                        );
            };

        } catch (SalesforceException ex) {

            throw ex;

        } catch (Exception ex) {

            throw new SalesforceException(
                    "Failed converting Avro value to Kafka Connect value. " +
                    "ConnectType=" +
                    connectSchema.type() +
                    ", RuntimeType=" +
                    avroValue.getClass().getName(),
                    ex
            );
        }
    }

    /**
     * Avro GenericRecord -> Kafka Connect Struct.
     */
    private static Struct toStruct(
            Schema connectSchema,
            Object avroValue) {

        if (!(avroValue instanceof GenericRecord record)) {

            throw new SalesforceException(
                    "Expected Avro GenericRecord for Connect STRUCT " +
                    "but received " +
                    avroValue.getClass().getName()
            );
        }

        Struct struct =
                new Struct(connectSchema);

        for (org.apache.kafka.connect.data.Field field :
                connectSchema.fields()) {

            org.apache.avro.Schema.Field avroField =
                    record.getSchema().getField(
                            field.name()
                    );

            /*
             * Salesforce CDC may omit unchanged fields.
             */
            if (avroField == null) {
                continue;
            }

            Object avroFieldValue =
                    record.get(field.name());

            Object connectValue =
                    toConnectValue(
                            field.schema(),
                            avroFieldValue
                    );

            /*
             * Null is valid only for optional fields.
             *
             * CDC fields generated by SourceRecordFactory are
             * optional, so this is normally safe.
             */
            if (connectValue == null) {

                if (field.schema().isOptional()) {
                    struct.put(
                            field,
                            null
                    );
                }

                continue;
            }

            struct.put(
                    field,
                    connectValue
            );
        }

        return struct;
    }

    /**
     * Avro ARRAY -> Connect ARRAY.
     */
    private static List<Object> toArray(
            Schema connectSchema,
            Object avroValue) {

        if (!(avroValue instanceof List<?> list)) {

            throw new SalesforceException(
                    "Expected List for Connect ARRAY but received " +
                    avroValue.getClass().getName()
            );
        }

        List<Object> result =
                new ArrayList<>(list.size());

        for (Object element : list) {

            result.add(
                    toConnectValue(
                            connectSchema.valueSchema(),
                            element
                    )
            );
        }

        return result;
    }

    /**
     * Avro MAP -> Connect MAP.
     */
    private static Map<Object, Object> toMap(
            Schema connectSchema,
            Object avroValue) {

        if (!(avroValue instanceof Map<?, ?> map)) {

            throw new SalesforceException(
                    "Expected Map for Connect MAP but received " +
                    avroValue.getClass().getName()
            );
        }

        Map<Object, Object> result =
                new LinkedHashMap<>();

        for (Map.Entry<?, ?> entry :
                map.entrySet()) {

            Object key =
                    entry.getKey();

            String stringKey =
                    key == null
                            ? null
                            : key.toString();

            Object value =
                    toConnectValue(
                            connectSchema.valueSchema(),
                            entry.getValue()
                    );

            result.put(
                    stringKey,
                    value
            );
        }

        return result;
    }

    /**
     * Converts all values expected by a Connect STRING schema.
     *
     * This is the important fix for:
     *
     * Invalid Java object for schema with type STRING:
     * GenericData$Record
     */
    private static String toStringValue(
            Object avroValue) {

        if (avroValue instanceof String) {
            return (String) avroValue;
        }

        if (avroValue instanceof Utf8) {
            return avroValue.toString();
        }

        if (avroValue instanceof GenericData.EnumSymbol) {
            return avroValue.toString();
        }

        if (avroValue instanceof CharSequence) {
            return avroValue.toString();
        }

        /*
         * Multi-type union containing RECORD.
         *
         * Never return GenericRecord directly because Connect STRING
         * validation will reject it.
         */
        if (avroValue instanceof GenericRecord) {
            return avroValue.toString();
        }

        /*
         * Multi-type union containing ARRAY.
         */
        if (avroValue instanceof List<?>) {
            return avroValue.toString();
        }

        /*
         * Multi-type union containing MAP.
         */
        if (avroValue instanceof Map<?, ?>) {
            return avroValue.toString();
        }

        /*
         * ByteBuffer is also safely represented as String.
         */
        if (avroValue instanceof ByteBuffer buffer) {

            ByteBuffer duplicate =
                    buffer.duplicate();

            byte[] bytes =
                    new byte[duplicate.remaining()];

            duplicate.get(bytes);

            return new String(
                    bytes,
                    java.nio.charset.StandardCharsets.UTF_8
            );
        }

        /*
         * Last-resort scalar conversion.
         *
         * Crucially, this always returns String.
         */
        return String.valueOf(avroValue);
    }

    /**
     * Avro BYTES/FIXED -> ByteBuffer.
     */
    private static ByteBuffer toBytesValue(
            Object avroValue) {

        if (avroValue instanceof ByteBuffer) {
            return (ByteBuffer) avroValue;
        }

        return ByteBuffer.wrap(
                toBytes(avroValue)
        );
    }

    /**
     * Avro INT -> Integer.
     */
    private static Integer toIntValue(
            Schema connectSchema,
            Object avroValue) {

        if (avroValue instanceof Integer) {
            return (Integer) avroValue;
        }

        if (avroValue instanceof Number) {
            return ((Number) avroValue).intValue();
        }

        throw incompatible(
                connectSchema,
                avroValue,
                "Integer"
        );
    }

    /**
     * Avro LONG -> Long.
     */
    private static Long toLongValue(
            Schema connectSchema,
            Object avroValue) {

        if (avroValue instanceof Long) {
            return (Long) avroValue;
        }

        if (avroValue instanceof Number) {
            return ((Number) avroValue).longValue();
        }

        throw incompatible(
                connectSchema,
                avroValue,
                "Long"
        );
    }

    /**
     * Avro FLOAT -> Float.
     */
    private static Float toFloatValue(
            Schema connectSchema,
            Object avroValue) {

        if (avroValue instanceof Float) {
            return (Float) avroValue;
        }

        if (avroValue instanceof Number) {
            return ((Number) avroValue).floatValue();
        }

        throw incompatible(
                connectSchema,
                avroValue,
                "Float"
        );
    }

    /**
     * Avro DOUBLE -> Double.
     */
    private static Double toDoubleValue(
            Schema connectSchema,
            Object avroValue) {

        if (avroValue instanceof Double) {
            return (Double) avroValue;
        }

        if (avroValue instanceof Number) {
            return ((Number) avroValue).doubleValue();
        }

        throw incompatible(
                connectSchema,
                avroValue,
                "Double"
        );
    }

    /**
     * BOOLEAN -> Boolean.
     */
    private static Boolean toBooleanValue(
            Schema connectSchema,
            Object avroValue) {

        if (avroValue instanceof Boolean) {
            return (Boolean) avroValue;
        }

        throw incompatible(
                connectSchema,
                avroValue,
                "Boolean"
        );
    }

    /**
     * INT8 / INT16 defensive conversion.
     */
    private static Object toNumberValue(
            Schema connectSchema,
            Object avroValue) {

        if (!(avroValue instanceof Number)) {

            throw incompatible(
                    connectSchema,
                    avroValue,
                    "Number"
            );
        }

        Number number =
                (Number) avroValue;

        return switch (connectSchema.type()) {

            case INT8 ->
                    number.byteValue();

            case INT16 ->
                    number.shortValue();

            default ->
                    number;
        };
    }

    /**
     * Converts Avro FIXED / BYTES / ByteBuffer.
     */
    private static byte[] toBytes(
            Object avroValue) {

        if (avroValue instanceof GenericData.Fixed) {

            GenericData.Fixed fixed =
                    (GenericData.Fixed) avroValue;

            return fixed.bytes();
        }

        if (avroValue instanceof byte[]) {
            return (byte[]) avroValue;
        }

        if (avroValue instanceof ByteBuffer) {

            ByteBuffer duplicate =
                    ((ByteBuffer) avroValue).duplicate();

            byte[] bytes =
                    new byte[duplicate.remaining()];

            duplicate.get(bytes);

            return bytes;
        }

        throw new SalesforceException(
                "Cannot convert " +
                avroValue.getClass().getName() +
                " to bytes"
        );
    }

    /**
     * Creates a consistent schema/value mismatch exception.
     */
    private static SalesforceException incompatible(
            Schema connectSchema,
            Object avroValue,
            String expected) {

        return new SalesforceException(
                "Avro/Connect schema mismatch. " +
                "ConnectType=" +
                connectSchema.type() +
                ", Expected=" +
                expected +
                ", RuntimeType=" +
                avroValue.getClass().getName()
        );
    }
}