package sh.oso.salesforce.pubsub;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Helpers for Change Data Capture events.
 *
 * Responsibilities:
 *
 * 1. Extract ChangeEventHeader from a CDC event.
 * 2. Expand Salesforce changedFields/nulledFields/diffFields bitmap
 *    entries into field names.
 *
 * Salesforce bitmap formats:
 *
 *   0x440000A0
 *       Top-level field bitmap.
 *
 *   5-0x08
 *       Nested bitmap where 5 is the top-level field index.
 *
 * Example:
 *
 *   BillingAddress.Street
 *
 * Production requirements:
 *
 * - Never call Schema#getFields() on a non-RECORD schema.
 * - Safely unwrap nullable Avro UNION schemas.
 * - Validate bitmap parent indexes.
 * - Ignore malformed/unknown bitmap entries instead of killing
 *   the Kafka Connect task.
 * - Preserve the original field ordering.
 */
public final class ChangeEventUtils {

    public static final String HEADER_FIELD = "ChangeEventHeader";

    private ChangeEventUtils() {
    }

    /**
     * Returns the ChangeEventHeader record when the event contains one.
     */
    public static Optional<GenericRecord> changeEventHeader(
            GenericRecord event) {

        if (event == null) {
            return Optional.empty();
        }

        Schema eventSchema = event.getSchema();

        if (eventSchema == null) {
            return Optional.empty();
        }

        Schema.Field headerField =
                eventSchema.getField(HEADER_FIELD);

        if (headerField == null) {
            return Optional.empty();
        }

        Object header =
                event.get(HEADER_FIELD);

        return header instanceof GenericRecord record
                ? Optional.of(record)
                : Optional.empty();
    }

    /**
     * Expands Salesforce CDC bitmap entries into field names.
     *
     * Supported formats:
     *
     *   0x440000A0
     *
     *       Bitmap over top-level fields.
     *
     *   5-0x08
     *
     *       Bitmap over nested fields belonging to top-level
     *       field index 5.
     *
     * Invalid entries are ignored rather than allowing the
     * Kafka Connect task to fail.
     */
    public static List<String> expandBitmapFields(
            Schema eventSchema,
            List<?> bitmapEntries) {

        List<String> out =
                new ArrayList<>();

        if (eventSchema == null ||
                bitmapEntries == null ||
                bitmapEntries.isEmpty()) {

            return out;
        }

        /*
         * A bitmap can only be resolved against a RECORD schema.
         *
         * Protect against:
         *
         *   Schema.getFields()
         *
         * being called on STRING/ARRAY/MAP/etc.
         */
        Schema root =
                unwrapNullable(eventSchema);

        if (!isRecord(root)) {
            return out;
        }

        for (Object entryObj : bitmapEntries) {

            if (entryObj == null) {
                continue;
            }

            String entry =
                    entryObj.toString();

            if (entry == null ||
                    entry.isBlank()) {

                continue;
            }

            entry =
                    entry.trim();

            try {

                int dash =
                        entry.indexOf('-');

                /*
                 * -----------------------------------------------------
                 * TOP-LEVEL BITMAP
                 * -----------------------------------------------------
                 *
                 * Example:
                 *
                 * 0x440000A0
                 */
                if (dash < 0) {

                    appendFields(
                            root,
                            entry,
                            null,
                            out
                    );

                    continue;
                }

                /*
                 * -----------------------------------------------------
                 * NESTED BITMAP
                 * -----------------------------------------------------
                 *
                 * Example:
                 *
                 * 5-0x08
                 */
                String parentIndexText =
                        entry.substring(
                                0,
                                dash
                        ).trim();

                String nestedBitmap =
                        entry.substring(
                                dash + 1
                        ).trim();

                if (parentIndexText.isEmpty() ||
                        nestedBitmap.isEmpty()) {

                    continue;
                }

                int parentIndex =
                        Integer.parseInt(
                                parentIndexText
                        );

                List<Schema.Field> fields =
                        root.getFields();

                /*
                 * Never allow an invalid Salesforce bitmap index
                 * to throw IndexOutOfBoundsException.
                 */
                if (parentIndex < 0 ||
                        parentIndex >= fields.size()) {

                    continue;
                }

                Schema.Field parent =
                        fields.get(parentIndex);

                if (parent == null) {
                    continue;
                }

                Schema nested =
                        unwrapNullable(
                                parent.schema()
                        );

                /*
                 * A nested bitmap only makes sense for a RECORD.
                 *
                 * For example:
                 *
                 * BillingAddress -> RECORD
                 *
                 * but:
                 *
                 * Name -> STRING
                 *
                 * If Salesforce sends a nested bitmap against a scalar,
                 * simply ignore it.
                 */
                if (!isRecord(nested)) {
                    continue;
                }

                appendFields(
                        nested,
                        nestedBitmap,
                        parent.name(),
                        out
                );

            } catch (NumberFormatException ignored) {

                /*
                 * Invalid bitmap/index.
                 *
                 * Do not kill the source task.
                 */
            } catch (IllegalArgumentException ignored) {

                /*
                 * Invalid hexadecimal bitmap or malformed value.
                 *
                 * Do not kill the source task.
                 */
            }
        }

        return out;
    }

    /**
     * Adds fields represented by a bitmap.
     *
     * IMPORTANT:
     *
     * This method ONLY calls getFields() after confirming that the
     * schema is a RECORD.
     *
     * This is the direct fix for:
     *
     *   AvroRuntimeException:
     *   Not a record: "string"
     */
    private static void appendFields(
            Schema recordSchema,
            String hexBitmap,
            String prefix,
            List<String> out) {

        if (recordSchema == null ||
                hexBitmap == null ||
                hexBitmap.isBlank() ||
                out == null) {

            return;
        }

        Schema schema =
                unwrapNullable(recordSchema);

        /*
         * NEVER call getFields() on STRING, INT, ARRAY, MAP, etc.
         */
        if (!isRecord(schema)) {
            return;
        }

        BigInteger bits;

        try {

            String normalized =
                    normalizeHexBitmap(
                            hexBitmap
                    );

            if (normalized.isEmpty()) {
                return;
            }

            bits =
                    new BigInteger(
                            normalized,
                            16
                    );

        } catch (NumberFormatException ignored) {

            /*
             * Invalid Salesforce bitmap.
             *
             * Ignore the entry instead of failing the connector.
             */
            return;
        }

        List<Schema.Field> fields =
                schema.getFields();

        for (int i = 0;
             i < fields.size();
             i++) {

            /*
             * Bit i corresponds to field i.
             */
            if (!bits.testBit(i)) {
                continue;
            }

            Schema.Field field =
                    fields.get(i);

            if (field == null) {
                continue;
            }

            String name =
                    field.name();

            if (name == null ||
                    name.isBlank()) {

                continue;
            }

            String fullName =
                    prefix == null ||
                            prefix.isBlank()
                        ? name
                        : prefix + "." + name;

            out.add(fullName);
        }
    }

    /**
     * Normalizes Salesforce bitmap values.
     *
     * Supports:
     *
     *   0x440000A0
     *   0X440000A0
     *   440000A0
     *   whitespace around the value
     */
    private static String normalizeHexBitmap(
            String bitmap) {

        if (bitmap == null) {
            return "";
        }

        String value =
                bitmap.trim();

        if (value.isEmpty()) {
            return "";
        }

        if (value.startsWith("0x") ||
                value.startsWith("0X")) {

            value =
                    value.substring(2);
        }

        return value.trim();
    }

    /**
     * Safely unwraps a nullable Avro UNION.
     *
     * Example:
     *
     *   ["null", "string"]
     *
     * returns:
     *
     *   "string"
     *
     * Example:
     *
     *   ["null", BillingAddressRecord]
     *
     * returns:
     *
     *   BillingAddressRecord
     *
     * For multi-type unions, the first non-null schema is returned.
     *
     * This matches the behavior of the existing implementation while
     * adding null safety.
     */
    public static Schema unwrapNullable(
            Schema schema) {

        if (schema == null) {
            return null;
        }

        if (schema.getType() !=
                Schema.Type.UNION) {

            return schema;
        }

        for (Schema type :
                schema.getTypes()) {

            if (type == null) {
                continue;
            }

            if (type.getType() !=
                    Schema.Type.NULL) {

                return type;
            }
        }

        return schema;
    }

    /**
     * Returns true only when the schema is a RECORD.
     */
    private static boolean isRecord(
            Schema schema) {

        return schema != null &&
                schema.getType() ==
                        Schema.Type.RECORD;
    }
}