package sh.oso.salesforce.source.debezium;

import org.apache.kafka.connect.data.Struct;

public interface BeforeImageStore {

    Struct get(
            String sobject,
            String id);

    void put(
            String sobject,
            String id,
            Struct value);

    void delete(
            String sobject,
            String id);
}