package sh.oso.salesforce.source.debezium;

import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryBeforeImageStore
        implements BeforeImageStore {

    private final Map<String, Struct> state =
            new ConcurrentHashMap<>();

    private String key(
            String sobject,
            String id) {

        return sobject + ":" + id;
    }

    @Override
    public Struct get(
            String sobject,
            String id) {

        return state.get(key(sobject, id));
    }

    @Override
    public void put(
            String sobject,
            String id,
            Struct value) {

        state.put(key(sobject, id), value);
    }

    @Override
    public void delete(
            String sobject,
            String id) {

        state.remove(key(sobject, id));
    }
}
