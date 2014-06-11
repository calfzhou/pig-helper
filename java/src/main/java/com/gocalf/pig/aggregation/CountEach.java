package com.gocalf.pig.aggregation;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountEach extends EvalFunc<Map<String, Long>>
        implements Algebraic, Accumulator<Map<String, Long>> {

    // EvalFunc

    @Override
    public Map<String, Long> exec(Tuple input) throws IOException {
        return countEach(input);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema valueSchema = new Schema(new Schema.FieldSchema(null, DataType.LONG));
        try {
            return new Schema(new Schema.FieldSchema(null, valueSchema, DataType.MAP));
        } catch (FrontendException e) {
            return new Schema(new Schema.FieldSchema(null, DataType.MAP));
        }
    }

    // Algebraic

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    public static class Initial extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            return tupleFactory.newTuple(countEach(input));
        }
    }

    public static class Intermediate extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            return tupleFactory.newTuple(combine(input));
        }
    }

    public static class Final extends EvalFunc<Map<String, Long>> {

        @Override
        public Map<String, Long> exec(Tuple input) throws IOException {
            return combine(input);
        }
    }

    // Accumulator

    private Map<String, Long> intermediateCount = null;

    public void accumulate(Tuple b) throws IOException {
        Map<String, Long> count = countEach(b);
        if (count == null) {
            return;
        }

        if (intermediateCount == null) {
            intermediateCount = new HashMap<String, Long>();
        }

        addEach(count, intermediateCount);
    }

    public Map<String, Long> getValue() {
        return intermediateCount;
    }

    public void cleanup() {
        intermediateCount = null;
    }

    // Other

    protected static void addEach(Map<String, Long> from, Map<String, Long> into) {
        if (from == null) {
            return;
        }

        for (String key : from.keySet()) {
            Long value = from.get(key);
            if (into.containsKey(key)) {
                into.put(key, into.get(key) + value);
            } else {
                into.put(key, value);
            }
        }
    }

    protected static Map<String, Long> countEach(Tuple input) throws IOException {
        DataBag values = (DataBag) input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Long> count = new HashMap<String, Long>();
        for (Tuple t : values) {
            Object item = t.get(0);
            if (item == null) {
                continue;
            }
            String key = item.toString();
            if (count.containsKey(key)) {
                count.put(key, count.get(key) + 1L);
            } else {
                count.put(key, 1L);
            }
        }

        return count.isEmpty() ? null : count;
    }

    protected static Map<String, Long> combine(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Long> count = new HashMap<String, Long>();
        for (Tuple t : values) {
            @SuppressWarnings("unchecked")
            Map<String, Long> map = (Map<String, Long>) (t.get(0));
            if (map == null) {
                continue;
            }
            addEach(map, count);
        }

        return count.isEmpty() ? null : count;
    }
}
