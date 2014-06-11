package com.gocalf.pig.aggregation;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class should never be used directly, use {@link com.gocalf.pig.aggregation.SumEach}.
 */
public class SumEachLong extends EvalFunc<Map<String, Long>>
        implements Algebraic, Accumulator<Map<String, Long>> {

    // EvalFunc

    @Override
    public Map<String, Long> exec(Tuple input) throws IOException {
        return sumEach(input);
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
            return tupleFactory.newTuple(sumEach(input));
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

    private Map<String, Long> intermediateSum = null;

    public void accumulate(Tuple b) throws IOException {
        Map<String, Long> sum = sumEach(b);
        if (sum == null) {
            return;
        }

        if (intermediateSum == null) {
            intermediateSum = new HashMap<String, Long>();
        }

        addEach(sum, intermediateSum);
    }

    public Map<String, Long> getValue() {
        return intermediateSum;
    }

    public void cleanup() {
        intermediateSum = null;
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

    protected static Map<String, Long> convert(Object value) throws ExecException {
        if (value == null) {
            return null;
        }

        Map<String, Long> longMap = new HashMap<String, Long>();
        Map rawMap = (Map) value;
        for (Object rawKey : rawMap.keySet()) {
            Object rawValue = rawMap.get(rawKey);
            if (rawValue == null) {
                // Ignore NULL value.
                continue;
            }

            Long x;
            if (rawValue instanceof Long) {
                x = (Long)rawValue;
            } else if (rawValue instanceof Integer) {
                x = ((Integer)rawValue).longValue();
            } else {
                throw new ExecException("Invalid value type in pig map: " + rawValue.getClass().getName());
            }
            longMap.put((String) rawKey, x);
        }

        return longMap.isEmpty() ? null : longMap;
    }

    protected static Map<String, Long> sumEach(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Long> sum = new HashMap<String, Long>();
        for (Tuple t : values) {
            Map<String, Long> map = convert(t.get(0));
            if (map == null) {
                continue;
            }
            addEach(map, sum);
        }

        return sum.isEmpty() ? null : sum;
    }

    protected static Map<String, Long> combine(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Long> sum = new HashMap<String, Long>();
        for (Tuple t : values) {
            @SuppressWarnings("unchecked")
            Map<String, Long> map = (Map<String, Long>) (t.get(0));
            if (map == null) {
                continue;
            }
            addEach(map, sum);
        }

        return sum.isEmpty() ? null : sum;
    }
}
