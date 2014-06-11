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
public class SumEachDouble extends EvalFunc<Map<String, Double>>
        implements Algebraic, Accumulator<Map<String, Double>> {

    // EvalFunc

    @Override
    public Map<String, Double> exec(Tuple input) throws IOException {
        return sumEach(input);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema valueSchema = new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
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

    public static class Final extends EvalFunc<Map<String, Double>> {

        @Override
        public Map<String, Double> exec(Tuple input) throws IOException {
            return combine(input);
        }
    }

    // Accumulator

    private Map<String, Double> intermediateSum = null;

    public void accumulate(Tuple b) throws IOException {
        Map<String, Double> sum = sumEach(b);
        if (sum == null) {
            return;
        }

        if (intermediateSum == null) {
            intermediateSum = new HashMap<String, Double>();
        }

        addEach(sum, intermediateSum);
    }

    public Map<String, Double> getValue() {
        return intermediateSum;
    }

    public void cleanup() {
        intermediateSum = null;
    }

    // Other

    protected static void addEach(Map<String, Double> from, Map<String, Double> into) {
        if (from == null) {
            return;
        }

        for (String key : from.keySet()) {
            Double value = from.get(key);
            if (into.containsKey(key)) {
                into.put(key, into.get(key) + value);
            } else {
                into.put(key, value);
            }
        }
    }

    protected static Map<String, Double> convert(Object value) throws ExecException {
        if (value == null) {
            return null;
        }

        Map<String, Double> doubleMap = new HashMap<String, Double>();
        Map rawMap = (Map) value;
        for (Object rawKey : rawMap.keySet()) {
            Object rawValue = rawMap.get(rawKey);
            if (rawValue == null) {
                // Ignore NULL value.
                continue;
            }

            Double x;
            if (rawValue instanceof Double) {
                x = (Double)rawValue;
            } else if (rawValue instanceof Float) {
                x = ((Float)rawValue).doubleValue();
            } else if (rawValue instanceof DataByteArray) {
                x = Double.valueOf(rawValue.toString());
            } else {
                throw new ExecException("Invalid value type in pig map: " + rawValue.getClass().getName());
            }
            doubleMap.put((String) rawKey, x);
        }

        return doubleMap.isEmpty() ? null : doubleMap;
    }

    protected static Map<String, Double> sumEach(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Double> sum = new HashMap<String, Double>();
        for (Tuple t : values) {
            Map<String, Double> map = convert(t.get(0));
            if (map == null) {
                continue;
            }
            addEach(map, sum);
        }

        return sum.isEmpty() ? null : sum;
    }

    protected static Map<String, Double> combine(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);
        if (values.size() == 0) {
            return null;
        }

        Map<String, Double> sum = new HashMap<String, Double>();
        for (Tuple t : values) {
            @SuppressWarnings("unchecked")
            Map<String, Double> map = (Map<String, Double>) (t.get(0));
            if (map == null) {
                continue;
            }
            addEach(map, sum);
        }

        return sum.isEmpty() ? null : sum;
    }
}
