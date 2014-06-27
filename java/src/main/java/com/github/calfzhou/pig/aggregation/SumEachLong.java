package com.github.calfzhou.pig.aggregation;

import com.github.calfzhou.pig.Utils;
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
 * This class should never be used directly, use {@link com.github.calfzhou.pig.aggregation.SumEach}.
 */
public class SumEachLong extends EvalFunc<Map<String, Long>>
        implements Algebraic, Accumulator<Map<String, Long>> {

    // EvalFunc

    @Override
    public Map<String, Long> exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        Map<String, Long> sum = new HashMap<>();
        sumEachInto((DataBag)input.get(0), sum);
        return sum.isEmpty() ? null : sum;
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
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return tupleFactory.newTuple((Object)null);
            }
            Map<String, Long> sum = new HashMap<>();
            sumEachInto((DataBag)input.get(0), sum);
            return tupleFactory.newTuple(sum.isEmpty() ? null : sum);
        }
    }

    public static class Intermediate extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return tupleFactory.newTuple((Object)null);
            }
            Map<String, Long> sum = new HashMap<>();
            sumEachInto((DataBag)input.get(0), sum);
            return tupleFactory.newTuple(sum.isEmpty() ? null : sum);
        }
    }

    public static class Final extends EvalFunc<Map<String, Long>> {

        @Override
        public Map<String, Long> exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return null;
            }
            Map<String, Long> sum = new HashMap<>();
            sumEachInto((DataBag)input.get(0), sum);
            return sum.isEmpty() ? null : sum;
        }
    }

    // Accumulator

    private Map<String, Long> intermediateSum = null;

    public void accumulate(Tuple b) throws IOException {
        if (b == null || b.size() == 0 || b.get(0) == null) {
            return;
        }
        if (intermediateSum == null) {
            intermediateSum = new HashMap<>();
        }
        sumEachInto((DataBag)b.get(0), intermediateSum);
    }

    public Map<String, Long> getValue() {
        return intermediateSum;
    }

    public void cleanup() {
        intermediateSum = null;
    }

    // Other

    protected static Long convertValue(Object value) {
        if (value instanceof Long) {
            return (Long)value;
        } else if (value instanceof Integer) {
            return ((Integer)value).longValue();
        } else {
            return null;
        }
    }

    protected static void sumEachInto(DataBag dataBag, Map<String, Long> sum) throws ExecException {
        for (Tuple t : dataBag) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>)t.get(0);
            if (map == null) {
                continue;
            }

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Long value = convertValue(entry.getValue());
                if (value != null) {
                    sum.put(key, Utils.mapGet(sum, key, 0L) + value);
                }
            }
        }
    }
}
