package com.github.calfzhou.pig.aggregation;

import com.github.calfzhou.pig.Utils;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
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
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        Map<String, Long> count = new HashMap<>();
        countEachInto((DataBag) input.get(0), count);
        return count.isEmpty() ? null : count;
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
            Map<String, Long> count = new HashMap<>();
            countEachInto((DataBag)input.get(0), count);
            return tupleFactory.newTuple(count.isEmpty() ? null : count);
        }
    }

    public static class Intermediate extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return tupleFactory.newTuple((Object)null);
            }
            Map<String, Long> count = new HashMap<>();
            sumEachInto((DataBag)input.get(0), count);
            return tupleFactory.newTuple(count.isEmpty() ? null : count);
        }
    }

    public static class Final extends EvalFunc<Map<String, Long>> {

        @Override
        public Map<String, Long> exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return null;
            }
            Map<String, Long> count = new HashMap<>();
            sumEachInto((DataBag)input.get(0), count);
            return count.isEmpty() ? null : count;
        }
    }

    // Accumulator

    private Map<String, Long> intermediateCount = null;

    public void accumulate(Tuple b) throws IOException {
        if (b == null || b.size() == 0 || b.get(0) == null) {
            return;
        }
        if (intermediateCount == null) {
            intermediateCount = new HashMap<>();
        }
        countEachInto((DataBag)b.get(0), intermediateCount);
    }

    public Map<String, Long> getValue() {
        return intermediateCount.isEmpty() ? null : intermediateCount;
    }

    public void cleanup() {
        intermediateCount = null;
    }

    // Other

    protected static void countEachInto(DataBag dataBag, Map<String, Long> count) throws ExecException {
        for (Tuple t : dataBag) {
            Object item = t.get(0);
            if (item == null) {
                continue;
            }

            String key = item.toString();
            count.put(key, Utils.mapGet(count, key, 0L) + 1L);
        }
    }

    protected static void sumEachInto(DataBag dataBag, Map<String, Long> sum) throws ExecException {
        for (Tuple t : dataBag) {
            @SuppressWarnings("unchecked")
            Map<String, Long> map = (Map<String, Long>)t.get(0);
            if (map == null) {
                continue;
            }

            for (Map.Entry<String, Long> entry : map.entrySet()) {
                String key = entry.getKey();
                Long value = entry.getValue();
                if (value != null) {
                    sum.put(key, Utils.mapGet(sum, key, 0L) + value);
                }
            }
        }
    }
}
