package com.gocalf.pig.aggregation;

import com.gocalf.pig.Utils;
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
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        Map<String, Double> sum = new HashMap<>();
        sumEachInto((DataBag)input.get(0), sum);
        return sum.isEmpty() ? null : sum;
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
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return tupleFactory.newTuple((Object)null);
            }
            Map<String, Double> sum = new HashMap<>();
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
            Map<String, Double> sum = new HashMap<>();
            sumEachInto((DataBag)input.get(0), sum);
            return tupleFactory.newTuple(sum.isEmpty() ? null : sum);
        }
    }

    public static class Final extends EvalFunc<Map<String, Double>> {

        @Override
        public Map<String, Double> exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0 || input.get(0) == null) {
                return null;
            }
            Map<String, Double> sum = new HashMap<>();
            sumEachInto((DataBag)input.get(0), sum);
            return sum.isEmpty() ? null : sum;
        }
    }

    // Accumulator

    private Map<String, Double> intermediateSum = null;

    public void accumulate(Tuple b) throws IOException {
        if (b == null || b.size() == 0 || b.get(0) == null) {
            return;
        }
        System.err.println("SumEach.accumulate - " + ((DataBag)b.get(0)).size());
        if (intermediateSum == null) {
            intermediateSum = new HashMap<>();
        }
        sumEachInto((DataBag)b.get(0), intermediateSum);
    }

    public Map<String, Double> getValue() {
        return intermediateSum;
    }

    public void cleanup() {
        intermediateSum = null;
    }

    // Other

    protected static Double convertValue(Object value) {
        if (value instanceof Double) {
            return (Double)value;
        } else if (value instanceof Float) {
            return ((Float)value).doubleValue();
        } else if (value instanceof DataByteArray) {
            return Double.valueOf(value.toString());
        } else {
            return null;
        }
    }

    protected static void sumEachInto(DataBag dataBag, Map<String, Double> sum) throws ExecException {
        for (Tuple t : dataBag) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>)t.get(0);
            if (map == null) {
                continue;
            }

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Double value = convertValue(entry.getValue());
                if (value != null) {
                    sum.put(key, Utils.mapGet(sum, key, 0D) + value);
                }
            }
        }
    }
}
