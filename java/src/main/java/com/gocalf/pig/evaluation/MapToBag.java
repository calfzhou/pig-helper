package com.gocalf.pig.evaluation;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapToBag extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>)input.get(0);
        if (map == null || map.isEmpty()) {
            return null;
        }

        DataBag bag = bagFactory.newDefaultBag();
        for (String key : map.keySet()) {
            Object value = map.get(key);
            Tuple oneKeyTuple = tupleFactory.newTuple(2);
            oneKeyTuple.set(0, key);
            oneKeyTuple.set(1, value);
            bag.add(oneKeyTuple);
        }

        return (bag.size() == 0) ? null : bag;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema innerSchema = new Schema();
            innerSchema.add(new Schema.FieldSchema("key", DataType.CHARARRAY));
            innerSchema.add(getMapValueSchema(input));
            Schema tupleSchema = new Schema(new Schema.FieldSchema(null, innerSchema, DataType.TUPLE));
            return new Schema(new Schema.FieldSchema(null, tupleSchema, DataType.BAG));
        } catch (FrontendException e) {
            return new Schema(new Schema.FieldSchema(null, DataType.BAG));
        }
    }

    protected Schema.FieldSchema getMapValueSchema(Schema input) throws FrontendException {
        if (input == null || input.size() == 0) {
            return null;
        }
        Schema.FieldSchema mapField = input.getField(0);
        if (mapField.type != DataType.MAP) {
            return null;
        }

        Schema valueSchema = mapField.schema;
        if (valueSchema == null || valueSchema.size() == 0) {
            return null;
        }
        Schema.FieldSchema valueField = valueSchema.getField(0);
        valueField.alias = "value";
        return valueField;
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.MAP))));
        return funcList;
    }

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
}

