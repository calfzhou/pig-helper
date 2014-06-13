package com.gocalf.pig.aggregation;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class SumEach extends SumEachDouble {

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<>();
        funcList.add(new FuncSpec(this.getClass().getName(), getBagSchema(DataType.BYTEARRAY)));
        funcList.add(new FuncSpec(SumEachDouble.class.getName(), getBagSchema(DataType.DOUBLE)));
        funcList.add(new FuncSpec(SumEachDouble.class.getName(), getBagSchema(DataType.FLOAT)));
        funcList.add(new FuncSpec(SumEachLong.class.getName(), getBagSchema(DataType.LONG)));
        funcList.add(new FuncSpec(SumEachLong.class.getName(), getBagSchema(DataType.INTEGER)));
        return funcList;
    }

    private static Schema getBagSchema(byte t) throws FrontendException {
        Schema valueSchema = new Schema(new Schema.FieldSchema(null, t));
        Schema mapSchema = new Schema(new Schema.FieldSchema(null, valueSchema, DataType.MAP));
        return new Schema(new Schema.FieldSchema(null, mapSchema, DataType.BAG));
    }
}
