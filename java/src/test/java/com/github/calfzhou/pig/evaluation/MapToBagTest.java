package com.github.calfzhou.pig.evaluation;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class MapToBagTest {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> testData = new ArrayList<>();

        testData.add(new Object[]{
                "Null map",
                buildInputTuple(null),
                null
        });

        testData.add(new Object[]{
                "Empty map",
                buildInputTuple(new HashMap<String, Object>()),
                null
        });

        testData.add(new Object[]{
                "Only one null key",
                buildInputTuple(new HashMap<String, Object>(){{
                    put(null, 29);
                }}),
                buildOutputBag(new HashMap<String, Object>(){{
                    put(null, 29);
                }})
        });

        testData.add(new Object[]{
                "Only one item",
                buildInputTuple(new HashMap<String, Object>(){{
                    put("a", 29);
                }}),
                buildOutputBag(new HashMap<String, Object>(){{
                    put("a", 29);
                }})
        });

        testData.add(new Object[]{
                "One null value item",
                buildInputTuple(new HashMap<String, Object>(){{
                    put("a", null);
                }}),
                buildOutputBag(new HashMap<String, Object>(){{
                    put("a", null);
                }})
        });

        testData.add(new Object[]{
                "Several items",
                buildInputTuple(new HashMap<String, Object>(){{
                    put("a", 29);
                    put(null, null);
                    put("c", null);
                    put("b", 8);
                    put("e", 9);
                    put("d", 200);
                }}),
                buildOutputBag(new HashMap<String, Object>(){{
                    put("a", 29);
                    put("b", 8);
                    put("c", null);
                    put("d", 200);
                    put("e", 9);
                    put(null, null);
                }})
        });

        return testData;
    }

    private static Tuple buildInputTuple(Map<String, Object> data) {
        return tupleFactory.newTuple(data);
    }

    private static DataBag buildOutputBag(Map<String, Object> data) {
        DataBag dataBag = bagFactory.newDefaultBag();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Tuple oneTuple = tupleFactory.newTuple();
            oneTuple.append(entry.getKey());
            oneTuple.append(entry.getValue());
            dataBag.add(oneTuple);
        }
        return dataBag;
    }

    @Parameterized.Parameter(value = 0)
    public String caseName;

    @Parameterized.Parameter(value = 1)
    public Tuple inputTuple;

    @Parameterized.Parameter(value = 2)
    public DataBag expected;

    private MapToBag mapToBag;

    @Before
    public void setUp() {
        mapToBag = new MapToBag();
    }

    @Test
    public void testExec() throws Exception {
        DataBag actual = mapToBag.exec(inputTuple);
        Assert.assertEquals(caseName, expected, actual);
    }
}
