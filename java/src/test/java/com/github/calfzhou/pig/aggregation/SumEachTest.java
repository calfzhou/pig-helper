package com.github.calfzhou.pig.aggregation;

import org.apache.pig.data.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class SumEachTest {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> data() {
        List<Object[]> testData = new ArrayList<>();

        testData.add(new Object[]{
                "Empty bag",
                buildInputTuple(new HashMap[0]),
                null
        });

        testData.add(new Object[]{
                "Only one null map",
                buildInputTuple(new HashMap[]{null}),
                null
        });

        testData.add(new Object[]{
                "Only one empty map",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Double>()
                }),
                null
        });

        testData.add(new Object[]{
                "Only one map",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Double>(){{
                            put("a", 0.2D);
                            put("b", 1.5D);
                            put("c", 10D);
                            put("d", 0.01D);
                        }}
                }),
                new HashMap<String, Double>(){{
                    put("a", 0.2D);
                    put("b", 1.5D);
                    put("c", 10D);
                    put("d", 0.01D);
                }}
        });

        testData.add(new Object[]{
                "Several maps with distinct keys",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Double>(){{
                            put("a", 0.2D);
                            put("c", 10D);
                        }},
                        new HashMap<String, Double>(){{
                            put("b", 1.5D);
                            put("d", 0.01D);
                        }}
                }),
                new HashMap<String, Double>(){{
                    put("a", 0.2D);
                    put("b", 1.5D);
                    put("c", 10D);
                    put("d", 0.01D);
                }},
        });

        testData.add(new Object[]{
                "Several maps",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Double>(){{
                            put("a", 0.2D);
                            put("b", 1.5D);
                            put("c", 10D);
                            put("d", 0.01D);
                        }},
                        new HashMap<String, Double>(){{
                            put("b", 10D);
                            put("c", 2.4D);
                            put("d", 1.5D);
                            put("e", 0.9D);
                        }},
                        new HashMap<String, Double>(){{
                            put("a", 0.9D);
                            put("f", 1.3D);
                        }},
                        new HashMap<String, Double>(){{
                            put("a", 3.6D);
                            put("d", 0.2D);
                            put("f", 5D);
                        }},
                        new HashMap<String, Double>(){{
                            put(null, 5.8D);
                            put("c", null);
                            put("d", 0D);
                            put("g", 0D);
                        }},
                        new HashMap<String, Double>(){{
                            put(null, null);
                            put("a", 1D);
                        }},
                        null,
                        new HashMap<String, Double>(),
                        new HashMap<String, Double>(){{
                            put("h", 4.2D);
                            put("a", 5.0D);
                        }},
                }),
                new HashMap<String, Double>(){{
                    put("a", 10.7D);
                    put("b", 11.5D);
                    put("c", 12.4D);
                    put("d", 1.71D);
                    put("e", 0.9D);
                    put("f", 6.3D);
                    put("g", 0D);
                    put("h", 4.2D);
                }}
        });

        testData.add(new Object[]{
                "Float values",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Float>(){{
                            put("a", 2F);
                            put("c", 10F);
                        }},
                        new HashMap<String, Float>(){{
                            put("a", 5F);
                            put("d", 1F);
                        }},
                }),
                new HashMap<String, Double>(){{
                    put("a", 7D);
                    put("c", 10D);
                    put("d", 1D);
                }}
        });

        testData.add(new Object[]{
                "Byte array values",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, DataByteArray>(){{
                            put("a", new DataByteArray(Double.valueOf(2D).toString()));
                            put("c", new DataByteArray(Double.valueOf(10D).toString()));
                        }},
                        new HashMap<String, DataByteArray>(){{
                            put("a", new DataByteArray(Double.valueOf(5D).toString()));
                            put("d", new DataByteArray(Double.valueOf(1D).toString()));
                        }},
                }),
                new HashMap<String, Double>(){{
                    put("a", 7D);
                    put("c", 10D);
                    put("d", 1D);
                }}
        });

        return testData;
    }

    private static Tuple buildInputTuple(Map[] data) {
        DataBag dataBag = bagFactory.newDefaultBag();
        for (Map map : data) {
            dataBag.add(tupleFactory.newTuple(map));
        }
        return tupleFactory.newTuple(dataBag);
    }

    @Parameterized.Parameter(value = 0)
    public String caseName;

    @Parameterized.Parameter(value = 1)
    public Tuple inputTuple;

    @Parameterized.Parameter(value = 2)
    public Map<String, Double> expected;

    private static Random rand = new Random();
    private SumEach sumEach;

    @Before
    public void setUp() throws Exception {
        sumEach = new SumEach();
    }

    @Test
    public void testExec() throws Exception {
        Map<String, Double> actual = sumEach.exec(inputTuple);
        Assert.assertEquals(caseName, expected, actual);
    }

    @Test
    public void testAlgebraic() throws Exception {
        SumEach.Initial initial = new SumEach.Initial();
        SumEach.Intermediate intermediate = new SumEach.Intermediate();
        SumEach.Final aFinal = new SumEach.Final();

        List<Tuple> initialOutputs = new ArrayList<>();
        for (final Tuple t : (DataBag)inputTuple.get(0)) {
            Tuple input = tupleFactory.newTuple(bagFactory.newDefaultBag(
                    new ArrayList<Tuple>() {{ add(t); }}
            ));
            Tuple output = initial.exec(input);
            initialOutputs.add(output);
        }

        List<Tuple> intermediateOutputs = new ArrayList<>();
        List<Tuple> intermediateInputList = new ArrayList<>();
        for (int i = 0; i < initialOutputs.size(); i++) {
            intermediateInputList.add(initialOutputs.get(i));
            if (i == initialOutputs.size() - 1 || rand.nextInt(3) == 0) {
                Tuple input = tupleFactory.newTuple(bagFactory.newDefaultBag(intermediateInputList));
                Tuple output = intermediate.exec(input);
                intermediateOutputs.add(output);
                intermediateInputList.clear();
            }
        }

        Tuple finalInput = tupleFactory.newTuple(bagFactory.newDefaultBag(intermediateOutputs));
        Map<String, Double> actual = aFinal.exec(finalInput);
        Assert.assertEquals(caseName, expected, actual);
    }

    @Test
    public void testAccumulate() throws Exception {
        sumEach.cleanup();
        List<Tuple> accumulateInputList = new ArrayList<>();
        for (Tuple t : (DataBag)inputTuple.get(0)) {
            accumulateInputList.add(t);
            if (rand.nextInt(3) == 0) {
                Tuple input = tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList));
                sumEach.accumulate(input);
                accumulateInputList.clear();
            }
        }
        sumEach.accumulate(tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList)));
        Map<String, Double> actual = sumEach.getValue();
        Assert.assertEquals(caseName, expected, actual);
    }
}
