package com.github.calfzhou.pig.aggregation;

import org.apache.pig.data.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class SumEachLongTest {

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
                        new HashMap<String, Long>()
                }),
                null
        });

        testData.add(new Object[]{
                "Only one map",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Long>(){{
                            put("a", 1L);
                            put("b", 2L);
                            put("c", 10L);
                            put("d", 3L);
                        }}
                }),
                new HashMap<String, Long>(){{
                    put("a", 1L);
                    put("b", 2L);
                    put("c", 10L);
                    put("d", 3L);
                }}
        });

        testData.add(new Object[]{
                "Several maps with distinct keys",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Long>(){{
                            put("a", 1L);
                            put("c", 10L);
                        }},
                        new HashMap<String, Long>(){{
                            put("b", 2L);
                            put("d", 3L);
                        }}
                }),
                new HashMap<String, Long>(){{
                    put("a", 1L);
                    put("b", 2L);
                    put("c", 10L);
                    put("d", 3L);
                }},
        });

        testData.add(new Object[]{
                "Several maps",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Long>(){{
                            put("a", 2L);
                            put("b", 15L);
                            put("c", 100L);
                            put("d", 1L);
                        }},
                        new HashMap<String, Long>(){{
                            put("b", 100L);
                            put("c", 24L);
                            put("d", 15L);
                            put("e", 9L);
                        }},
                        new HashMap<String, Long>(){{
                            put("a", 9L);
                            put("f", 13L);
                        }},
                        new HashMap<String, Long>(){{
                            put("a", 36L);
                            put("d", 2L);
                            put("f", 50L);
                        }},
                        new HashMap<String, Long>(){{
                            put(null, 58L);
                            put("c", null);
                            put("d", 0L);
                            put("g", 0L);
                        }},
                        new HashMap<String, Long>(){{
                            put(null, null);
                            put("a", 10L);
                        }},
                        null,
                        new HashMap<String, Long>(),
                        new HashMap<String, Long>(){{
                            put("h", 42L);
                            put("a", 50L);
                        }},
                }),
                new HashMap<String, Long>(){{
                    put("a", 107L);
                    put("b", 115L);
                    put("c", 124L);
                    put("d", 18L);
                    put("e", 9L);
                    put("f", 63L);
                    put("g", 0L);
                    put("h", 42L);
                }}
        });

        testData.add(new Object[]{
                "Float values",
                buildInputTuple(new HashMap[]{
                        new HashMap<String, Integer>(){{
                            put("a", 2);
                            put("c", 10);
                        }},
                        new HashMap<String, Integer>(){{
                            put("a", 5);
                            put("d", 1);
                        }},
                }),
                new HashMap<String, Long>(){{
                    put("a", 7L);
                    put("c", 10L);
                    put("d", 1L);
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
    public Map<String, Long> expected;

    private static Random rand = new Random();
    private SumEachLong sumEachLong;

    @Before
    public void setUp() throws Exception {
        sumEachLong = new SumEachLong();
    }

    @Test
    public void testExec() throws Exception {
        Map<String, Long> actual = sumEachLong.exec(inputTuple);
        Assert.assertEquals(caseName, expected, actual);
    }

    @Test
    public void testAlgebraic() throws Exception {
        SumEachLong.Initial initial = new SumEachLong.Initial();
        SumEachLong.Intermediate intermediate = new SumEachLong.Intermediate();
        SumEachLong.Final aFinal = new SumEachLong.Final();

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
        Map<String, Long> actual = aFinal.exec(finalInput);
        Assert.assertEquals(caseName, expected, actual);
    }

    @Test
    public void testAccumulate() throws Exception {
        sumEachLong.cleanup();
        List<Tuple> accumulateInputList = new ArrayList<>();
        for (Tuple t : (DataBag)inputTuple.get(0)) {
            accumulateInputList.add(t);
            if (rand.nextInt(3) == 0) {
                Tuple input = tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList));
                sumEachLong.accumulate(input);
                accumulateInputList.clear();
            }
        }
        sumEachLong.accumulate(tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList)));
        Map<String, Long> actual = sumEachLong.getValue();
        Assert.assertEquals(caseName, expected, actual);
    }
}
