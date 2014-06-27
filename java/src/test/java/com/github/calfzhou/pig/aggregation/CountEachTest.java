package com.github.calfzhou.pig.aggregation;

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
public class CountEachTest {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> testData = new ArrayList<>();

        testData.add(new Object[]{
                "Empty bag",
                buildInputTuple(new Object[0]),
                null
        });

        testData.add(new Object[]{
                "Item is null",
                buildInputTuple(new String[]{null}),
                null
        });

        testData.add(new Object[]{
                "Only one item",
                buildInputTuple(new String[]{"a"}),
                new HashMap<String, Long>(){{
                    put("a", 1L);
                }}
        });

        testData.add(new Object[]{
                "Several of the same item",
                buildInputTuple(new String[]{"a", "a", "a", "a"}),
                new HashMap<String, Long>(){{
                    put("a", 4L);
                }}
        });

        testData.add(new Object[]{
                "Several items",
                buildInputTuple(new String[]{"a", "b", null, "a", "c", null, "a", "d", "c"}),
                new HashMap<String, Long>(){{
                    put("a", 3L);
                    put("b", 1L);
                    put("c", 2L);
                    put("d", 1L);
                }}
        });

        testData.add(new Object[]{
                "Non string items",
                buildInputTuple(new Integer[]{1, 2, 6, 2, null, 4, 2, 3, 5, 3, 8, 1}),
                new HashMap<String, Long>(){{
                    put("1", 2L);
                    put("2", 3L);
                    put("3", 2L);
                    put("4", 1L);
                    put("5", 1L);
                    put("6", 1L);
                    put("8", 1L);
                }}
        });

        List<String> randomItems = new ArrayList<>();
        Map<String, Long> randomCount = new HashMap<>();
        String[] distinctItems = {"a", "b", "c", "d", "e", "f", "g", null, ""};
        for (String str : distinctItems) {
            int occurrence = rand.nextInt(10);
            for (int i = 0; i < occurrence; i++) {
                randomItems.add(str);
            }
            if (str != null && occurrence > 0) {
                randomCount.put(str, (long)occurrence);
            }
        }
        Collections.shuffle(randomItems);
        testData.add(new Object[]{
                "Lots of random items",
                buildInputTuple(randomItems.toArray()),
                randomCount.isEmpty() ? null : randomCount
        });

        return testData;
    }

    private static Tuple buildInputTuple(Object[] data) {
        DataBag dataBag = bagFactory.newDefaultBag();
        for (Object obj : data) {
            dataBag.add(tupleFactory.newTuple(obj));
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
    private CountEach countEach;

    @Before
    public void setUp() {
        countEach = new CountEach();
    }

    @Test
    public void testExec() throws Exception {
        Map<String, Long> actual = countEach.exec(inputTuple);
        Assert.assertEquals(caseName, expected, actual);
    }

    @Test
    public void testAlgebraic() throws Exception {
        CountEach.Initial initial = new CountEach.Initial();
        CountEach.Intermediate intermediate = new CountEach.Intermediate();
        CountEach.Final aFinal = new CountEach.Final();

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
        countEach.cleanup();
        List<Tuple> accumulateInputList = new ArrayList<>();
        for (Tuple t : (DataBag)inputTuple.get(0)) {
            accumulateInputList.add(t);
            if (rand.nextInt(3) == 0) {
                Tuple input = tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList));
                countEach.accumulate(input);
                accumulateInputList.clear();
            }
        }
        countEach.accumulate(tupleFactory.newTuple(bagFactory.newDefaultBag(accumulateInputList)));
        Map<String, Long> actual = countEach.getValue();
        Assert.assertEquals(caseName, expected, actual);
    }
}
