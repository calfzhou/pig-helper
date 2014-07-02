package com.github.calfzhou.pig.integration;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.test.Util;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PigScriptIT {

    private static final String TARGET_ROOT = "target";
    private static final String HELPER_JAR_PATH = Paths.get(TARGET_ROOT, "pig-helper-*.jar").toString();

    private static String SCRIPT_ROOT;
    private static String DATA_ROOT;

    @BeforeClass
    public static void setUpAll() throws Exception {
        SCRIPT_ROOT = Paths.get(PigScriptIT.class.getResource("/scripts").toURI()).toString();
        DATA_ROOT = Paths.get(PigScriptIT.class.getResource("/data").toURI()).toString();
    }

    @Test
    public void testCountEach() throws Exception {
        String scriptPath = Paths.get(SCRIPT_ROOT, "count_each_example.pig").toString();
        String[] args = {
                "input_path=" + Paths.get(DATA_ROOT, "sample_data").toString(),
                "output_path=output",
                "pig_helper_path=" + HELPER_JAR_PATH,
        };
        PigTest test = new PigTest(scriptPath, args);

        String expectedPath = Paths.get(DATA_ROOT, "count_each_result").toString();
        String schema = "name:chararray, count_map:[long]";
        Util.checkQueryOutputsAfterSortRecursive(
                test.getAlias("each_key_counts_no_null"), readTupleLines(expectedPath), schema
        );
    }

    @Test
    public void testSumEachInt() throws Exception {
        testSumEach("int", "long");
    }

    @Test
    public void testSumEachLong() throws Exception {
        testSumEach("long", "long");
    }

    @Test
    public void testSumEachFloat() throws Exception {
        testSumEach("float", "double");
    }

    @Test
    public void testSumEachDouble() throws Exception {
        testSumEach("double", "double");
    }

    @Test
    public void testSumEachByteArray() throws Exception {
        testSumEach("bytearray", "double");
    }

    private void testSumEach(String inputValueType, String resultValueType) throws Exception {
        String scriptPath = Paths.get(SCRIPT_ROOT, "sum_each_example.pig").toString();
        String[] args = {
                "input_path=" + Paths.get(DATA_ROOT, "sample_data").toString(),
                "output_path=output",
                "pig_helper_path=" + HELPER_JAR_PATH,
                "value_type=" + inputValueType,
        };
        PigTest test = new PigTest(scriptPath, args);

        String expectedPath = Paths.get(DATA_ROOT, "sum_each_result").toString();
        String schema = String.format("name:chararray, total_map:[%s]", resultValueType);
        Util.checkQueryOutputsAfterSortRecursive(
                test.getAlias("each_key_totals_no_null"), readTupleLines(expectedPath), schema
        );
    }

    @Test
    public void testMapToBag() throws Exception {
        String scriptPath = Paths.get(SCRIPT_ROOT, "map_to_bag_example.pig").toString();
        String[] args = {
                "input_path=" + Paths.get(DATA_ROOT, "students_score_data").toString(),
                "output_path=output",
                "pig_helper_path=" + HELPER_JAR_PATH,
        };
        PigTest test = new PigTest(scriptPath, args);

        String expectedPath = Paths.get(DATA_ROOT, "students_score_result").toString();
        String schema = "name:chararray, course:chararray, score:int";
        Util.checkQueryOutputsAfterSortRecursive(
                test.getAlias("score_table"), readTupleLines(expectedPath), schema
        );
    }

    private static String[] readTupleLines(String fileName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                line = String.format("(%s)", line.replace("\t", ","));
                lines.add(line);
            }
            return lines.toArray(new String[lines.size()]);
        }
    }
}
