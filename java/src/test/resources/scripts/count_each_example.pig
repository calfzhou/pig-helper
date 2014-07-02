-- Params
--   input_path
--   output_path
--   pig_helper_path

REGISTER $pig_helper_path;

raw_data =
    LOAD '$input_path'
    AS (
        name:chararray, key:chararray, value:long
    );

data_groups =
    GROUP raw_data BY name;

each_key_counts =
    FOREACH data_groups
    GENERATE
        group AS name,
        com.github.calfzhou.pig.aggregation.CountEach(raw_data.key) AS count_map;

each_key_counts_no_null =
    FILTER each_key_counts
    BY count_map IS NOT NULL;

STORE each_key_counts_no_null INTO '$output_path';
