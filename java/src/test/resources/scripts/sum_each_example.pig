-- Params
--   input_path
--   output_path
--   pig_helper_path
%default value_type 'double'

REGISTER $pig_helper_path;

raw_data =
    LOAD '$input_path'
    AS (
        name:chararray, key:chararray, value:$value_type
    );

data_maps =
    FOREACH raw_data
    GENERATE name, TOMAP(key, value) AS key_value_map:[$value_type];

data_groups =
    GROUP data_maps BY name;

each_key_totals =
    FOREACH data_groups
    GENERATE
        group AS name,
        com.github.calfzhou.pig.aggregation.SumEach(data_maps.key_value_map) AS total_map;

each_key_totals_no_null =
    FILTER each_key_totals
    BY total_map IS NOT NULL;

STORE each_key_totals_no_null INTO '$output_path';
