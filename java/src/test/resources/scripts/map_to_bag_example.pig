-- Params
--   input_path
--   output_path
--   pig_helper_path

REGISTER $pig_helper_path;

raw_scores =
    LOAD '$input_path'
    AS (
        name:chararray, chinese_score:int, math_score:int, english_score:int, computer_score:int
    );

score_maps =
    FOREACH raw_scores
    GENERATE
        name,
        TOMAP('Chinese', chinese_score, 'Math', math_score, 'English', english_score,
            'Computer', computer_score) AS score_map:[int];

score_bags =
    FOREACH score_maps
    GENERATE
        name,
        com.github.calfzhou.pig.evaluation.MapToBag(score_map) AS score_bag:{(course:chararray, score:int)};

score_table =
    FOREACH score_bags
    GENERATE
        name,
        FLATTEN(score_bag) AS (course:chararray, score:int);

STORE score_table INTO '$output_path';
