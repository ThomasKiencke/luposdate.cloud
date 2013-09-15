-- TRIPLE PATTERN #1
-- TriplePattern: ?journal <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Journal> .
PATTERN_0a = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>0') as (columncontent_0:map[]);
INTERMEDIATE_BAG_0a = foreach PATTERN_0a generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output0:chararray);

-- TriplePattern: ?journal <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Journal> .
PATTERN_0b = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>1') as (columncontent_0:map[]);
INTERMEDIATE_BAG_0b = foreach PATTERN_0b generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output0:chararray);

-- TriplePattern: ?journal <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Journal> .
PATTERN_0c = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>2') as (columncontent_0:map[]);
INTERMEDIATE_BAG_0c = foreach PATTERN_0c generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output0:chararray);

-- TRIPLE PATTERN #2
-- TriplePattern: ?journal <http://purl.org/dc/elements/1.1/title> "Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string> .
PATTERN_1a = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/elements/1.1/title>,"Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string>0') as (columncontent_1:map[]);
INTERMEDIATE_BAG_1a = foreach PATTERN_1a generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray);

-- TriplePattern: ?journal <http://purl.org/dc/elements/1.1/title> "Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string> .
PATTERN_1b = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/elements/1.1/title>,"Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string>1') as (columncontent_1:map[]);
INTERMEDIATE_BAG_1b = foreach PATTERN_1b generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray);

-- TriplePattern: ?journal <http://purl.org/dc/elements/1.1/title> "Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string> .
PATTERN_1c = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/elements/1.1/title>,"Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string>2') as (columncontent_1:map[]);
INTERMEDIATE_BAG_1c = foreach PATTERN_1c generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1:chararray);

-- TRIPLE PATTERN #3
-- TriplePattern: ?journal <http://purl.org/dc/terms/issued> ?yr .
PATTERN_2a = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>00') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2a = foreach PATTERN_2a generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2b = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>01') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2b = foreach PATTERN_2b generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2c = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>02') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2c = foreach PATTERN_2c generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray);

-- TriplePattern: ?journal <http://purl.org/dc/terms/issued> ?yr .
PATTERN_2d = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>10') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2d = foreach PATTERN_2d generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2e = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>11') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2e = foreach PATTERN_2e generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2f = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>12') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2f = foreach PATTERN_2f generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

-- TriplePattern: ?journal <http://purl.org/dc/terms/issued> ?yr .
PATTERN_2g = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>20') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2g = foreach PATTERN_2g generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2h = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>21') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2h = foreach PATTERN_2h generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

PATTERN_2i = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadUDF('HexaSub', '-caching 10','<http://purl.org/dc/terms/issued>22') as (columncontent_2:map[]);
INTERMEDIATE_BAG_2i = foreach PATTERN_2i generate flatten(lupos.cloud.pig.udfs.MapToBagUDF($0)) as (output1_2:chararray, output2_2:chararray); 

-- Join over ?journal
INTERMEDIATE_BAG_3a = JOIN INTERMEDIATE_BAG_0a BY $0, INTERMEDIATE_BAG_1a BY $0;
INTERMEDIATE_BAG_3b = JOIN INTERMEDIATE_BAG_0b BY $0, INTERMEDIATE_BAG_1b BY $0;
INTERMEDIATE_BAG_3c = JOIN INTERMEDIATE_BAG_0c BY $0, INTERMEDIATE_BAG_1c BY $0;

INTERMEDIATE_BAG_4a = JOIN INTERMEDIATE_BAG_3a BY $0, INTERMEDIATE_BAG_2a BY $0;
INTERMEDIATE_BAG_4b = JOIN INTERMEDIATE_BAG_3a BY $0, INTERMEDIATE_BAG_2b BY $0;
INTERMEDIATE_BAG_4c = JOIN INTERMEDIATE_BAG_3a BY $0, INTERMEDIATE_BAG_2c BY $0;

INTERMEDIATE_BAG_4d = JOIN INTERMEDIATE_BAG_3b BY $0, INTERMEDIATE_BAG_2d BY $0;
INTERMEDIATE_BAG_4e = JOIN INTERMEDIATE_BAG_3b BY $0, INTERMEDIATE_BAG_2e BY $0;
INTERMEDIATE_BAG_4f = JOIN INTERMEDIATE_BAG_3b BY $0, INTERMEDIATE_BAG_2f BY $0;

INTERMEDIATE_BAG_4g = JOIN INTERMEDIATE_BAG_3c BY $0, INTERMEDIATE_BAG_2g BY $0;
INTERMEDIATE_BAG_4h = JOIN INTERMEDIATE_BAG_3c BY $0, INTERMEDIATE_BAG_2h BY $0;
INTERMEDIATE_BAG_4i = JOIN INTERMEDIATE_BAG_3c BY $0, INTERMEDIATE_BAG_2i BY $0;


-- Projection: ?yr
X1 = FOREACH INTERMEDIATE_BAG_4a GENERATE $3;
X2 = FOREACH INTERMEDIATE_BAG_4b GENERATE $3;
X3 = FOREACH INTERMEDIATE_BAG_4c GENERATE $3;
X4 = FOREACH INTERMEDIATE_BAG_4d GENERATE $3;
X5 = FOREACH INTERMEDIATE_BAG_4e GENERATE $3;
X6 = FOREACH INTERMEDIATE_BAG_4f GENERATE $3;
X7 = FOREACH INTERMEDIATE_BAG_4g GENERATE $3;
X8 = FOREACH INTERMEDIATE_BAG_4h GENERATE $3;
X9 = FOREACH INTERMEDIATE_BAG_4i GENERATE $3;


X = UNION X1, X2, X3, X4, X5, X6, X7, X8, X9;
