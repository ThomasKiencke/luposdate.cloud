-- TriplePattern: ?journal <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Journal> .
INTERMEDIATE_BAG_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '-caching 10','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Journal>') as (output0:chararray);

-- TriplePattern: ?journal <http://purl.org/dc/elements/1.1/title> "Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string> .
INTERMEDIATE_BAG_1 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '-caching 10','<http://purl.org/dc/elements/1.1/title>,"Journal 1 (1940)"^^<http://www.w3.org/2001/XMLSchema#string>') as (output1:chararray);

-- TriplePattern: ?journal <http://purl.org/dc/terms/issued> ?yr .
INTERMEDIATE_BAG_2 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '-caching 10','<http://purl.org/dc/terms/issued>') as (output1_2:chararray, output2_2:chararray); 

-- Join over ?journal
INTERMEDIATE_BAG_3 = JOIN INTERMEDIATE_BAG_0 BY $0, INTERMEDIATE_BAG_1 BY $0 USING 'merge' PARALLEL 5;
INTERMEDIATE_BAG_3 = Foreach INTERMEDIATE_BAG_3 GENERATE $0;
INTERMEDIATE_BAG_3 = ORDER INTERMEDIATE_BAG_3 BY $0 PARALLEL 5;

INTERMEDIATE_BAG_4 = JOIN INTERMEDIATE_BAG_3 BY $0, INTERMEDIATE_BAG_2 BY $0 USING 'merge' PARALLEL 5;
INTERMEDIATE_BAG_4 = Foreach INTERMEDIATE_BAG_4 GENERATE $1, $2;

-- Projection: ?yr
X = FOREACH INTERMEDIATE_BAG_4 GENERATE $1;
X = INTERMEDIATE_BAG_4;
--X = INTERMEDIATE_BAG_3;
