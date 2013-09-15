-- TriplePattern: ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Article> .
INTERMEDIATE_BAG_0 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Article>') as(output0:chararray);

-- TriplePattern: ?article <http://purl.org/dc/elements/1.1/creator> ?person .
INTERMEDIATE_BAG_1 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://purl.org/dc/elements/1.1/creator>') as(output1_1:chararray, output2_1:chararray); 

-- TriplePattern: ?inproc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://localhost/vocabulary/bench/Inproceedings> .
INTERMEDIATE_BAG_2 = load 'hbase://PO_S' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://localhost/vocabulary/bench/Inproceedings>') as(output2:chararray);

-- TriplePattern: ?inproc <http://purl.org/dc/elements/1.1/creator> ?person2 .
INTERMEDIATE_BAG_3 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://purl.org/dc/elements/1.1/creator>') as(output1_3:chararray, output2_3:chararray); 

-- TriplePattern: ?person <http://xmlns.com/foaf/0.1/name> ?name2 .
INTERMEDIATE_BAG_4 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://xmlns.com/foaf/0.1/name>') as(output1_4:chararray, output2_4:chararray); 

-- TriplePattern: ?person2 <http://xmlns.com/foaf/0.1/name> ?name2 .
INTERMEDIATE_BAG_5 = load 'hbase://P_SO' using lupos.cloud.pig.udfs.HBaseLoadBagUDF('Hexa', '','<http://xmlns.com/foaf/0.1/name>') as(output1_5:chararray, output2_5:chararray); 

-- Join over ?article
INTERMEDIATE_BAG_6 = JOIN INTERMEDIATE_BAG_0 BY $0, INTERMEDIATE_BAG_1 BY $0 USING 'merge' PARALLEL 3;

-- Join over ?inproc
INTERMEDIATE_BAG_7 = JOIN INTERMEDIATE_BAG_2 BY $0, INTERMEDIATE_BAG_3 BY $0 USING 'merge' PARALLEL 3;

-- Join over ?name2
INTERMEDIATE_BAG_8 = JOIN INTERMEDIATE_BAG_4 BY $1, INTERMEDIATE_BAG_5 BY $1 USING 'merge' PARALLEL 3;

-- Projection: ?person2, ?person, ?name2
INTERMEDIATE_BAG_9 = FOREACH INTERMEDIATE_BAG_8 GENERATE $2, $0, $1;

-- Join over ?person
INTERMEDIATE_BAG_10 = JOIN INTERMEDIATE_BAG_6 BY $2, INTERMEDIATE_BAG_9 BY $1 PARALLEL 3;

-- Projection: ?person2, ?person, ?name2
INTERMEDIATE_BAG_11 = FOREACH INTERMEDIATE_BAG_10 GENERATE $3, $2, $5;

-- Join over ?person2
INTERMEDIATE_BAG_12 = JOIN INTERMEDIATE_BAG_7 BY $2, INTERMEDIATE_BAG_11 BY $0 PARALLEL 3;

-- Projection: ?person, ?name2
INTERMEDIATE_BAG_13 = FOREACH INTERMEDIATE_BAG_12 GENERATE $4, $5;

-- Distinct: 
X = DISTINCT INTERMEDIATE_BAG_13 PARALLEL 3;
