-- ** Aggregate (COUNT, AVG, etc.) + Tumbling Time Window **
-- Performs function on the aggregate rows over a 10 second tumbling window for a specified column. 
--          .----------.   .----------.   .----------.              
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |              
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |              
--          '----------'   '----------'   '----------'               
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM
-- Create output stream, which can be used to send to a destination
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (who VARCHAR(100),  min_speed real,  max_speed real, dist_to_finish real, num_events INTEGER);
-- Create a pump which continuously selects from a source stream (SOURCE_SQL_STREAM_001)
-- performs an aggregate count that is grouped by columns ticker over a 10-second tumbling window
-- and inserts into output stream (DESTINATION_SQL_STREAM)
CREATE OR REPLACE  PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
-- Aggregate function COUNT|AVG|MAX|MIN|SUM|STDDEV_POP|STDDEV_SAMP|VAR_POP|VAR_SAMP)
SELECT STREAM who
, min(speed) as min_speed
, max(speed) as min_speed
, 0 as dist_to_finish
, COUNT(*) AS num_events
FROM "SOURCE_SQL_STREAM_001"
-- Uses a 10-second tumbling time window
GROUP BY who, FLOOR(("SOURCE_SQL_STREAM_001".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);