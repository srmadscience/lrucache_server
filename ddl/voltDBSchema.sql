

CREATE TABLE subscriber  ( 
  s_id    BIGINT NOT NULL PRIMARY KEY
, sub_nbr VARCHAR(80) NOT NULL
, f_tinyint tinyint
, f_smallint smallint
, f_integer integer
, f_bigint bigint
, f_float float
, f_decimal decimal
, f_geography geography
, f_geography_point GEOGRAPHY_POINT
, f_varchar varchar(80)
, f_varbinary varbinary(1024)
, flush_date TIMESTAMP 
, last_use_date TIMESTAMP NOT NULL) ;


CREATE INDEX s_idx_lu ON subscriber (last_use_date);

CREATE INDEX s_idx_fd ON subscriber (flush_date);

CREATE INDEX s_idx_fint ON subscriber (f_integer, s_id);

PARTITION TABLE subscriber ON COLUMN s_id;

create view sub_generation as 
select f_integer generation, min(s_id) min_id, max(s_id) max_id, count(*) how_many
from subscriber 
group by f_integer;

create view sub_flush_range as 
select min(flush_date) min_flush_date,max(flush_date) max_flush_date, count(*) how_many
from subscriber ;


CREATE STREAM subscriber_export 
EXPORT TO TARGET sub_archive ( 
  s_id    BIGINT NOT NULL 
, sub_nbr VARCHAR(80) NOT NULL
, f_tinyint tinyint
, f_smallint smallint
, f_integer integer
, f_bigint bigint
, f_float float
, f_decimal decimal
, f_geography geography
, f_geography_point GEOGRAPHY_POINT
, f_varchar varchar(80)
, f_varbinary varbinary(1024)
, flush_date TIMESTAMP  NOT NULL
, last_use_date TIMESTAMP NOT NULL) ;




load classes /Users/drolfe/Desktop/EclipseWorkspace/lrucache_server.jar;


CREATE PROCEDURE 
   PARTITION ON TABLE subscriber COLUMN s_id
   FROM CLASS lrucache.server.GetById;
   
CREATE PROCEDURE 
   PARTITION ON TABLE subscriber COLUMN s_id
   FROM CLASS lrucache.server.GetByIdCheckGeneration;
   
CREATE PROCEDURE 
   PARTITION ON TABLE subscriber COLUMN s_id
   FROM CLASS lrucache.server.UpdateGeneration;
   
   CREATE PROCEDURE 
   PARTITION ON TABLE subscriber COLUMN s_id
   FROM CLASS lrucache.server.Purge;
   
CREATE PROCEDURE 
   FROM CLASS lrucache.server.CountId;
   
CREATE PROCEDURE 
   FROM CLASS lrucache.server.TruncateSubscriber;
   
