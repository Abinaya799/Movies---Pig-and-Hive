movies_data = LOAD 'hdfs://localhost:9000/movies_en.json' USING JsonLoader('id:chararray,title:chararray,yearr:int,genre:chararray,summary:chararray,country:chararray,director: (id:chararray,last_name:chararray,first_name:chararray,year_of_birth:chararray ), actors: {(id:chararray,role:chararray)}');

artist_data = LOAD 'hdfs:///artists_en.json' USING JsonLoader('id:chararray,last_name:chararray, first_name:chararray, year_of_birth:chararray');

movies_data1 = FOREACH movies_data GENERATE $0,$1,$2,$3,$5,$6,$7;

filterUS = FILTER movies_data1 by country=='USA';

mUS_year = FOREACH (GROUP filterUS by yearr) GENERATE group as yearr,filterUS.title ;

mUS_director =  FOREACH (GROUP filterUS by director) GENERATE group as director,filterUS.title ;

mUS_actor =  FOREACH filterUS GENERATE id,FLATTEN (actors);

mUS_actor_full = FOREACH (JOIN mUS_actor by $1 , artist_data by $0) GENERATE mUS_actor::id as movie_id, mUS_actor::actors::id as actor_id,mUS_actor::actors::role as actor_role, artist_data::last_name as artist_last_name ,artist_data::first_name as artist_first_name,artist_data::year_of_birth as actor_yob;

mUS_full_group = FOREACH (COGROUP mUS_actor_full by movie_id) GENERATE group as movie_id, mUS_actor_full;

mUS_full_join = JOIN movies_data1 by id , mUS_actor_full by movie_id;

A = FILTER (FOREACH movies_data1 GENERATE id,FLATTEN(director),title,FLATTEN(actors)) by director::id == actors::id;

B = GROUP A by (director::id,director::last_name,director::first_name,director::year_of_birth,actors::id);

REGISTER myudfs.jar;

mUSFilterUDF = FILTER filterUS by myudfs.FilterByLastFirstName($4);
