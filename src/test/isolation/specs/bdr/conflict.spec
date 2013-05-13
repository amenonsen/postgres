conninfo "d1" "port=5433 dbname=postgres"
conninfo "d2" "port=5434 dbname=postgres"

session "s1"
connection "d1"
setup { CREATE TABLE x(a int primary key, b text); CREATE FUNCTION wait_for(id int) RETURNS void AS $$ DECLARE tmp int; BEGIN LOOP SELECT a INTO tmp FROM x WHERE a=$1; IF found THEN RETURN; END IF; EXECUTE pg_sleep(1); END LOOP; END; $$ LANGUAGE 'plpgsql'; }
step "s1a" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1b" { BEGIN; }
step "s1c" { UPDATE x SET b='baz' WHERE a=1; }
step "s1d" { COMMIT; }
step "s1e" { SELECT * from x order by a; }
teardown { DROP TABLE x; DROP FUNCTION wait_for(int); }

session "s2"
connection "d2"
setup { CREATE TABLE x(a int primary key, b text); CREATE FUNCTION wait_for(id int) RETURNS void AS $$ DECLARE tmp int; BEGIN LOOP SELECT a INTO tmp FROM x WHERE a=$1; IF found THEN RETURN; END IF; EXECUTE pg_sleep(1); END LOOP; END; $$ LANGUAGE 'plpgsql'; }
step "s2a" { SELECT wait_for(1); }
step "s2b" { BEGIN; }
step "s2c" { UPDATE x SET b='quux' WHERE a=1; }
step "s2d" { COMMIT; }
step "s2e" { SELECT * from x order by a; }
teardown { DROP TABLE x; DROP FUNCTION wait_for(int); }

permutation "s1a" "s2a" "s1b" "s2b" "s1c" "s2c" "s1d" "s2d" "s1e" "s2e"
