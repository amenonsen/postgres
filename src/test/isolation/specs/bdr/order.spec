# Demonstrates that changes to the upstream master, d1, become visible
# to the downstream master d2 in the order that they're committed.
#
# This test requires that log-streaming logical replication be
# configured on the "postgres" database with d1 on port 5433 as
# upstream master to d2 on port 5434.

server "d1"
server "d2"

setup { SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
setup { DROP TABLE IF EXISTS x; }
setup { CREATE TABLE x(a int primary key, b text); }
setup {
    CREATE OR REPLACE FUNCTION wait_for(id int) RETURNS int AS $$
        DECLARE tmp int;
        BEGIN LOOP
            SELECT a INTO tmp FROM x WHERE a=$1;
            IF found THEN RETURN tmp; END IF;
            EXECUTE pg_sleep(0.1);
        END LOOP; END;
    $$ LANGUAGE 'plpgsql' VOLATILE;
}
teardown {
    DROP table x;
    DROP FUNCTION wait_for(int);
}

session "s1"
connect_to "d1"
step "s1a" { BEGIN; }
step "s1b" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1c" { COMMIT; }

session "s2"
connect_to "d2"
step "s2a" { BEGIN; }
step "s2b" { INSERT INTO x(a,b) values (2,'bar'); }
step "s2c" { COMMIT; }

session "s3"
connect_to "d1"
step "s3a" { BEGIN; }
step "s3b" { INSERT INTO x(a,b) values (3,'baz'); }
step "s3c" { COMMIT; }

session "s4"
connect_to "d2"
step "s4a" { SELECT wait_for(1); }
step "s4b" { SELECT wait_for(2); }
step "s4c" { SELECT wait_for(3); }
step "s4d" { SELECT * from x order by a; }

permutation "s1a" "s2a" "s3a" "s1b" "s2b" "s3b" "s4d" "s1c" "s4a" "s4d" "s2c" "s4b" "s4d" "s3c" "s4c" "s4d"
