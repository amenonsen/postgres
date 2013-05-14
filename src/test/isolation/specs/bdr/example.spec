server "d1"
server "d2"

setup { SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
setup { DROP TABLE IF EXISTS x; }
setup { CREATE TABLE x(a int, b text); }
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
    DROP TABLE x;
    DROP FUNCTION wait_for(int);
}

session "s1"
connect_to "d1"
step "s1a" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1b" { SELECT wait_for(2); }
step "s1c" { SELECT * from x order by a; }

session "s2"
connect_to "d2"
step "s2a" { INSERT INTO x(a,b) values (2,'bar'); }
step "s2b" { SELECT wait_for(1); }
step "s2c" { SELECT * from x order by a; }

permutation "s1a" "s2a" "s1b" "s2b" "s1c" "s2c"
