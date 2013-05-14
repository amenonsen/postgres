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
    DROP TABLE x;
    DROP FUNCTION wait_for(int);
}

session "s1"
connect_to "d1"
step "s1a" { BEGIN; }
step "s1b" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1c" { COMMIT; }
step "s1d" { INSERT INTO x(a,b) values (2,'baz'); }
step "s1e" { SELECT wait_for(3); }
step "s1f" { SELECT * from x order by a; }

session "s2"
connect_to "d2"
step "s2a" { BEGIN; }
step "s2b" { INSERT INTO x(a,b) values (1,'bar'); }
step "s2c" { COMMIT; }
step "s2d" { INSERT INTO x(a,b) values (3,'quux'); }
step "s2e" { SELECT wait_for(2); }
step "s2f" { SELECT * from x order by a; }

permutation "s1a" "s2a" "s1b" "s2b" "s1c" "s2c" "s1d" "s2d" "s1e" "s2e" "s1f" "s2f"
