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
step "s1a" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1b" { BEGIN; }
step "s1c" { DELETE from x WHERE a=1; }
step "s1d" { COMMIT; }
step "s1e" { INSERT INTO x(a,b) values (2,'bar'); }
step "s1f" { SELECT wait_for(3); }
step "s1g" { SELECT * from x order by a; }

session "s2"
connect_to "d2"
step "s2a" { SELECT wait_for(1); }
step "s2b" { BEGIN; }
step "s2c" { UPDATE x SET b='quux' WHERE a=1; }
step "s2d" { COMMIT; }
step "s2e" { INSERT INTO x(a,b) values (3,'baz'); }
step "s2f" { SELECT wait_for(2); }
step "s2g" { SELECT * from x order by a; }

permutation "s1a" "s2a" "s1b" "s2b" "s1c" "s2c" "s1d" "s2d" "s1e" "s2e" "s1f" "s2f" "s1g" "s2g"
