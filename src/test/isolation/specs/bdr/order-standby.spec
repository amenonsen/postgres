# Demonstrates that changes to the upstream master, d1, become visible on the
# streaming replica 'r1' that follows 'd1' and the streaming replica 'r2' that
# follows downstream master 'd2' in the order that they're committed on the upstream
# master.
#
# This test requires that log-streaming logical replication be configured on
# the "postgres" database with d1 on port 5433 as upstream master to d2 on port
# 5434.
#
# Each master must have a streaming replica, r1 and r2 respectively, on ports
# 5435 and 5436.

server "d1"
server "d2"
server "r1" readonly
server "r2" readonly

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

# Three sessions that each commit in order to the upstream master
session "s1"
connect_to "d1"
step "s1a" { BEGIN; }
step "s1b" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1c" { COMMIT; }

session "s2"
connect_to "d1"
step "s2a" { BEGIN; }
step "s2b" { INSERT INTO x(a,b) values (2,'bar'); }
step "s2c" { COMMIT; }

session "s3"
connect_to "d1"
step "s3a" { BEGIN; }
step "s3b" { INSERT INTO x(a,b) values (3,'baz'); }
step "s3c" { COMMIT; }

# Runs on r1 after each commit to show it is visible and no unexpected rows are
# visible yet.
session "sr1"
connect_to "d2"
step "sr1a" { SELECT wait_for(1); }
step "sr1b" { SELECT wait_for(2); }
step "sr1c" { SELECT wait_for(3); }
step "sr1d" { SELECT * from x order by a; }

# Same as sr1 but for r2
session "sr1"
connect_to "d2"
step "sr2a" { SELECT wait_for(1); }
step "sr2b" { SELECT wait_for(2); }
step "sr2c" { SELECT wait_for(3); }
step "sr2d" { SELECT * from x order by a; }


permutation "s1a" "s2a" "s3a" "s1b" "s2b" "s3b" "sr1d" "sr2d" "s1c" "sr1a" "sr2a" "sr1d" "sr2d" "s2c" "sr1b" "sr2b" "sr1d" "sr2d" "s3c" "sr1c" "sr2c" "sr1d" "sr2d"
