CREATE EXTENSION test_logical_decoding;
-- predictability
SET synchronous_commit = on;

-- faster startup
CHECKPOINT;

SELECT 'init' FROM init_logical_replication('test_decoding');

CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));
BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (1, 1);
INSERT INTO replication_example(somedata, text) VALUES (1, 2);
COMMIT;

ALTER TABLE replication_example ADD COLUMN bar int;

INSERT INTO replication_example(somedata, text, bar) VALUES (2, 1, 4);

BEGIN;
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 2, 4);
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 3, 4);
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 4, NULL);
COMMIT;

ALTER TABLE replication_example DROP COLUMN bar;
INSERT INTO replication_example(somedata, text) VALUES (3, 1);

BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (3, 2);
INSERT INTO replication_example(somedata, text) VALUES (3, 3);
COMMIT;

ALTER TABLE replication_example RENAME COLUMN text TO somenum;

INSERT INTO replication_example(somedata, somenum) VALUES (4, 1);

-- collect all changes
SELECT data FROM start_logical_replication('now');

ALTER TABLE replication_example ALTER COLUMN somenum TYPE int4 USING (somenum::int4);
-- throw away changes, they contain oids
SELECT count(data) FROM start_logical_replication('now');

INSERT INTO replication_example(somedata, somenum) VALUES (5, 1);

BEGIN;
INSERT INTO replication_example(somedata, somenum) VALUES (6, 1);
ALTER TABLE replication_example ADD COLUMN zaphod1 int;
INSERT INTO replication_example(somedata, somenum, zaphod1) VALUES (6, 2, 1);
ALTER TABLE replication_example ADD COLUMN zaphod2 int;
INSERT INTO replication_example(somedata, somenum, zaphod2) VALUES (6, 3, 1);
INSERT INTO replication_example(somedata, somenum, zaphod1) VALUES (6, 4, 2);
COMMIT;

/*
 * check whether the correct indexes are chosen for deletions
 */

CREATE TABLE tr_unique(id2 serial unique NOT NULL, data int);
INSERT INTO tr_unique(data) VALUES(10);
--show deletion with unique index
DELETE FROM tr_unique;

ALTER TABLE tr_unique RENAME TO tr_pkey;

-- show changes
SELECT data FROM start_logical_replication('now');

-- hide changes bc of oid visible in full table rewrites
ALTER TABLE tr_pkey ADD COLUMN id serial primary key;
SELECT count(data) FROM start_logical_replication('now');

INSERT INTO tr_pkey(data) VALUES(1);
--show deletion with primary key
DELETE FROM tr_pkey;

/* display results */
SELECT data FROM start_logical_replication('now');

/*
 * check that disk spooling works
 */
BEGIN;
CREATE TABLE tr_etoomuch (id serial primary key, data int);
INSERT INTO tr_etoomuch(data) SELECT g.i FROM generate_series(1, 10234) g(i);
DELETE FROM tr_etoomuch WHERE id < 5000;
UPDATE tr_etoomuch SET data = - data WHERE id > 5000;
COMMIT;

/* display results, but hide most of the output */
SELECT count(*), min(data), max(data)
FROM start_logical_replication('now')
GROUP BY substring(data, 1, 24)
ORDER BY 1;

/*
 * check whether we subtransactions correctly in relation with each other
 */
CREATE TABLE tr_sub (id serial primary key, path text);

-- toplevel, subtxn, toplevel, subtxn, subtxn
BEGIN;
INSERT INTO tr_sub(path) VALUES ('1-top-#1');

SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('1-top-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-1-#2');
RELEASE SAVEPOINT a;

SAVEPOINT b;
SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#2');
RELEASE SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-#1');
RELEASE SAVEPOINT b;
COMMIT;

SELECT data FROM start_logical_replication('now');

-- check that we handle xlog assignments correctly
BEGIN;
-- nest 80 subtxns
SAVEPOINT subtop;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
-- assign xid by inserting
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#1');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#2');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#3');
RELEASE SAVEPOINT subtop;
INSERT INTO tr_sub(path) VALUES ('2-top-#1');
COMMIT;

SELECT data FROM start_logical_replication('now');


-- done, free logical replication slot
SELECT data FROM start_logical_replication('now');
SELECT stop_logical_replication();
