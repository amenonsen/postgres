Parsed test spec with 2 sessions

starting permutation: s1a s2a s1b s2b s1c s2c s1d s2d s1e s2e s1f s2f s1g s2g
step s1a: INSERT INTO x(a,b) values (1,'foo');
step s2a: SELECT wait_for(1);
wait_for       

1              
step s1b: BEGIN;
step s2b: BEGIN;
step s1c: UPDATE x SET b='baz' WHERE a=1;
step s2c: UPDATE x SET b='quux' WHERE a=1;
step s1d: COMMIT;
step s2d: COMMIT;
step s1e: INSERT INTO x(a,b) values (2,'bar');
step s2e: INSERT INTO x(a,b) values (3,'baz');
step s1f: SELECT wait_for(3);
wait_for       

3              
step s2f: SELECT wait_for(2);
wait_for       

2              
step s1g: SELECT * from x order by a;
a              b              

1              quux           
2              bar            
3              baz            
step s2g: SELECT * from x order by a;
a              b              

1              quux           
2              bar            
3              baz            
