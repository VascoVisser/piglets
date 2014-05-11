DEFINE TupToBAg nl.devnet.piglets.eval.TupleToBag();

A = LOAD 'data' as (f1:int, f2:());
out = FOREACH A GENERATE f1, FLATTEN(TupToBAg(f2));