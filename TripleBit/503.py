



load complete!
>>>queryLUBM5
>>>tripleID 0 selectivity : 3199424
tripleID 1 selectivity : 5001
tripleID 2 selectivity : 99987
tripleID 3 selectivity : 104426368
tripleID 4 selectivity : 12596397
tripleID 5 selectivity : 32390686
TripleNodeID: 1 Selectivity: 5001
TripleNodeID: 2 Selectivity: 99987
TripleNodeID: 0 Selectivity: 3199424
TripleNodeID: 4 Selectivity: 12596397
TripleNodeID: 5 Selectivity: 32390686
TripleNodeID: 3 Selectivity: 104426368
TripleNodeID: 1
scanOp: 0
varHeight 
2 0

TripleNodeID: 2
scanOp: 0
varHeight 
3 0

TripleNodeID: 0
scanOp: 14
varHeight 
3 1
2 1

TripleNodeID: 4
scanOp: 0
varHeight 
1 0

TripleNodeID: 5
scanOp: 14
varHeight 
1 1
2 2

TripleNodeID: 3
scanOp: 14
varHeight 
1 2
3 2

@pattern 1 find chunks time elapsed: 0 s
ChunkCount:8 taskEnqueue time elapsed: 0 s
#######find pattern 1 time elapsed: 0.007401
tripleNodeID 1 SIZE: 0
@pattern 2 find chunks time elapsed: 0 s
ChunkCount:125 taskEnqueue time elapsed: 0.001 s
#######find pattern 2 time elapsed: 0.005315
tripleNodeID 2 SIZE: 0
@pattern 0 find chunks time elapsed: 0 s
ChunkCount:2369 taskEnqueue time elapsed: 0.001 s
#######find pattern 0 time elapsed: 0.060709
tripleNodeID 0 SIZE: 99987
@pattern 4 find chunks time elapsed: 0 s
ChunkCount:18568 taskEnqueue time elapsed: 0.007 s
#######find pattern 4 time elapsed: 0.501013
tripleNodeID 4 SIZE: 0
@pattern 5 find chunks time elapsed: 0 s
ChunkCount:24248 taskEnqueue time elapsed: 0.008 s
#######find pattern 5 time elapsed: 1.03763
tripleNodeID 5 SIZE: 12596534
@pattern 3 find chunks time elapsed: 0 s
ChunkCount:76961 taskEnqueue time elapsed: 0.029 s
#######find pattern 3 time elapsed: 2.07862
tripleNodeID 3 SIZE: 12596396
after reduce p5 , got size
12596534
after reduce p0 , got size
20020
-----------------x part------------
-----------------xy part------------
-----------------x part------------
-----------------xy part------------
-----------------x part------------
-----------------xy part------------
get sort : 4.673
keyPos 1 2
verifyPos 0 3
ready get result join time:5.799

------------before getJoin------------
---buffer sig---
rows : 12596534
cols : 2
sorted : 1
sortKey : 1
first row : 37877 4 
last row : 164411205 45130 
---buffer sig---
rows : 20020
cols : 2
sorted : 1
sortKey : 1
first row : 7 4 
last row : 31655864 45130 
---buffer sig---
rows : 12596396
cols : 2
sorted : 1
sortKey : 1
first row : 1996 7 
last row : 164416388 164415381 
21467803 38 21466707 21467803 
8889453 120 8888315 8889453 
31129148 228 31128283 31129148 
13686757 245 13685753 13686757 
7450439 282 7449384 7450439 
7856826 2068 7856026 7856826 
15627623 2151 15626793 15627623 
18553177 2525 18552271 18553177 
15411855 3260 15410758 15411855 
1827491 4399 1826448 1827491 
10731877 5174 10730997 10731877 
23764493 6831 23763310 23764493 
17077401 7575 17076218 17077401 
24901136 7606 24900130 24901136 
20907247 7657 20906206 20907247 
30615117 8287 30614309 30615117 
29164661 8444 29163568 29164661 
9213086 8445 9212127 9213086 
12762159 11827 12761011 12762159 
16393193 18446 16391929 16393193 
18374160 22786 18373098 18374160 
10969519 44331 10968444 10969519 

------------after getJoin------------
ans: 22
gR: 264768437
getResult_Join time:400.1
 time elapsed: 409.788 s