#include "../TripleBit.h"
#include "../ThreadPool.h"
class EntityIDBuffer;
class SemiJoin
{
public:
	void Join(EntityIDBuffer* entBuffer1,map<ID,EntityIDBuffer*>* entBuffer2,
			int joinKey1, int joinKey2);
	void Join(EntityIDBuffer* entBuffer1,EntityIDBuffer*entBuffer2,
				int joinKey1, int joinKey2,bool sort1,bool sort2,bool modify1,bool modify2);
	void Join10(EntityIDBuffer* entBuffer1,EntityIDBuffer*entBuffer2,
					int joinKey1, int joinKey2,bool modify1,bool modify2);
	void Join01(EntityIDBuffer* entBuffer1,EntityIDBuffer*entBuffer2,
						int joinKey1, int joinKey2,bool modify1,bool modify2);
	void Join00(EntityIDBuffer* entBuffer1,EntityIDBuffer*entBuffer2,
						int joinKey1, int joinKey2,bool modify1,bool modify2);

};
