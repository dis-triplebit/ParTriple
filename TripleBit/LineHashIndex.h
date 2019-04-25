//
// Created by Administrator on 2019/4/23.
//

#ifndef LineHashIndexEnhance_H_
#define LineHashIndexEnhance_H_

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"

class LineHashIndex {
public:
	struct Point{
		double x;
		double y;
	};

	struct chunkMetaData
		//except the firstChunk , minIDx, minIDy and offsetBegin will not change with update
		//offsetEnd may change but I think it makes little difference to the result
		//by Frankfan
	{
		double minIDx;    //The minIDx of a chunk
		double minIDy;		//The minIDy of a chunk
        unsigned long offsetBegin;	//The beginoffset of a chunk(not include MetaData and relative to the startPtr)
	};

	enum IndexType { SUBJECT_INDEX, OBJECT_INDEX};
    enum ObjectType {INT,FLOAT,DOUBLE};

private:

	//TODO
	MemoryBuffer* idTable;
	double* idTableEntries;
	ChunkManager& chunkManager;
	IndexType indexType;
	ObjectType xyType;
	size_t tableSize;   //chunk number plus 1,because the end edge
	char* LineHashIndexEnhanceBase; //used to do update

	//line parameters;
	double upperk[4];
	double upperb[4];
	double lowerk[4];
	double lowerb[4];

	double startID[4];

public:
	//some useful thing about the chunkManager
	uchar *startPtr, *endPtr;
	vector<chunkMetaData> chunkMeta;

private:
	void insertEntries(double id);
	size_t searchChunkFrank(double id);
	bool buildLine(int startEntry, int endEntry, int lineNo);
	double MetaID(size_t index);
	double MetaYID(size_t index);
public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType index_type, ObjectType xy_type);
	//TODO 根据ChunkType的不同,然后创建不同的索引
	Status buildIndex(unsigned chunkType);
	void getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd);
	size_t searchChunk(double xID, double yID);
	bool searchChunk(double xID, double yID, size_t& offsetID);
	bool isQualify(size_t offsetId, double xID, double yID);
	unsigned int getTableSize() { return tableSize; }
	size_t save(MMapBuffer*& indexBuffer);
	void saveDelta(MMapBuffer*& indexBuffer, size_t& offset ,const size_t predicateSize);
	virtual ~LineHashIndex();
	void updateChunkMetaData(int offsetId, unsigned chunkType);
	void updateLineIndex();
private:
	//判断Buffer已经被写满
	bool isBufferFull();
public:
	static LineHashIndex* load(ChunkManager& manager, IndexType index_type, ObjectType xy_type, char* buffer, size_t& offset);
};

#endif /* LineHashIndexEnhance_H_ */
