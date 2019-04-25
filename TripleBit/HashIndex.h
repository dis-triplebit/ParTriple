/*
 * HashIndex.h
 *
 *   Created on: 2010-6-13
 *       Author: IBM
 *  Modified on: 2010-9-20
 */

#ifndef HASHINDEX_H_
#define HASHINDEX_H_

class ChunkManager;
class MemoryBuffer;
class MMapBuffer;

#include "TripleBit.h"

class HashIndex {
public:
	enum IndexType { ID,FLOAT,DOUBLE};
private:
	// store the chunks' position and the offset in chunk
	MemoryBuffer* hashTable;
	double* hashTableEntries;
	//MMapBuffer* secondaryHashTable;
	/// the current size of hash index;
	unsigned int hashTableSize;
	//unsigned int secondaryHashTableSize;

	ChunkManager& chunkManager;
	/// index type;
	IndexType type;
	bool OSFlag;

	unsigned nextHashValue;// lastSecondaryHashTableOffset, secondaryHashTableOffset;
	unsigned firstValue;
	//ID* secondaryHashTableWriter;
protected:
	void insertFirstValue(double value);
public:
	HashIndex(ChunkManager& _chunkManager,bool OSFlag,IndexType type);
	virtual ~HashIndex();
	/// build hash index; chunkType: 1 or 2
	Status buildIndex(unsigned chunkType);
	/// search the chunk and offset in chunk by id; typeID 1 or 2
	Status getOffsetByID(double id, unsigned& offset, unsigned typeID);
	void save(MMapBuffer*& buffer);
public:
	static HashIndex* load(ChunkManager& manager, IndexType type, char* buffer, unsigned int& offset);
private:
	/// insert a record into index; position is the position of chunk in chunks vector.
	Status hashInsert(double id, unsigned int offset);
	unsigned hash(ID id);
	unsigned next(ID id);
};

#endif /* HASHINDEX_H_ */