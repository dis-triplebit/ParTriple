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
private:
	/// store the chunks' position and the offset in chunk
	MemoryBuffer* hashTable;
	double* hashTableEntries;
	//MMapBuffer* secondaryHashTable;
	/// the current size of hash index;
	unsigned int hashTableSize;
	//unsigned int secondaryHashTableSize;

	ChunkManager& chunkManager;
	/// index type;
	unsigned chunkType;

	unsigned nextHashValue;// lastSecondaryHashTableOffset, secondaryHashTableOffset;
	unsigned firstValue;
	//ID* secondaryHashTableWriter;
protected:
	void insertFirstValue(unsigned value);
public:
	HashIndex(ChunkManager& _chunkManager, unsigned chunkType);
	virtual ~HashIndex();
	/// build hash index; chunkType: 1 or 2
	Status buildIndex(unsigned chunkType);
	/// search the chunk and offset in chunk by id; typeID 1 or 2
	Status getOffsetByID(double id, unsigned& offset, unsigned chunkType);
	void save(MMapBuffer*& buffer);
public:
	static HashIndex* load(ChunkManager& manager,unsigned chunkType, char* buffer, unsigned int& offset);
private:
	/// insert a record into index; position is the position of chunk in chunks vector.
	Status hashInsert(double id, unsigned int offset);
	unsigned hash(double id);
	unsigned next(double id);
};

#endif /* HASHINDEX_H_ */