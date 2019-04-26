#include "HashIndex.h"
#include "BitmapBuffer.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"

HashIndex::HashIndex(ChunkManager& _chunkManager, unsigned chunkType) : chunkManager(_chunkManager), chunkType(chunkType) {
	//how to init two-dim vector
	hashTable = NULL;
	hashTableSize = 0;
	hashTableEntries = NULL;
	firstValue = 0;
//	secondaryHashTable = NULL;
//	secondaryHashTableSize = 0;
}

HashIndex::~HashIndex() {
	if(hashTable != NULL) {
		//删除临时文件
		//hashTable->discard();
		delete hashTable;
		hashTable = NULL;
	}
//	if(secondaryHashTable != NULL)
//		delete secondaryHashTable;
	hashTableEntries = NULL;
	hashTableSize = 0;
//	secondaryHashTableSize = 0;
}

Status HashIndex::hashInsert(double id, unsigned int offset) {
	id = id - firstValue;
	for(; id / HASH_RANGE >= hashTableSize; ) {
		hashTableEntries[hashTableSize] = 0;
		hashTable->resize(HASH_CAPACITY_INCREASE);
		hashTableEntries = (double*)hashTable->getBuffer();
		hashTableSize += HASH_CAPACITY_INCREASE / sizeof(double);
//		hashTableEntries[hashTableSize] = firstValue;
	}

	if(id >= nextHashValue) {
		hashTableEntries[(ID)id / HASH_RANGE] = offset;
		while(nextHashValue <= id) nextHashValue += HASH_RANGE;
	}

	return OK;
}

void HashIndex::insertFirstValue(unsigned value)
{
	firstValue = (value / HASH_RANGE) * HASH_RANGE;
	//ID* hashTableEntries = (ID*)hashTable->getBuffer();
//	hashTableEntries[hashTableSize] = firstValue;
}

static void getTempIndexFilename(string& filename, int pid, unsigned type, unsigned chunkType)
{
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("tempIndex_");
	char temp[4];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	filename.append("_");
	sprintf(temp, "%d", type);
	filename.append(temp);
	filename.append("_");
	sprintf(temp, "%d", chunkType<3?"SO":"OS");
	filename.append(temp);
}

Status HashIndex::buildIndex(unsigned chunkType)
{
	if(hashTable == NULL) {
		string filename;
		getTempIndexFilename(filename, chunkManager.meta->pid, chunkManager.meta->type, chunkType);
		hashTable = new MemoryBuffer(HASH_CAPACITY);
		hashTableSize = hashTable->getSize() / sizeof(unsigned) - 1;
		hashTableEntries = (double*)hashTable->getBuffer();
		nextHashValue = 0;
	}

	const uchar* begin, *limit, *reader;
	double x,y;
	double startID = 0;
	unsigned _offset = 0;
	if(chunkType < 3) {
		begin = reader = chunkManager.getStartPtr(chunkType);
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0;
		reader = Chunk::readXId(reader,x,chunkType);

		insertFirstValue(x);
		hashInsert(x, 1);
		while(startID <= x) startID += HASH_RANGE;
		reader = Chunk::skipId(reader,1,chunkType);

		limit = chunkManager.getEndPtr(chunkType);
		while(reader < limit) {
			x = 0;
			_offset = reader - begin + 1;
			reader = Chunk::readXId(reader,x,chunkType);
			if(x >= startID) {
				hashInsert(x, _offset);
				while(startID <= x) startID += HASH_RANGE;
			} else if(x == 0)
				break;
			reader = Chunk::skipId(reader, 1,chunkType);
		}
	}
	else {
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;
		int dataType=chunkType%3;
		begin = reader = chunkManager.getStartPtr(dataType);

		x = y = 0;
		reader = Chunk::readXId(reader, x,dataType);
		//reader = Chunk::readYId(reader, y,dataType);
		insertFirstValue(x);
		hashInsert(x, 1);
		while(startID <= x) startID += HASH_RANGE;

		limit = chunkManager.getEndPtr(dataType);
		while(reader < limit) {
			x = 0;
			_offset = reader - begin + 1;
			reader = Chunk::readXId(reader, x,chunkType);
			//reader = Chunk::readYId(reader, y,chunkType);

			if(x >= startID) {
				hashInsert(x, _offset);
				while(startID <= x) startID += HASH_RANGE;
			} else if(  x == 0) {
				break;
			}
		}
	}

	return OK;
}

Status HashIndex::getOffsetByID(double id, unsigned& offset, unsigned chunkType)
{
	unsigned pBegin = hash(id);
	unsigned pEnd = next(id);

	const uchar* beginPtr = NULL, *reader = NULL;
	int low = 0, high = 0, mid = 0, lastmid = 0;
	double x,y;

	if(chunkManager.getTripleCount(chunkType%objTypeNum) == 0)
		return NOT_FOUND;

	if (chunkType < 3 ) {
		if(pBegin != 0) {
			low = pBegin - 1;
			high = pEnd;
		} else {
			low = 0;
			high = offset = pEnd - 1;
			return OK;
		}
		reader = chunkManager.getStartPtr(chunkType) + low;
		beginPtr = chunkManager.getStartPtr(chunkType);
		Chunk::readXId(reader, x,chunkType);

		if ( x == id ) {
			offset = low;
			return OK;
		} else if(x > id)
			return OK;

		while(low <= high) {
			x = 0;
			lastmid = mid = low + (high - low) / 2;
			reader = Chunk::skipBackward(beginPtr + mid,chunkType);
			mid = reader - beginPtr;
			Chunk::readXId(reader, x,chunkType);

			if(x == id){
				while(mid >= (int)MemoryBuffer::pagesize) {
					x = 0;
					lastmid = mid;
					mid = mid - MemoryBuffer::pagesize;
					reader = Chunk::skipBackward(beginPtr + mid,chunkType);
					mid = beginPtr - reader;
					if(mid <= 0) {
						offset = 0;
						return OK;
					}
					Chunk::readXId(reader, x,chunkType);
					if(x < id)
						break;
				}
				//mid = lastmid;
				if(mid < (int)MemoryBuffer::pagesize)
					mid = 0;
				while(mid <= lastmid) {
					x = 0;
					reader = Chunk::readXId(beginPtr + mid, x,chunkType);
					reader = Chunk::skipId(reader,1,chunkType);
					if(x >= id) {
						offset = mid;
						return OK;
					}
					mid = reader - beginPtr;
					if(mid == lastmid) {
						offset = mid;
						return OK;
					}
				}
				return OK;
			} else if ( x > id ) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}
	else{
		int dataType=chunkType%3;
		if(pBegin != 0) {
			low = pBegin - 1;
			high = pEnd;
		} else {
			low = 0;
			high = offset = pEnd - 1;
			return OK;
		}
		reader = chunkManager.getStartPtr(dataType) + low;
		beginPtr = chunkManager.getStartPtr(dataType);

		reader = Chunk::readXId(reader, x,chunkType);
		//reader = Chunk::readYId(reader, y);
		if ( x == id ) {
			offset = low;
			return OK;
		}

		if(x > id)
			return OK;

		while(low <= high) {
			x = 0;
			mid = low + (high - low) / 2;//(low + high) / 2;
			reader = Chunk::skipBackward(beginPtr + mid,chunkType);
			lastmid = mid = reader - beginPtr;
			reader = Chunk::readXId(reader, x,chunkType);
			//reader = Chunk::readYId(reader, y,chunkType);
			if(x == id){
				while(mid >= (int)MemoryBuffer::pagesize) {
					x = 0; y = 0;
					lastmid = mid;
					mid = mid - MemoryBuffer::pagesize;
					reader = Chunk::skipBackward(beginPtr + mid,chunkType);
					mid = beginPtr - reader;
					if (mid <= 0) {
						offset = 0;
						return OK;
					}
					Chunk::readXYId(reader,x,y,chunkType);
					if (x < id)
						break;
				}

				if(mid < (int)MemoryBuffer::pagesize)
					mid = 0;
				while( mid <= lastmid) {
					x = y = 0;
					reader = Chunk::readXId(Chunk::readXId(beginPtr + mid, x,chunkType), y,chunkType);
					if( x >= id) {
						offset = mid;
						return OK;
					}

					mid = reader - beginPtr;
					if(mid == lastmid) {
						offset = mid;
						return OK;
					}
				}
			} else if ( x > id ) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}
	if(mid <= 0)
		offset = 0;
	else //if not found, offset is the first id which is bigger than the given id.
		offset = Chunk::skipBackward(beginPtr + mid,chunkType) - beginPtr;
	
	return OK;
}

unsigned HashIndex::hash(double id)
{
	id = id - firstValue;
	if((int)id < 0)
		return 0;

	unsigned firstHash = id / HASH_RANGE;
	if(firstHash >= hashTableSize)
		return hashTableEntries[hashTableSize - 1];

	return hashTableEntries[firstHash];
}

unsigned HashIndex::next(double id)
{
	id = id - firstValue;
	if((int)id < 0)
		return 1;
	unsigned firstHash;
	firstHash = id / HASH_RANGE + 1;
	if(firstHash >= hashTableSize)
		return hashTableEntries[hashTableSize - 1];

	while(hashTableEntries[firstHash] == 0) {
		firstHash++;
	}

	return hashTableEntries[firstHash];
}

char* writeData(char* writer, double data)
{
	memcpy(writer, &data, sizeof(double));
	return writer+sizeof(double);
}

const char* readData(const char* reader,double& data)
{
	memcpy(&data, reader, sizeof(double));
	return reader+sizeof(double);
}

void HashIndex::save(MMapBuffer*& buffer)
{
//	hashTable->flush();
	//size_t size = buffer->getSize();
	char* writeBuf;

	if(buffer == NULL) {
		buffer = MMapBuffer::create(string(string(DATABASE_PATH) + "BitmapBuffer_index").c_str(), hashTable->getSize() + sizeof(double));
		writeBuf = buffer->get_address();
	} else {
		size_t size = buffer->getSize();
		writeBuf = buffer->resize(hashTable->getSize() + sizeof(double)) + size;
	}

//	assert(hashTableSize == hashTable->getSize() / 4 - 1);
	hashTableEntries[nextHashValue / HASH_RANGE] = firstValue;
	*(ID*)writeBuf = (nextHashValue / HASH_RANGE) * sizeof(double); writeBuf += sizeof(double);
	memcpy(writeBuf, (char*)hashTableEntries, (nextHashValue / HASH_RANGE) * sizeof(double));
	buffer->flush();

//	cout<<firstValue<<endl;
	delete hashTable;
	hashTable = NULL;
}

HashIndex* HashIndex::load(ChunkManager& manager, unsigned chunkType, char* buffer, unsigned int& offset)
{
	HashIndex* index = new HashIndex(manager, chunkType);
	size_t size = ((ID*)(buffer + offset))[0];
	index->hashTableEntries = (double*)(buffer + offset + sizeof(double));
	index->hashTableSize = size / sizeof(double) - 1;
	index->firstValue = index->hashTableEntries[index->hashTableSize];

//	cout<<index->firstValue<<endl;
	offset += size + sizeof(double);
	return index;
}