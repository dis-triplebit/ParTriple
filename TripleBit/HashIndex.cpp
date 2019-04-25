#include "HashIndex.h"
#include "BitmapBuffer.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"
//ChunkManager和索引类型
HashIndex::HashIndex(ChunkManager& _chunkManager, IndexType type) : chunkManager(_chunkManager), type(type) {
    // TODO Auto-generated constructor stub
    //how to init two-dim vector
    //store the chunks' position and the offset in chunk
    hashTable = NULL;
    hashTableSize = 0;
    //ID的向量
    hashTableEntries = NULL;
    firstValue = 0;
//	secondaryHashTable = NULL;
//	secondaryHashTableSize = 0;
}

HashIndex::~HashIndex() {
    // TODO Auto-generated destructor stub
    if(hashTable != NULL) {
        //删除临时文件＄1�7
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

Status HashIndex::hashInsert(ID id, unsigned int offset) {
    //通过firstValue获取差值
    id = id - firstValue;
    for(; id / HASH_RANGE >= hashTableSize; ) {
        hashTableEntries[hashTableSize] = 0;
        //扩容为500
        hashTable->resize(HASH_CAPACITY_INCREASE);
        //通过hashTable得到的是char指针类型的数组，这个转换为ID*类型
        hashTableEntries = (double*)hashTable->getBuffer();
        //增加hashTableSize
        hashTableSize += HASH_CAPACITY_INCREASE / sizeof(unsigned);
//		hashTableEntries[hashTableSize] = firstValue;
    }
    //
    if(id >= nextHashValue) {
        //
        hashTableEntries[id / HASH_RANGE] = offset;
        while(nextHashValue <= id) nextHashValue += HASH_RANGE;
    }

    return OK;
}

void HashIndex::insertFirstValue(unsigned value)
{
    //默认的HASH_RANGE为200,[0,200)区间的firstValue为0,[200,400)区间的firstValue为1
    firstValue = ((unsigned)(value / HASH_RANGE)) * HASH_RANGE;
    //ID* hashTableEntries = (ID*)hashTable->getBuffer();
//	hashTableEntries[hashTableSize] = firstValue;
}

static void getTempIndexFilename(string& filename, int pid, unsigned type, unsigned chunkType)
{
    //清空字符串内容
    filename.clear();
    //添加路径
    filename.append(DATABASE_PATH);
    //添加索引名称
    filename.append("tempIndex_");
    char temp[4];
    sprintf(temp, "%d", pid);
    filename.append(temp);
    filename.append("_");
    sprintf(temp, "%d", type);
    filename.append(temp);
    filename.append("_");
    sprintf(temp, "%d", chunkType);
    filename.append(temp);
}

//
Status HashIndex::buildIndex(unsigned chunkType,bool OSFlag)
{
    if(hashTable == NULL) {
        string filename;
        //得到临时索引的名称，第一个参数是文件名，第二个参数是Chunk内的predicate id，第三个参数是元数据SO块还是OS块
        getTempIndexFilename(filename, chunkManager.meta->pid, chunkManager.meta->type, chunkType);
        /// 初始化Buffer，默认大小为500
        hashTable = new MemoryBuffer(HASH_CAPACITY);
        //hashTableSize的大小因为需要使用char类型来存储ID
        hashTableSize = hashTable->getSize() / sizeof(double) - 1;
        //char*强转为ID*
        hashTableEntries = (double*)hashTable->getBuffer();
        //初始值为0
        nextHashValue = 0;
    }

    const uchar* begin, *limit, *reader;
    double x,y;
    double startID = 0;
    unsigned _offset = 0;
    //如果ChunkType为1,
    if(chunkType == 0) {
        begin = reader = chunkManager.getStartPtr(0);
        //表示chunk只是初始化了，并没有任何数据
        if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
            return OK;

        x = 0;
        //TODO @ganpeng
        reader = Chunk::readXId(reader, x,OSFlag,chunkType);
        ///插入第一个值
        insertFirstValue(x);
        //hash插入索引位置
        hashInsert(x, 1);
        //startID为0,200,400,600
        while(startID <= x) startID += HASH_RANGE;
        //TODO 在原来方法上加了一个chunkType表示某种类型
        reader = Chunk::skipId(reader, 1,chunkType);
        //Chunk填充的最后一个位置
        limit = chunkManager.getEndPtr(0);
        //遍历,找到合适的位置插入
        while(reader < limit) {
            x = 0;
            _offset = reader - begin + 1;
            reader = Chunk::readXId(reader,x,OSFlag,chunkType);
            if(x >= startID) {
                hashInsert(x, _offset);
                while(startID <= x) startID += HASH_RANGE;
            } else if(x == 0)
                break;
            reader = Chunk::skipId(reader, 1);
        }
    }
    //这个类型就是Float类型
    if(chunkType == 1) {
        //如果chunk内没有数据那么就直接返回
        if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
            return OK;
        //chunk数据开始的位置
        begin = reader = chunkManager.getStartPtr(1);

        x = y = 0;
        //读取Object的开始位置
        reader = Chunk::readXId(reader, x,OSFlag,chunkType);
//        //读取Subject的开始位置
//        reader = Chunk::readYId(reader, y,OSFlag,chunkType);
        //插入的值为
        insertFirstValue(x);
        hashInsert(x , 1);
        while(startID <= x ) startID += HASH_RANGE;

        limit = chunkManager.getEndPtr(1);
        while(reader < limit) {
            x = y = 0;
            _offset = reader - begin + 1;
            //TODO ganpeng
            reader = Chunk::readXId(reader, x,OSFlag,chunkType);
            //reader = Chunk::readYId(reader, y);

            if(x + y >= startID) {
                hashInsert(x, _offset);
                while(startID <= x + y) startID += HASH_RANGE;
            } else if( x == 0) {
                break;
            }
        }
    }
//这个类型就是double类型
    if(chunkType == 2) {
        //如果chunk内没有数据那么就直接返回
        if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
            return OK;
        //chunk数据开始的位置
        begin = reader = chunkManager.getStartPtr(2);

        x = y = 0;
        //读取Object的开始位置
        reader = Chunk::readXId(reader, x,OSFlag,chunkType);
//        //读取Subject的开始位置
//        reader = Chunk::readYId(reader, y,OSFlag,chunkType);
        //插入的值为
        insertFirstValue(x);
        hashInsert(x , 1);
        while(startID <= x ) startID += HASH_RANGE;

        limit = chunkManager.getEndPtr(2);
        while(reader < limit) {
            x = y = 0;
            _offset = reader - begin + 1;
            //TODO ganpeng
            reader = Chunk::readXId(reader, x,OSFlag,chunkType);
            //reader = Chunk::readYId(reader, y);

            if(x + y >= startID) {
                hashInsert(x, _offset);
                while(startID <= x + y) startID += HASH_RANGE;
            } else if( x == 0) {
                break;
            }
        }
    }
    return OK;
}

Status HashIndex::getOffsetByID(double id, unsigned& offset, unsigned typeID)
{
    unsigned pBegin = hash((ID)id);
    unsigned pEnd = next((ID)id);

    const uchar* beginPtr = NULL, *reader = NULL;
    int low = 0, high = 0, mid = 0, lastmid = 0;
    double x,y;
    //如果Triple的数量为0那么就是没有
    if(chunkManager.getTripleCount(typeID) == 0)
        return NOT_FOUND;

    if(pBegin != 0) {
        low = pBegin - 1;
        high = pEnd;
    } else {
        low = 0;
        high = offset = pEnd - 1;
        return OK;
    }
    reader = chunkManager.getStartPtr(typeID) + low;
    beginPtr = chunkManager.getStartPtr(typeID);
    Chunk::readXId(reader, x,OSFlag,typeID);

    if ( x == id ) {
        offset = low;
        return OK;
    } else if(x > id)
        return OK;
    //二分查找
    while(low <= high) {
        x = 0;
        lastmid = mid = low + (high - low) / 2;
        reader = Chunk::skipBackward(beginPtr + mid,OSFlag,typeID);
        mid = reader - beginPtr;
        Chunk::readXId(reader, x);

        if(x == id){
            while(mid >= (int)MemoryBuffer::pagesize) {
                x = 0;
                lastmid = mid;
                mid = mid - MemoryBuffer::pagesize;
                reader = Chunk::skipBackward(beginPtr + mid,OSFlag,typeID);
                mid = beginPtr - reader;
                if(mid <= 0) {
                    offset = 0;
                    return OK;
                }
                Chunk::readXId(reader, x);
                if(x < id)
                    break;
            }
            //mid = lastmid;
            if(mid < (int)MemoryBuffer::pagesize)
                mid = 0;
            while(mid <= lastmid) {
                x = 0;
                reader = Chunk::readXId(beginPtr + mid, x,OSFlag,typeID);
                reader = Chunk::skipId(reader, 1,OSFlag,typeID);
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

    if(mid <= 0)
        offset = 0;
    else //if not found, offset is the first id which is bigger than the given id.
        offset = Chunk::skipBackward(beginPtr + mid,OSFlag,typeID) - beginPtr;

    return OK;
}

unsigned HashIndex::hash(ID id)
{
    //得到差值
    id = id - firstValue;
    if((int)id < 0)
        return 0;
    //得到商
    unsigned firstHash = id / HASH_RANGE;
    if(firstHash >= hashTableSize)
        return hashTableEntries[hashTableSize - 1];

    return hashTableEntries[firstHash];
}

unsigned HashIndex::next(ID id)
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
    *(double*)writeBuf = (nextHashValue / HASH_RANGE) * sizeof(double); writeBuf += sizeof(double);
    memcpy(writeBuf, (char*)hashTableEntries, (nextHashValue / HASH_RANGE) * sizeof(double));
    buffer->flush();

//	cout<<firstValue<<endl;
    delete hashTable;
    hashTable = NULL;
}

HashIndex* HashIndex::load(ChunkManager& manager, IndexType type,bool OSFlag, char* buffer, unsigned int& offset)
{
    HashIndex* index = new HashIndex(manager, type);
    size_t size = ((ID*)(buffer + offset))[0];
    index->hashTableEntries = (ID*)(buffer + offset + sizeof(double));
    index->hashTableSize = size / sizeof(double) - 1;
    index->firstValue = index->hashTableEntries[index->hashTableSize];
    index->OSFlag=OSFlag;
//	cout<<index->firstValue<<endl;
    offset += size + sizeof(double);
    return index;
}