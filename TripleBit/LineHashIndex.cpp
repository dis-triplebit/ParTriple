//
// Created by Administrator on 2019/4/23.
//

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>

/**
 * linear fit;
 * f(x)=kx + b;
 * used to calculate the parameter k and b;
 */
static bool calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo)
{
	//如果点的个数是小于2的，当然不能画线啦
	if (pointNo < 2)
		return false;

	double mX, mY, mXX, mXY;
	mX = mY = mXX = mXY = 0;
	int i;
	for (i = 0; i < pointNo; i++)
	{
		mX += a[i].x;
		mY += a[i].y;
		mXX += a[i].x * a[i].x;
		mXY += a[i].x * a[i].y;
	}

	if (mX * mX - mXX * pointNo == 0)
		return false;

	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);
	return true;
}

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType index_type, ObjectType xy_type) :
		chunkManager(_chunkManager), indexType(index_type), xyType(xy_type)
{
	// MemoryBuffer* idTable
	idTable = NULL;
	// ID* idTableEntries
	idTableEntries = NULL;
	//char* LineHashIndexEnhanceBase; //used to do update
	LineHashIndexEnhanceBase = NULL;

	startID[0] = startID[1] = startID[2] = startID[3] = UINT_MAX;
}

LineHashIndex::~LineHashIndex()
{
	idTable = NULL;
	idTableEntries = NULL;
	startPtr = NULL;
	endPtr = NULL;
	chunkMeta.clear();
	swap(chunkMeta, chunkMeta);
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
//这个函数的用意是建立论文中的线
bool LineHashIndex::buildLine(int startEntry, int endEntry, int lineNo)
{
	vector<Point> vpt;
	Point pt;
	int i;

	//build lower limit line;
	for (i = startEntry; i < endEntry; i++)
	{
		//这里面存储的ChunkID
		pt.x = idTableEntries[i];
		pt.y = i;
		vpt.push_back(pt);
	}

	double ktemp, btemp;
	//统计这些点的个数
	int size = vpt.size();
	//如果画线失败,那么就返回,不要有任何犹豫
	if (calculateLineKB(vpt, ktemp, btemp, size) == false)
		return false;
	//斜率
	double difference = btemp; //(vpt[0].y - (ktemp * vpt[0].x + btemp));
	double difference_final = difference;

	for (i = 1; i < size; i++)
	{
		// 偏移
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		//cout<<"differnce: "<<difference<<endl;
		// 得出最小的偏移
		if ((difference < difference_final) == true)
			difference_final = difference;
	}
	//最小的偏移
	btemp = difference_final;
	//将每一页的k,b以及他的开始ID计算出来
	lowerk[lineNo] = ktemp;
	lowerb[lineNo] = btemp;
	startID[lineNo] = vpt[0].x;

	//每一次计算后重新开始
	vpt.resize(0);
	//build upper limit line;
	for (i = startEntry; i < endEntry; i++)
	{
		pt.x = idTableEntries[i + 1];
		pt.y = i;
		vpt.push_back(pt);
	}

	size = vpt.size();
	calculateLineKB(vpt, ktemp, btemp, size);

	difference = btemp;		//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	difference_final = difference;

	for (i = 1; i < size; i++)
	{
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		if (difference > difference_final)
			difference_final = difference;
	}
	btemp = difference_final;

	upperk[lineNo] = ktemp;
	upperb[lineNo] = btemp;
	return true;
}

static ID splitID[3] =
		{ 255, 65535, 16777215 };

Status LineHashIndex::buildIndex(unsigned chunkType) // chunkType: 1: x>y ; 2: x<y
{
	//如果这个MemoryBuffer是空，就分配内存同时初始化它
	if (idTable == NULL)
	{
		//默认的大小是500
		idTable = new MemoryBuffer(HASH_CAPACITY);
		//将char*类型转换为ID*
		idTableEntries = (double*) idTable->getBuffer();
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;
	//用于表subject和object
	double x;//double y;

	int lineNo = 0;
	int startEntry = 0, endEntry = 0;
	if (chunkType <3)
	{


		//当数据类型是ID时

		//0表示ID类型,由于chunkType是SO块，因而这个是第一块
		reader = chunkManager.getStartPtr(chunkType);
		limit = chunkManager.getEndPtr(chunkType);
		begin = reader;
		//如果地址块没有被初始化，那么就直接返回
		if (begin == limit){
			return OK;
		}
		//数据块，表示这一块的最小ID，已经用了的ID
		MetaData* metaData = (MetaData*) reader;
		//这里需要强制转换成ID
		x = metaData->minID;
		//插入最小的ID
		insertEntries(x);

		reader = reader + (int) (MemoryBuffer::pagesize - sizeof(ChunkManagerMeta));
		//将minID插入到MemeoryBuffer中
		while (reader < limit)
		{

			metaData = (MetaData*) reader;
			x = metaData->minID;
			insertEntries(x);
			//如果大于分离ID，那么就计算这个块的上下线
			if (x > splitID[lineNo])
			{
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true)
				{
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		//TODO @ganpeng
		reader = Chunk::skipBackward(limit, begin, chunkType);
		x = 0;
		//TODO @ganpeng
		Chunk::readXId(reader,x,chunkType);
		insertEntries(x);

		startEntry = endEntry;
		endEntry = tableSize;
		if (buildLine(startEntry, endEntry, lineNo) == true)
		{
			++lineNo;
		}
	}
	else
	{
		int dataType=chunkType%3;
		reader = chunkManager.getStartPtr(dataType);
		limit = chunkManager.getEndPtr(dataType);
		begin = reader;
		if (begin == limit){
			return OK;
		}

		while (reader < limit)
		{
			MetaData* metaData = (MetaData*) reader;
			x = metaData->minID;
			insertEntries(x);

			if (x > splitID[lineNo])
			{
				startEntry = endEntry;
				endEntry = tableSize;
				if (buildLine(startEntry, endEntry, lineNo) == true)
				{
					++lineNo;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}
		x = 0;
		//TODO @ganpeng,
		reader = Chunk::skipBackward(limit, begin,chunkType);
		//TODO @ganpeng
		Chunk::readXId(limit,x,chunkType);
		insertEntries(x);
		startEntry = endEntry;
		endEntry = tableSize;
		if (buildLine(startEntry, endEntry, lineNo) == true)
		{
			++lineNo;
		}
	}
	return OK;
}

//TODO 这里需要改变因为ID的类型改变了，所以idTable的长度就应该根据不同的类型来改变
bool LineHashIndex::isBufferFull()
{
	return tableSize >= idTable->getSize() / sizeof(double);
}

//TODO 插入ID，这个应该改变
void LineHashIndex::insertEntries(double id)
{
	//如果buffer已经满了那么
	if (isBufferFull())
	{
		//那么就将IdTable继续扩容
		idTable->resize(HASH_CAPACITY);
		//这个指向它新的头地址
		idTableEntries = (double*) idTable->get_address();
	}
	idTableEntries[tableSize] = id;

	tableSize++;
}

double LineHashIndex::MetaID(size_t index)
{
	assert(index < chunkMeta.size());
	return chunkMeta[index].minIDx;
}

double LineHashIndex::MetaYID(size_t index)
{
	assert(index < chunkMeta.size());
	return chunkMeta[index].minIDy;
}
// 通过id搜索，返回offset
size_t LineHashIndex::searchChunkFrank(double id)
{
	size_t low = 0, mid = 0, high = tableSize - 1;

	if (low == high){
		return low;
	}
	while (low < high)
	{
		mid = low + (high-low) / 2;
		while (MetaID(mid) == id)
		{
			//从minId小于它的chunk搜索
			if (mid > 0 && MetaID(mid - 1) < id){
				return mid - 1;
			}
			if (mid == 0){
				return mid;
			}
			//也就是说mid-1的chunk的最小id还是等于id
			mid--;
		}
		//id大于chunk的最小id
		if (MetaID(mid) < id){
			low = mid + 1;
		}
			//id小于chunk的最小id
		else if (MetaID(mid) > id){
			high = mid;
		}
	}
	//返回chunk id
	if (low > 0 && MetaID(low) >= id){
		return low - 1;
	}
	else{
		return low;
	}
}

size_t LineHashIndex::searchChunk(double xID, double yID){
	//如果存储id的最小值或者没有索引那么直接返回0
	if(MetaID(0) > xID || tableSize == 0){
		return 0;
	}
	// 二分查找搜索offsetID，也就是chunk的块号
	size_t offsetID = searchChunkFrank(xID);
	//
	if(offsetID == tableSize-1){
		return offsetID-1;
	}
	while(offsetID < tableSize-2){
		if(MetaID(offsetID+1) == xID){
			if(MetaYID(offsetID+1) > yID){
				return offsetID;
			}
			else{
				offsetID++;
			}
		}
		else{
			return offsetID;
		}
	}
	return offsetID;
}

bool LineHashIndex::searchChunk(double xID, double yID, size_t& offsetID)
//return the  exactly which chunk the triple(xID, yID) is in
{
	if(MetaID(0) > xID || tableSize == 0){
		offsetID = 0;
		return false;
	}

	offsetID = searchChunkFrank(xID);
	if (offsetID == tableSize-1)
	{
		return false;
	}

	while (offsetID < tableSize - 2)
	{
		if (MetaID(offsetID + 1) == xID)
		{
			if (MetaYID(offsetID + 1) > yID)
			{
				return true;
			}
			else
			{
				offsetID++;
			}
		}
		else
		{
			return true;
		}
	}
	return true;
}

bool LineHashIndex::isQualify(size_t offsetId, double xID, double yID)
{
	return (xID < MetaID(offsetId + 1) || (xID == MetaID(offsetId + 1) && yID < MetaYID(offsetId + 1))) && (xID > MetaID(offsetId) || (xID == MetaID(offsetId) && yID >= MetaYID(offsetId)));
}

void LineHashIndex::getOffsetPair(size_t offsetID, unsigned& offsetBegin, unsigned& offsetEnd)
//get the offset of the data begin and end of the offsetIDth Chunk to the startPtr
{
	if(tableSize == 0){
		offsetEnd = offsetBegin = sizeof(MetaData);
	}
	offsetBegin = chunkMeta[offsetID].offsetBegin;
	MetaData* metaData = (MetaData*) (startPtr + offsetBegin - sizeof(MetaData));
	offsetEnd = offsetBegin - sizeof(MetaData) + metaData->usedSpace;
}

size_t LineHashIndex::save(MMapBuffer*& indexBuffer)
//tablesize , (startID, lowerk , lowerb, upperk, upperb) * 4
{
	char* writeBuf;
	size_t offset;

	if (indexBuffer == NULL)
	{
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(),
										 sizeof(unsigned) + 16 * sizeof(double) + 4 * sizeof(double));
		writeBuf = indexBuffer->get_address();
		offset = 0;
	}
	else
	{
		size_t size = indexBuffer->get_length();
		indexBuffer->resize(size + sizeof(unsigned) + 16 * sizeof(double) + 4 * sizeof(double), false);
		writeBuf = indexBuffer->get_address() + size;
		offset = size;
	}

	*(unsigned *) writeBuf = tableSize;
	writeBuf += sizeof(unsigned);

	for (int i = 0; i < 4; ++i)
	{
		*(double*) writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*) writeBuf = lowerk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*) writeBuf = lowerb[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*) writeBuf = upperk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*) writeBuf = upperb[i];
		writeBuf = writeBuf + sizeof(double);
	}

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;

	return offset;
}

void LineHashIndex::updateLineIndex()
{
	char* base = LineHashIndexEnhanceBase;

	*(unsigned *) base = tableSize;
	base += sizeof(unsigned);

	for (int i = 0; i < 4; ++i)
	{
		*(double*) base = startID[i];
		base += sizeof(double);

		*(double*) base = lowerk[i];
		base += sizeof(double);
		*(double*) base = lowerb[i];
		base += sizeof(double);

		*(double*) base = upperk[i];
		base += sizeof(double);
		*(double*) base = upperb[i];
		base += sizeof(double);
	}

	delete idTable;
	idTable = NULL;
}

void LineHashIndex::updateChunkMetaData(int offsetId, unsigned chunkType)
{
	if (offsetId == 0)
	{
		const uchar* reader = NULL;
		register double x = 0, y = 0;

		reader = startPtr + chunkMeta[offsetId].offsetBegin;
		//TODO @ganpeng
		reader = Chunk::readXYId(reader,x,y,chunkType);
		chunkMeta[offsetId].minIDx = x;
		chunkMeta[offsetId].minIDy = y;
//        if (xyType == LineHashIndex::YBIGTHANX)
//        {
//            chunkMeta[offsetId].minIDx = x;
//            chunkMeta[offsetId].minIDy = x + y;
//        }
//        else if (xyType == LineHashIndex::XBIGTHANY)
//        {
//            chunkMeta[offsetId].minIDx = x + y;
//            chunkMeta[offsetId].minIDy = x;
//        }
	}
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType index_type, ObjectType xy_type, char*buffer,
												 size_t& offset)
{
	LineHashIndex* index = new LineHashIndex(manager, index_type, xy_type);
	char* base = buffer + offset;
	index->LineHashIndexEnhanceBase = base;

	index->tableSize = *((unsigned *) base);
	base = base + sizeof(unsigned);

	for (int i = 0; i < 4; ++i)
	{
		index->startID[i] = *(double*) base;
		base = base + sizeof(double);

		index->lowerk[i] = *(double*) base;
		base = base + sizeof(double);
		index->lowerb[i] = *(double*) base;
		base = base + sizeof(double);

		index->upperk[i] = *(double*) base;
		base = base + sizeof(double);
		index->upperb[i] = *(double*) base;
		base = base + sizeof(double);
	}
	offset = offset + sizeof(unsigned) + 16 * sizeof(double) + 4 * sizeof(double);

	//get something useful for the index
	const uchar* reader;
	const uchar* temp;
	register double x, y;
	unsigned OSFlag=3;
	if (index->xyType == LineHashIndex::INT)
	{
		index->startPtr = index->chunkManager.getStartPtr(0);
		index->endPtr = index->chunkManager.getEndPtr(0);
		if (index->startPtr == index->endPtr)
		{
			index->chunkMeta.push_back(
					{ 0, 0, sizeof(MetaData) });
			return index;
		}

		temp = index->startPtr + sizeof(MetaData);

		if(index_type==LineHashIndex::SUBJECT_INDEX)
			OSFlag=0;
		//TODO @ganpeng
		Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
		index->chunkMeta.push_back(
				{ x, y, sizeof(MetaData) });

		reader = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (reader < index->endPtr)
		{
			temp = reader + sizeof(MetaData);
			//TODO @ganpeng
			Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
			index->chunkMeta.push_back(
					{ x, y, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}
		reader = index->endPtr;
		//TODO @ganpeng
		reader = Chunk::skipBackward(reader, index->startPtr,OSFlag);
		//TODO @ganpeng
		Chunk::readXYId(reader,x,y,OSFlag);
		index->chunkMeta.push_back(
				{ x, y });
	}
	else if (index->xyType == LineHashIndex::FLOAT)
	{
		index->startPtr = index->chunkManager.getStartPtr(1);
		index->endPtr = index->chunkManager.getEndPtr(1);
		if (index->startPtr == index->endPtr)
		{
			index->chunkMeta.push_back(
					{ 0, 0, sizeof(MetaData) });
			return index;
		}
		if(index_type==LineHashIndex::SUBJECT_INDEX)
			OSFlag=0;
		temp = index->startPtr + sizeof(MetaData);
		//TODO @ganpeng
		Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
		index->chunkMeta.push_back(
				{  y, x, sizeof(MetaData) });

		reader = index->startPtr + MemoryBuffer::pagesize;
		while (reader < index->endPtr)
		{
			temp = reader + sizeof(MetaData);
			//TODO @ganpeng
			Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
			index->chunkMeta.push_back(
					{ y, x, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}

		reader = index->endPtr;
		//TODO @ganpeng
		reader = Chunk::skipBackward(reader, index->startPtr,OSFlag);
		//TODO @ganpeng
		Chunk::readXYId(reader,x,y,OSFlag);
		index->chunkMeta.push_back(
				{y, x });
	}else if (index->xyType == LineHashIndex::DOUBLE)
	{
		index->startPtr = index->chunkManager.getStartPtr(2);
		index->endPtr = index->chunkManager.getEndPtr(2);
		if (index->startPtr == index->endPtr)
		{
			index->chunkMeta.push_back(
					{ 0, 0, sizeof(MetaData) });
			return index;
		}
		if(index_type==LineHashIndex::SUBJECT_INDEX)
			OSFlag=0;
		temp = index->startPtr + sizeof(MetaData);
		//TODO @ganpeng
		Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
		index->chunkMeta.push_back(
				{  y, x, sizeof(MetaData) });

		reader = index->startPtr + MemoryBuffer::pagesize;
		while (reader < index->endPtr)
		{
			temp = reader + sizeof(MetaData);
			//TODO @ganpeng
			Chunk::readYId(Chunk::readXId(temp, x,OSFlag), y,OSFlag);
			index->chunkMeta.push_back(
					{ y, x, reader - index->startPtr + sizeof(MetaData) });

			reader = reader + MemoryBuffer::pagesize;
		}

		reader = index->endPtr;
		//TODO @ganpeng
		reader = Chunk::skipBackward(reader, index->startPtr,OSFlag);
		//TODO @ganpeng
		Chunk::readXYId(reader,x,y,OSFlag);
		index->chunkMeta.push_back(
				{ y, x });
	}
	return index;
}