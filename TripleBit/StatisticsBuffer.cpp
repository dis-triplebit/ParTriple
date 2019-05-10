//
// Created by youyujie on 2019/4/29.
//

#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "HashIndex.h"
#include "EntityIDBuffer.h"
#include "URITable.h"
#include "MemoryBuffer.h"

//extern char* writeData(char* writer, double data);
//extern const char* readData(const char* reader, double & data);
static char* writeData(char* writer, unsigned data)
{
    memcpy(writer, &data, sizeof(unsigned));
    return writer+sizeof(unsigned);
}

static const char* readData(const char* reader, unsigned & data){
    memcpy(&data, reader, sizeof(unsigned));
    return reader+sizeof(unsigned);
}

static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
//将数值进行还原
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }

static void writeUint32(unsigned char* target,unsigned value)
// Write a 32bit value
{
    //这是值得高八位
    target[0]=value>>24;
    //第二个8位
    target[1]=(value>>16)&0xFF;
    //第三个8位
    target[2]=(value>>8)&0xFF;
    //第四个八位,也就是最低位
    target[3]=value&0xFF;
}



static unsigned char* writeDelta0(unsigned char* buffer, unsigned value)
// Write an integer with varying size
{
    //如果传入的值大于2^24位时，才使用这个写入
    if (value >= (1 << 24)) {
        //使用32位写入
        writeUint32(buffer, value);
        //返回buffer偏移4个字节
        return buffer + 4;
    } else if (value >= (1 << 16)) { //如果值大于等于2^16小于等于2^24,就是用3个字节
        buffer[0] = value >> 16;
        buffer[1] = (value >> 8) & 0xFF;
        buffer[2] = value & 0xFF;
        return buffer + 3;
    } else if (value >= (1 << 8)) {//如果值大于等于2^8小于2^16，就是用2个字节
        buffer[0] = value >> 8;
        buffer[1] = value & 0xFF;
        return buffer + 2;
    } else if (value > 0) {//如果小于2^8，那么就用1个字节
        buffer[0] = value;
        return buffer + 1;
    } else
        return buffer;
}

static unsigned char* writeFloat(unsigned char* buffer, float value)
// Write an integer with varying size
{
    memcpy(buffer, &value, sizeof(float));
    return buffer+sizeof(float);
}


static const float readFloat(const unsigned char* buffer)
// Write an integer with varying size
{
    float value;
    memcpy(&value, buffer, sizeof(float));
    return value;
}


static unsigned char* writeDouble(unsigned char* buffer, double value)
// Write an integer with varying size
{
    memcpy(buffer, &value, sizeof(double));
    return buffer+sizeof(double);
}

static const double readDouble(const unsigned char* buffer)
// Write an integer with varying size
{
    double value;
    memcpy(&value, buffer, sizeof(double));
    return value;
}

StatisticsBuffer::StatisticsBuffer() : HEADSPACE(2) {

}

StatisticsBuffer::~StatisticsBuffer() {
}

/////////////////////////////////////////////////////////////////
//一个常量统计方法,传入的参数是路径和统计类型
OneConstantStatisticsBuffer::OneConstantStatisticsBuffer(const string path, StatisticsType type, unsigned dataType) : StatisticsBuffer(), type(type), reader(NULL), ID_HASH(50)
{
    buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    //MemoryBuffer的首地址
    writer = (unsigned char*)buffer->getBuffer();
//	reader = NULL;
    //vector<unsigned> 默认大小为2000
    index.resize(2000);
    //默认的值为0
    nextHashValue = 0;
    lastId = 0;
    usedSpace = 0;
    reader = NULL;
    if(type==StatisticsType::SUBJECT_STATIS||type==StatisticsType::OBJECT_STATIS&&dataType==0)
        //默认大小为50
        triples = new Triple[ID_HASH];
    else if(type==StatisticsType::OBJECT_STATIS&&dataType==1)
        f_triples = new Triple_f[ID_HASH];
    else if(type==StatisticsType::OBJECT_STATIS&&dataType==2)
        d_triples = new Triple_d[ID_HASH];
    first = true;
}
//析构函数
OneConstantStatisticsBuffer::~OneConstantStatisticsBuffer()
{
    if(buffer != NULL) {
        delete buffer;
    }

    if(triples != NULL) {
        delete[] triples;
        triples = NULL;
    }

    if(f_triples != NULL) {
        delete[] triples;
        triples = NULL;
    }

    if(d_triples != NULL) {
        delete[] triples;
        triples = NULL;
    }
    buffer = NULL;
}
//写入ID， write a id to buffer; isID indicate the id really is a ID, maybe is a count.
void OneConstantStatisticsBuffer::writeId(unsigned id, char*& ptr, bool isID)
{
    //如果id是ID那么进行如下的操作
    if ( isID == true ) {
        //当id大于128时，那么继续判断
        while (id >= 128) {
            //计算后7位的值
            unsigned char c = static_cast<unsigned char> (id & 127);
            //将后7位的值赋值给ptr
            *ptr = c;
            //ptr 进行累加
            ptr++;
            //摒弃后七位
            id >>= 7;
        }
        *ptr = static_cast<unsigned char> (id & 127);
        ptr++;
    } else {//如果这是一个值
        while (id >= 128) {
            // 拿到8位的值
            unsigned char c = static_cast<unsigned char> (id | 128);
            *ptr = c;
            ptr++;
            id >>= 7;
        }
        *ptr = static_cast<unsigned char> (id | 128);
        ptr++;
    }
}

//如果buffer的大小的值大于buffer的size那么就是已经满了
bool OneConstantStatisticsBuffer::isPtrFull(unsigned len)
{
    //当前的地址减去传入的首地址加上传入的个数转换为数值和buffer的大小相比
    return (unsigned int) ( writer - (unsigned char*)buffer->getBuffer() + len ) > buffer->getSize() ? true : false;
}

//计算这个值所用字节
unsigned OneConstantStatisticsBuffer::getLen(unsigned v)
{
    if (v >= (1 << 24))
        return 4;
    else if (v >= (1 << 16))
        return 3;
    else if (v >= (1 << 8))
        return 2;
    else if(v > 0)
        return 1;
    else
        return 0;
}

// 统计实体,参数为begin和end,用于统计
static unsigned int countEntity(const unsigned char* begin, const unsigned char* end,unsigned dataType)
{
    if(begin >= end)
        return 0;
    unsigned int entityCount = 0;
    //cout<<"begin - end: "<<end - begin<<endl;
    if(dataType==0)
    {

        entityCount = 1;
        //将begin进行加8个字节
        begin = begin + 8;
        //还没有遍历时，进行循环遍历
        while(begin < end) {
            // Decode the header byte
            unsigned info = *(begin++);
            // Small gap only?
            if (info < 0x80) {
                if (!info)
                    break;
                /*
                count = (info >> 4) + 1;
                value1 += (info & 15);
                (*writer).value1 = value1;
                (*writer).count = count;
                ++writer;
                */
                entityCount++ ;
                continue;
            }
            // Decode the parts
            //value1 += 1;
            switch (info & 127) {
                case 0: break;
                case 1: begin += 1; break;
                case 2: begin += 2;break;
                case 3: begin += 3;break;
                case 4: begin += 4;break;
                case 5: begin += 1;break;
                case 6: begin += 2;break;
                case 7: begin += 3;break;
                case 8: begin += 4;break;
                case 9: begin += 5; break;
                case 10: begin += 2; break;
                case 11: begin += 3; break;
                case 12: begin += 4; break;
                case 13: begin += 5; break;
                case 14: begin += 6; break;
                case 15: begin += 3; break;
                case 16: begin += 4; break;
                case 17: begin += 5; break;
                case 18: begin += 6; break;
                case 19: begin += 7;break;
                case 20: begin += 4;break;
                case 21: begin += 5;break;
                case 22: begin += 6;break;
                case 23: begin += 7;break;
                case 24: begin += 8;break;
            }
            entityCount++;
        }
    }
    else if(dataType==1)
    {
        entityCount = 1;
        //将begin进行加8个字节
        begin = begin + 8;

        //还没有遍历时，进行循环遍历
        while(begin < end) {
            readFloat(begin);
            begin += sizeof(float);
            readDelta4(begin);
            begin += sizeof(unsigned);
            entityCount++;
        }
    }
    else if(dataType==2)
    {
        entityCount = 1;
        //将begin进行加8个字节
        begin = begin + 8;

        //还没有遍历时，进行循环遍历
        while(begin < end) {
            readDouble(begin);
            begin += sizeof(double);
            readDelta4(begin);
            begin += sizeof(unsigned);
            entityCount++;
        }
    }
    return entityCount;
}

// decode a chunk
const unsigned char* OneConstantStatisticsBuffer::decode(const unsigned char* begin, const unsigned char* end, unsigned dataType)
{
    if(dataType==0)
    {
        Triple* writer = triples;
        unsigned value1, count;
        value1 = readDelta4(begin);
        begin += 4;
        count = readDelta4(begin);
        begin += 4;

        (*writer).value1 = value1;
        (*writer).count = count;
        writer++;

        while (begin < end) {
            // Decode the header byte
            unsigned info = *(begin++);
            // Small gap only?
            if (info < 0x80) {
                if (!info)
                    break;
                count = (info >> 4) + 1;
                value1 += (info & 15);
                (*writer).value1 = value1;
                (*writer).count = count;
                ++writer;
                continue;
            }
            // Decode the parts
            value1 += 1;
            switch (info & 127) {
                case 0: count = 1;break;
                case 1: count = readDelta1(begin) + 1; begin += 1; break;
                case 2: count = readDelta2(begin) + 1;begin += 2;break;
                case 3: count = readDelta3(begin) + 1;begin += 3;break;
                case 4: count = readDelta4(begin) + 1;begin += 4;break;
                case 5: value1 += readDelta1(begin);count = 1;begin += 1;break;
                case 6: value1 += readDelta1(begin);count = readDelta1(begin + 1) + 1;begin += 2;break;
                case 7: value1 += readDelta1(begin);count = readDelta2(begin + 1) + 1;begin += 3;break;
                case 8: value1 += readDelta1(begin);count = readDelta3(begin + 1) + 1;begin += 4;break;
                case 9: value1 += readDelta1(begin); count = readDelta4(begin + 1) + 1; begin += 5; break;
                case 10: value1 += readDelta2(begin); count = 1; begin += 2; break;
                case 11: value1 += readDelta2(begin); count = readDelta1(begin + 2) + 1; begin += 3; break;
                case 12: value1 += readDelta2(begin); count = readDelta2(begin + 2) + 1; begin += 4; break;
                case 13: value1 += readDelta2(begin); count = readDelta3(begin + 2) + 1; begin += 5; break;
                case 14: value1 += readDelta2(begin); count = readDelta4(begin + 2) + 1; begin += 6; break;
                case 15: value1 += readDelta3(begin); count = 1; begin += 3; break;
                case 16: value1 += readDelta3(begin); count = readDelta1(begin + 3) + 1; begin += 4; break;
                case 17: value1 += readDelta3(begin); count = readDelta2(begin + 3) + 1; begin += 5; break;
                case 18: value1 += readDelta3(begin); count = readDelta3(begin + 3) + 1; begin += 6; break;
                case 19: value1 += readDelta3(begin);count = readDelta4(begin + 3) + 1;begin += 7;break;
                case 20: value1 += readDelta4(begin);count = 1;begin += 4;break;
                case 21: value1 += readDelta4(begin);count = readDelta1(begin + 4) + 1;begin += 5;break;
                case 22: value1 += readDelta4(begin);count = readDelta2(begin + 4) + 1;begin += 6;break;
                case 23: value1 += readDelta4(begin);count = readDelta3(begin + 4) + 1;begin += 7;break;
                case 24: value1 += readDelta4(begin);count = readDelta4(begin + 4) + 1;begin += 8;break;
            }
            (*writer).value1 = value1;
            (*writer).count = count;
            ++writer;
        }

        pos = triples;
        posLimit = writer;
    }

    if(dataType==1)
    {
        Triple_f* writer = f_triples;
        float value1;
        unsigned count;
        value1 = readFloat(begin);
        begin += sizeof(float);
        count = readFloat(begin);
        begin += sizeof(float);;

        (*writer).value1 = value1;
        (*writer).count = count;
        writer++;

        while (begin < end) {
            // Decode the header byte
            value1 = readFloat(begin);
            begin += sizeof(float);
            count = readFloat(begin);
            begin += sizeof(float);

            (*writer).value1 = value1;
            (*writer).count = count;
            writer++;
        }

        f_pos = f_triples;
        f_posLimit = writer;
    }


    if(dataType==2)
    {
        Triple_d* writer = d_triples;
        double value1;
        unsigned count;
        value1 = readDouble(begin);
        begin += sizeof(double);
        count = readFloat(begin);
        begin += sizeof(float);

        (*writer).value1 = value1;
        (*writer).count = count;
        writer++;

        while (begin < end) {
            value1 = readDouble(begin);
            begin += sizeof(double);
            count = readFloat(begin);
            begin += sizeof(float);;

            (*writer).value1 = value1;
            (*writer).count = count;
            writer++;
        }

        d_pos = d_triples;
        d_posLimit = writer;
    }
    return begin;
}


//subject count或者 object count，谁调用它，就读谁的
unsigned int OneConstantStatisticsBuffer::getEntityCount(unsigned dataType)
{
    unsigned int entityCount = 0;
    unsigned i = 0;

    const unsigned char* begin, *end;
    unsigned beginChunk = 0, endChunk = 0;

#ifdef DEBUG
    cout<<"indexSize: "<<indexSize<<endl;
#endif
    for(i = 1; i <= indexSize; i++) {
        if(i < indexSize)
            endChunk = index[i];

        while(endChunk == 0 && i < indexSize) {
            i++;
            endChunk = index[i];
        }

        if(i == indexSize) {
            endChunk = usedSpace;
        }

        if(endChunk != 0) {
            begin = (const unsigned char*)(buffer->getBuffer()) + beginChunk;
            end = (const unsigned char*)(buffer->getBuffer()) + endChunk;
            entityCount = entityCount + countEntity(begin, end,dataType);

            beginChunk = endChunk;
        }

        //beginChunk = endChunk;
    }

    return entityCount;
}
/// add a statistics record，v1应该是Object或者Subject,v2表示数值
Status OneConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3 /* = 0 */)
{
//	static bool first = true;
    unsigned interVal;
    //默认的nextHashValue是0,如果这个值大于0
    if ( v1 >= nextHashValue ) {
        interVal = v1;
    } else {
        //那么增量存储差值
        interVal = v1 - lastId;
    }

    unsigned len;
    //Subject或者Object的值大于nextHashValue的值,那么将len等于8
    if(v1 >= nextHashValue) {
        //len等于8
        len = 8;
    } else if(interVal < 16 && v2 <= 8) {  //如果差值在16之内且数量在8个之内
        len = 1;
    } else {
        //为什么要都减一呢,因为前面要使用1
        len = 1 + getLen(interVal - 1) + getLen(v2 - 1);
    }

    if ( isPtrFull(len) == true ) {
        //已经使用的空间
        usedSpace = writer - (unsigned char*)buffer->getBuffer();
        buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
        //从没有数据的位置开始出发
        writer = (unsigned char*)buffer->getBuffer() + usedSpace;
    }
    //如果这是第一次写入或者v1的值大于等于nextHashValue
    if ( first || (v1 >= nextHashValue) ) {
        //那么就计算偏移量
        unsigned offset = writer - (uchar*)buffer->getBuffer();
        //如果index的大小小于等于V1/id_hash,那么就需要扩容，记住index的初始大小是2000,每次扩容增加一倍
        while (index.size() <= (v1 / ID_HASH)) {
            index.resize(index.size() + 2000, 0);
#ifdef DEBUG
            cout<<"index size"<<index.size()<<" v1 / ID_HASH: "<<(v1 / ID_HASH)<<endl;
#endif
        }
        //将在这个的偏移量，也就是在MemoryBuffer的位置，存储在vector向量中
        index[v1 / ID_HASH] = offset;
        //如果v1大于等于nextHashValue就增加200
        while(nextHashValue <= v1) nextHashValue += HASH_RANGE;
        //将这个Object或者Subject写入到buffer中
        writeUint32(writer, v1);
        writer += 4;
        //将数量写入到buffer中
        writeUint32(writer, v2);
        writer += 4;
        //将first置为false
        first = false;
    } else {//如果并且第一次写入且v1小于index的长度,那么就进行这一步
        //如果len==1，也就是差值在16之内且数量在8个之内
        if(len == 1) {
            *writer = ((v2 - 1) << 4) | (interVal);
            writer++;
        } else {
            // 1000000 | 字节长度乘以5+（v2-1）的字节数
            *writer = 0x80|((getLen(interVal-1)*5)+getLen(v2 - 1));
            writer++;
            // 将差值写入到buffer中,写入几个字节就一定移动几个字节
            writer = writeDelta0(writer,interVal - 1);
            writer = writeDelta0(writer, v2 - 1);
        }
    }
    // lastId为最后插入的Object
    lastId = v1;
    // 更新用过的空间
    usedSpace = writer - (uchar*)buffer->getBuffer();

    return OK;
}

Status OneConstantStatisticsBuffer::addStatis(float v1, unsigned v2, unsigned v3 /* = 0 */)
{
    int len=sizeof(float);
    if ( isPtrFull(len) == true ) {
        //已经使用的空间
        usedSpace = writer - (unsigned char*)buffer->getBuffer();
        buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
        //从没有数据的位置开始出发
        writer = (unsigned char*)buffer->getBuffer() + usedSpace;
    }
    //如果这是第一次写入或者v1的值大于等于nextHashValue
    if ( first || (v1 >= nextHashValue) ) {
        //那么就计算偏移量
        unsigned offset = writer - (uchar*)buffer->getBuffer();
        //如果index的大小小于等于V1/id_hash,那么就需要扩容，记住index的初始大小是2000,每次扩容增加一倍
        while (index.size() <= ((unsigned)v1 / ID_HASH)) {
            index.resize(index.size() + 2000, 0);
#ifdef DEBUG
            cout<<"index size"<<index.size()<<" v1 / ID_HASH: "<<(v1 / ID_HASH)<<endl;
#endif
        }
        //将在这个的偏移量，也就是在MemoryBuffer的位置，存储在vector向量中
        index[(unsigned)v1 / ID_HASH] = offset;
        //如果v1大于等于nextHashValue就增加200
        while(nextHashValue <= v1) nextHashValue += HASH_RANGE;
        //将这个Object或者Subject写入到buffer中
        writer=writeFloat(writer,v1);
        //将数量写入到buffer中
        writeUint32(writer, v2);
        writer += 4;
        //将first置为false
        first = false;
    } else {//如果并且第一次写入且v1小于index的长度,那么就进行这一步

        writer=writeFloat(writer,v1);
        writer=writeFloat(writer, v2);
    }
    // lastId为最后插入的Object
    //lastId = v1;
    // 更新用过的空间
    usedSpace = writer - (uchar*)buffer->getBuffer();

    return OK;
}

Status OneConstantStatisticsBuffer::addStatis(double v1, unsigned v2, unsigned v3 /* = 0 */)
{
    int len=sizeof(double);
    if ( isPtrFull(len) == true ) {
        //已经使用的空间
        usedSpace = writer - (unsigned char*)buffer->getBuffer();
        buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
        //从没有数据的位置开始出发
        writer = (unsigned char*)buffer->getBuffer() + usedSpace;
    }
    //如果这是第一次写入或者v1的值大于等于nextHashValue
    if ( first || (v1 >= nextHashValue) ) {
        //那么就计算偏移量
        unsigned offset = writer - (uchar*)buffer->getBuffer();
        //如果index的大小小于等于V1/id_hash,那么就需要扩容，记住index的初始大小是2000,每次扩容增加一倍
        while (index.size() <= ((unsigned)v1 / ID_HASH)) {
            index.resize(index.size() + 2000, 0);
#ifdef DEBUG
            cout<<"index size"<<index.size()<<" v1 / ID_HASH: "<<(v1 / ID_HASH)<<endl;
#endif
        }
        //将在这个的偏移量，也就是在MemoryBuffer的位置，存储在vector向量中
        index[(unsigned)v1 / ID_HASH] = offset;
        //如果v1大于等于nextHashValue就增加200
        while(nextHashValue <= v1) nextHashValue += HASH_RANGE;
        //将这个Object或者Subject写入到buffer中
        writer=writeDouble(writer,v1);
        //将数量写入到buffer中
        writeUint32(writer, v2);
        writer += 4;
        //将first置为false
        first = false;
    } else {//如果并且第一次写入且v1小于index的长度,那么就进行这一步

        writer=writeDouble(writer,v1);
        writer=writeFloat(writer, v2);
    }
    // lastId为最后插入的Object
    //lastId = v1;
    // 更新用过的空间
    usedSpace = writer - (uchar*)buffer->getBuffer();

    return OK;
}

//利用二分查找找到这个值
bool OneConstantStatisticsBuffer::find(unsigned value)
{
    //const Triple* l = pos, *r = posLimit;
    int l = 0, r = posLimit - pos;
    int m;
    while (l != r) {
        m = l + ((r - l) / 2);
        if (value > pos[m].value1) {
            l = m + 1;
        } else if ((!m) || value > pos[m - 1].value1) {
            break;
        } else {
            r = m;
        }
    }

    if(l == r)
        return false;
    else {
        pos = &pos[m];
        return true;
    }

}

bool OneConstantStatisticsBuffer::find(float value)
{
    //const Triple* l = pos, *r = posLimit;
    int l = 0, r = f_posLimit - f_pos;
    int m;
    while (l != r) {
        m = l + ((r - l) / 2);
        if (value > f_pos[m].value1) {
            l = m + 1;
        } else if ((!m) || value > f_pos[m - 1].value1) {
            break;
        } else {
            r = m;
        }
    }

    if(l == r)
        return false;
    else {
        f_pos = &f_pos[m];
        return true;
    }

}


bool OneConstantStatisticsBuffer::find(double value)
{
    //const Triple* l = pos, *r = posLimit;
    int l = 0, r = d_posLimit - d_pos;
    int m;
    while (l != r) {
        m = l + ((r - l) / 2);
        if (value > d_pos[m].value1) {
            l = m + 1;
        } else if ((!m) || value > d_pos[m - 1].value1) {
            break;
        } else {
            r = m;
        }
    }

    if(l == r)
        return false;
    else {
        d_pos = &d_pos[m];
        return true;
    }

}

bool OneConstantStatisticsBuffer::find_last(unsigned value)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (value < pos[middle].value1) {
            right = middle;
        } else if ((!middle) || value < pos[middle + 1].value1) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        pos = &pos[middle];
        return true;
    }
}

bool OneConstantStatisticsBuffer::find_last(float value)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = f_posLimit - f_pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (value < f_pos[middle].value1) {
            right = middle;
        } else if ((!middle) || value < f_pos[middle + 1].value1) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        f_pos = &f_pos[middle];
        return true;
    }
}

bool OneConstantStatisticsBuffer::find_last(double value)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = d_posLimit - d_pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (value < d_pos[middle].value1) {
            right = middle;
        } else if ((!middle) || value < d_pos[middle + 1].value1) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        d_pos = &d_pos[middle];
        return true;
    }
}

Status OneConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2 /* = 0 */)
{
    unsigned i;
    //通过数组来判断开始
    unsigned begin = index[ v1 / ID_HASH];
    unsigned end = 0;

    i = v1 / ID_HASH + 1;
    while(i < indexSize) { // get next chunk start offset;
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    //如果找不到的话，。那么就找到最后的地址位置
    if(i == indexSize)
        end = usedSpace;
    //计算从开始位置
    reader = (unsigned char*)buffer->getBuffer() + begin;
    //计算第一个
    lastId = readDelta4(reader);
    //将偏移加4
    reader += 4;

    //reader = readId(lastId, reader, true);
    if ( lastId == v1 ) {
        //reader = readId(v1, reader, false);
        v1 = readDelta4(reader);
        return OK;
    }

    const uchar* limit = (uchar*)buffer->getBuffer() + end;
    // 解压
    this->decode(reader - 4, limit,0);
    if(this->find(v1)) {
        if(pos->value1 == v1) {
            v1 = pos->count;
            return OK;
        }
    }

    v1 = 0;
    return ERROR;
}

//得到的返回值存储在v1中，需要将v1强转为int
Status OneConstantStatisticsBuffer::getStatis(float& v1, unsigned v2 /* = 0 */)
{
    unsigned i;
    //通过数组来判断开始
    unsigned begin = index[ (unsigned) v1 / ID_HASH];
    unsigned end = 0;

    i = ((unsigned)v1 / ID_HASH) + 1;
    while(i < indexSize) { // get next chunk start offset;
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    //如果找不到的话，。那么就找到最后的地址位置
    if(i == indexSize)
        end = usedSpace;
    //计算从开始位置
    reader = (unsigned char*)buffer->getBuffer() + begin;
    //计算第一个
    float lastId = readFloat(reader);
    //将偏移加4
    reader += sizeof(float);

    //reader = readId(lastId, reader, true);
    if ( lastId == v1 ) {
        //reader = readId(v1, reader, false);
        v1 = readDelta4(reader);
        return OK;
    }

    const uchar* limit = (uchar*)buffer->getBuffer() + end;
    // 解压
    this->decode(reader +4, limit,1);
    if(this->find(v1)) {
        if(f_pos->value1 == v1) {
            v1 = f_pos->count;
            return OK;
        }
    }

    v1 = 0;
    return ERROR;
}

//得到的返回值存储在v1中，需要将v1强转为int
Status OneConstantStatisticsBuffer::getStatis(double& v1, unsigned v2 /* = 0 */)
{
    unsigned i;
    //通过数组来判断开始
    unsigned begin = index[ (unsigned) v1 / ID_HASH];
    unsigned end = 0;

    i = ((unsigned)v1 / ID_HASH) + 1;
    while(i < indexSize) { // get next chunk start offset;
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    //如果找不到的话，。那么就找到最后的地址位置
    if(i == indexSize)
        end = usedSpace;
    //计算从开始位置
    reader = (unsigned char*)buffer->getBuffer() + begin;
    //计算第一个
    lastId = readDouble(reader);
    //将偏移加4
    reader += sizeof(double);

    //reader = readId(lastId, reader, true);
    if ( lastId == v1 ) {
        //reader = readId(v1, reader, false);
        v1 = readDelta4(reader);
        return OK;
    }

    const uchar* limit = (uchar*)buffer->getBuffer() + end;
    // 解压
    this->decode(reader + 4, limit,2);
    if(this->find(v1)) {
        if(d_pos->value1 == v1) {
            v1 = d_pos->count;
            return OK;
        }
    }

    v1 = 0;
    return ERROR;
}

//保存索引信息，type为0时表示Subject或者Object，为0表示Object为int时,为1表示Object为float时,为2表示Object为double时
Status OneConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer,StatisticsType type,unsigned dataType)
{
    string fileName="/statIndex";
//    if(type==StatisticsType::SUBJECT_STATIS)
//    {
//        fileName="/SubjectStatIndex";
//    }
//    else if(type==StatisticsType::OBJECT_STATIS&&dataType==0)
//    {
//        fileName="/ObjectIntStatIndex";
//    }
//    else if(type==StatisticsType::OBJECT_STATIS&&dataType==1)
//    {
//        fileName="/ObjectFloatStatIndex";
//    }
//    else if(type==StatisticsType::OBJECT_STATIS&&dataType==2)
//    {
//        fileName="/ObjectDoubleStatIndex";
//    }
#ifdef DEBUG
    cout<<"index size: "<<index.size()<<endl;
#endif
    char * writer;
    if(indexBuffer == NULL) {
        //由于存储的是整数那么就乘以4,为什么要加2呢?保存使用的大小和长度
        indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + fileName).c_str(), (index.size() + 2) * 4);
        writer = indexBuffer->get_address();
    } else {
        size_t size = indexBuffer->getSize();
        //扩容了偏移的大小,这需要存储的是所用的空间，索引的大小，以及索引
        indexBuffer->resize((index.size() + 2) * 4);
        writer = indexBuffer->get_address() + size;
    }
    //将使用空间写入，注意，由于之前改过，这里需要将这个方法改成写unsigned类型的
    writer = writeData(writer, usedSpace);
    cout<<"usedSpace:"<<usedSpace<<endl;
    //将索引大小写入，注意，由于之前改过，这里需要将这个方法改成写unsigned类型的
    writer = writeData(writer, index.size());
    cout<<"indexSize:"<<index.size()<<endl;
    vector<unsigned>::iterator iter, limit;

    for(iter = index.begin(), limit = index.end(); iter != limit; iter++) {
        writer = writeData(writer, *iter);
    }
    //memcpy(writer, index, indexSize * sizeof(unsigned));

    return OK;
}

OneConstantStatisticsBuffer* OneConstantStatisticsBuffer::load(StatisticsType type,const string path, char*& indexBuffer, unsigned dataType)
{
    OneConstantStatisticsBuffer* statBuffer = new OneConstantStatisticsBuffer(path, type,dataType);

    unsigned size, first;

    indexBuffer = (char*)readData(indexBuffer, (unsigned &)statBuffer->usedSpace);
    cout<<"load:usedSpace:"<<statBuffer->usedSpace<<endl;
    indexBuffer = (char*)readData(indexBuffer, (unsigned &)size);
    cout<<"load:indexsize:"<<size<<endl;
    statBuffer->index.resize(0);

    statBuffer->indexSize = size;

    //依次读取index内的值放入statBuffer数组中
    for( unsigned i = 0; i < size; i++ ) {
        indexBuffer = (char*)readData(indexBuffer, (unsigned &)first);
        statBuffer->index.push_back(first);
    }

    return statBuffer;
}
/// get the subject or object ids from minID to maxID;
Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID)
{
    unsigned i = 0, endEntry = 0;
    unsigned begin = index[ minID / HASH_RANGE], end = 0;
    reader = (uchar*)buffer->getBuffer() + begin;

    i = minID / ID_HASH;
    while(i < indexSize) {
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    if(i == indexSize)
        end = usedSpace;
    endEntry = i;

    const uchar* limit = (uchar*)buffer->getBuffer() + end;

    lastId = readDelta4(reader);
    decode(reader, limit,0);
    if ( lastId != minID ) {
        find(minID);
    }

    i = maxID / ID_HASH + 1;
    unsigned end1;
    while(index[i] == 0 && i < indexSize) {
        i++;
    }
    if(i == indexSize)
        end1 = usedSpace;
    else
        end1 = index[i];

    while(true) {
        if(end == end1) {
            Triple* temp = pos;
            if(find(maxID) == true)
                posLimit = pos + 1;
            pos = temp;
        }

        while(pos < posLimit) {
            entBuffer->insertID(pos->value1);
            pos++;
        }

        begin = end;
        if(begin == end1)
            break;

        endEntry = endEntry + 1;
        while(index[endEntry] != 0 && endEntry < indexSize) {
            endEntry++;
        }
        if(endEntry == indexSize) {
            end = usedSpace;
        } else {
            end = index[endEntry];
        }

        reader = (const unsigned char*)buffer->getBuffer() + begin;
        limit = (const unsigned char*)buffer->getBuffer() + end;
        decode(reader, limit ,0);
    }

    return OK;
}


Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, float minID, float maxID)
{
    unsigned i = 0, endEntry = 0;
    unsigned begin = index[ (unsigned)minID / HASH_RANGE], end = 0;
    reader = (uchar*)buffer->getBuffer() + begin;

    i =(unsigned) minID / ID_HASH;
    while(i < indexSize) {
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    if(i == indexSize)
        end = usedSpace;
    endEntry = i;

    const uchar* limit = (uchar*)buffer->getBuffer() + end;

    float lastId = readFloat(reader);
    decode(reader, limit,1);
    if ( lastId != minID ) {
        find(minID);
    }

    i =(unsigned) maxID / ID_HASH + 1;
    unsigned end1;
    while(index[i] == 0 && i < indexSize) {
        i++;
    }
    if(i == indexSize)
        end1 = usedSpace;
    else
        end1 = index[i];

    while(true) {
        if(end == end1) {
            Triple_f* temp = f_pos;
            if(find(maxID) == true)
                f_posLimit = f_pos + 1;
            f_pos = temp;
        }

        while(f_pos < f_posLimit) {
            entBuffer->insertID(f_pos->value1);
            f_pos++;
        }

        begin = end;
        if(begin == end1)
            break;

        endEntry = endEntry + 1;
        while(index[endEntry] != 0 && endEntry < indexSize) {
            endEntry++;
        }
        if(endEntry == indexSize) {
            end = usedSpace;
        } else {
            end = index[endEntry];
        }

        reader = (const unsigned char*)buffer->getBuffer() + begin;
        limit = (const unsigned char*)buffer->getBuffer() + end;
        decode(reader, limit,1);
    }

    return OK;
}

Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, double minID, double maxID)
{
    unsigned i = 0, endEntry = 0;
    unsigned begin = index[(unsigned) minID / HASH_RANGE], end = 0;
    reader = (uchar*)buffer->getBuffer() + begin;

    i = (unsigned)minID / ID_HASH;
    while(i < indexSize) {
        if(index[i] != 0) {
            end = index[i];
            break;
        }
        i++;
    }
    if(i == indexSize)
        end = usedSpace;
    endEntry = i;

    const uchar* limit = (uchar*)buffer->getBuffer() + end;

    double lastId = readDouble(reader);
    decode(reader, limit,2);
    if ( lastId != minID ) {
        find(minID);
    }

    i = (unsigned) maxID / ID_HASH + 1;
    unsigned end1;
    while(index[i] == 0 && i < indexSize) {
        i++;
    }
    if(i == indexSize)
        end1 = usedSpace;
    else
        end1 = index[i];

    while(true) {
        if(end == end1) {
            Triple_d* temp = d_pos;
            if(find(maxID) == true)
                d_posLimit = d_pos + 1;
            d_pos = temp;
        }

        while(d_pos < d_posLimit) {
            entBuffer->insertID(pos->value1);
            d_pos++;
        }

        begin = end;
        if(begin == end1)
            break;

        endEntry = endEntry + 1;
        while(index[endEntry] != 0 && endEntry < indexSize) {
            endEntry++;
        }
        if(endEntry == indexSize) {
            end = usedSpace;
        } else {
            end = index[endEntry];
        }

        reader = (const unsigned char*)buffer->getBuffer() + begin;
        limit = (const unsigned char*)buffer->getBuffer() + end;
        decode(reader, limit,2);
    }

    return OK;
}
//////////////////////////////////////////////////////////////////////

TwoConstantStatisticsBuffer::TwoConstantStatisticsBuffer(const string path, StatisticsType type, unsigned dataType) : StatisticsBuffer(), type(type), reader(NULL)
{
    //创建缓冲区
    buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    //index = (Triple*)malloc(MemoryBuffer::pagesize * sizeof(Triple));
    //得到缓冲区首地址
    writer = (uchar*)buffer->getBuffer();
    lastId = 0; lastPredicate = 0;
    usedSpace = 0;
    indexPos = 0;
    indexSize = 0; //MemoryBuffer::pagesize;
    index = NULL;

    first = true;
}

TwoConstantStatisticsBuffer::~TwoConstantStatisticsBuffer()
{
    if(buffer != NULL) {
        delete buffer;
    }
    buffer = NULL;
    index = NULL;
    index_f = NULL;
    index_d = NULL;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end, unsigned dataType)
{
    if(dataType==0)
    {
        unsigned value1 = readDelta4(begin); begin += 4;
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple* writer = &triples[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;

        // Decompress the remainder of the page
        while (begin < end) {
            // Decode the header byte
            unsigned info = *(begin++);
            // Small gap only?
            if (info < 0x80) {
                if (!info)
                    break;
                count = (info >> 5) + 1;
                value2 += (info & 31);
                (*writer).value1 = value1;
                (*writer).value2 = value2;
                (*writer).count = count;
                ++writer;
                continue;
            }
            // Decode the parts
            switch (info&127) {
                case 0: count=1; break;
                case 1: count=readDelta1(begin)+1; begin+=1; break;
                case 2: count=readDelta2(begin)+1; begin+=2; break;
                case 3: count=readDelta3(begin)+1; begin+=3; break;
                case 4: count=readDelta4(begin)+1; begin+=4; break;
                case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
                case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
                case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
                case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
                case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
                case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
                case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
                case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
                case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
                case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
                case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
                case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
                case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
                case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
                case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
                case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
                case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
                case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
                case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
                case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
                case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
                case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
                case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
                case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
                case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
                case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
                case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
                case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
                case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
                case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
                case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
                case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
                case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
                case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
                case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
                case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
                case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
                case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
                case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
                case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
                case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
                case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
                case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
                case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
                case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
                case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
                case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
                case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
                case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
                case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
                case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
                case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
                case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
                case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
                case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
                case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
                case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
                case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
                case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
                case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
                case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
                case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
                case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
                case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
                case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
                case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
                case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
                case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
                case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
                case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
                case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
                case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
                case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
                case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
                case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
                case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
                case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
                case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
                case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
                case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
                case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
                case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
                case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
                case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
                case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
                case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
                case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
                case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
                case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
                case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
                case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
                case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
                case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
                case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
                case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
                case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
                case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
                case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
                case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
                case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
                case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
                case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
                case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
                case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
                case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
                case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
                case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
                case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
                case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
                case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
                case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
                case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
                case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
                case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
                case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
                case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
                case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
                case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
                case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
                case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
            }
            (*writer).value1=value1;
            (*writer).value2=value2;
            (*writer).count=count;
            ++writer;
        }

        // Update the entries
        pos=triples;
        posLimit=writer;
    }
    else if(dataType==1)
    {
        float value1 = readFloat(begin); begin += sizeof(float);
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple_f* writer = &triples_f[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;
        while(begin < end)
        {
            value1 = readFloat(begin); begin += sizeof(float);
            value2 = readDelta4(begin); begin += 4;
            count = readDelta4(begin); begin += 4;
            (*writer).value1 = value1;
            (*writer).value2 = value2;
            (*writer).count = count;
            ++writer;
        }
        f_pos=triples_f;
        f_posLimit=writer;
    }
    else if(dataType==2)
    {
        double value1 = readDouble(begin); begin += sizeof(double);
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple_d* writer = &triples_d[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;
        while(begin < end)
        {
            value1 = readDouble(begin); begin += sizeof(double);
            value2 = readDelta4(begin); begin += 4;
            count = readDelta4(begin); begin += 4;
            (*writer).value1 = value1;
            (*writer).value2 = value2;
            (*writer).count = count;
            ++writer;
        }
        d_pos=triples_d;
        d_posLimit=writer;
    }
    return begin;
}

const uchar* TwoConstantStatisticsBuffer::decodeIdAndPredicate(const uchar* begin, const uchar* end, unsigned dataType)
{
    if(dataType==0)
    {
        unsigned value1 = readDelta4(begin); begin += 4;
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple* writer = &triples[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;

        // Decompress the remainder of the page
        while (begin < end) {
            // Decode the header byte
            unsigned info = *(begin++);
            // Small gap only?
            if (info < 0x80) {
                if (!info)
                    break;
                //count = (info >> 5) + 1;
                value2 += (info & 31);
                (*writer).value1 = value1;
                (*writer).value2 = value2;
                (*writer).count = count;
                ++writer;
                continue;
            }
            // Decode the parts
            switch (info&127) {
                case 0: break;
                case 1: begin+=1; break;
                case 2: begin+=2; break;
                case 3: begin+=3; break;
                case 4: begin+=4; break;
                case 5: value2 += readDelta1(begin); begin+=1; break;
                case 6: value2 += readDelta1(begin); begin+=2; break;
                case 7: value2 += readDelta1(begin); begin+=3; break;
                case 8: value2 += readDelta1(begin); begin+=4; break;
                case 9: value2 += readDelta1(begin); begin+=5; break;
                case 10: value2 += readDelta2(begin); begin+=2; break;
                case 11: value2 += readDelta2(begin); begin+=3; break;
                case 12: value2 += readDelta2(begin); begin+=4; break;
                case 13: value2 += readDelta2(begin); begin+=5; break;
                case 14: value2 += readDelta2(begin); begin+=6; break;
                case 15: value2 += readDelta3(begin); begin+=3; break;
                case 16: value2 += readDelta3(begin); begin+=4; break;
                case 17: value2 += readDelta3(begin); begin+=5; break;
                case 18: value2 += readDelta3(begin); begin+=6; break;
                case 19: value2 += readDelta3(begin); begin+=7; break;
                case 20: value2 += readDelta4(begin); begin+=4; break;
                case 21: value2 += readDelta4(begin); begin+=5; break;
                case 22: value2 += readDelta4(begin); begin+=6; break;
                case 23: value2 += readDelta4(begin); begin+=7; break;
                case 24: value2 += readDelta4(begin); begin+=8; break;
                case 25: value1 += readDelta1(begin); value2=0; begin+=1; break;
                case 26: value1 += readDelta1(begin); value2=0; begin+=2; break;
                case 27: value1 += readDelta1(begin); value2=0; begin+=3; break;
                case 28: value1 += readDelta1(begin); value2=0; begin+=4; break;
                case 29: value1 += readDelta1(begin); value2=0; begin+=5; break;
                case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=2; break;
                case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=3; break;
                case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=4; break;
                case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=5; break;
                case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=6; break;
                case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=3; break;
                case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=4; break;
                case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=5; break;
                case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=6; break;
                case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=7; break;
                case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=4; break;
                case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=5; break;
                case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=6; break;
                case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=7; break;
                case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=8; break;
                case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=5; break;
                case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=6; break;
                case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=7; break;
                case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=8; break;
                case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=9; break;
                case 50: value1+=readDelta2(begin); value2=0; begin+=2; break;
                case 51: value1+=readDelta2(begin); value2=0; begin+=3; break;
                case 52: value1+=readDelta2(begin); value2=0; begin+=4; break;
                case 53: value1+=readDelta2(begin); value2=0; begin+=5; break;
                case 54: value1+=readDelta2(begin); value2=0; begin+=6; break;
                case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=3; break;
                case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=4; break;
                case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=5; break;
                case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=6; break;
                case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=7; break;
                case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=4; break;
                case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=5; break;
                case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=6; break;
                case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=7; break;
                case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=8; break;
                case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=5; break;
                case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=6; break;
                case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=7; break;
                case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=8; break;
                case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=9; break;
                case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=6; break;
                case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=7; break;
                case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=8; break;
                case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=9; break;
                case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=10; break;
                case 75: value1+=readDelta3(begin); value2=0; begin+=3; break;
                case 76: value1+=readDelta3(begin); value2=0; begin+=4; break;
                case 77: value1+=readDelta3(begin); value2=0; begin+=5; break;
                case 78: value1+=readDelta3(begin); value2=0; begin+=6; break;
                case 79: value1+=readDelta3(begin); value2=0; begin+=7; break;
                case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=4; break;
                case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=5; break;
                case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=6; break;
                case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=7; break;
                case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=8; break;
                case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=5; break;
                case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=6; break;
                case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=7; break;
                case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=8; break;
                case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=9; break;
                case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=6; break;
                case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=7; break;
                case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=8; break;
                case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=9; break;
                case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=10; break;
                case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=7; break;
                case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=8; break;
                case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=9; break;
                case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=10; break;
                case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=11; break;
                case 100: value1+=readDelta4(begin); value2=0; begin+=4; break;
                case 101: value1+=readDelta4(begin); value2=0; begin+=5; break;
                case 102: value1+=readDelta4(begin); value2=0; begin+=6; break;
                case 103: value1+=readDelta4(begin); value2=0; begin+=7; break;
                case 104: value1+=readDelta4(begin); value2=0; begin+=8; break;
                case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=5; break;
                case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=6; break;
                case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=7; break;
                case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=8; break;
                case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=9; break;
                case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=6; break;
                case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=7; break;
                case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=8; break;
                case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=9; break;
                case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=10; break;
                case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=7; break;
                case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=8; break;
                case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=9; break;
                case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=10; break;
                case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=11; break;
                case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=8; break;
                case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=9; break;
                case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=10; break;
                case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=11; break;
                case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=12; break;
            }
            (*writer).value1=value1;
            (*writer).value2=value2;
            (*writer).count=count;
            ++writer;
        }

        // Update the entries
        pos=triples;
        posLimit=writer;
    }
    else if(dataType==1)
    {
        float value1 = readFloat(begin); begin += sizeof(float);
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple_f* writer = &triples_f[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;

        // Decompress the remainder of the page
        while (begin < end) {
            value1 = readFloat(begin); begin += sizeof(float);
            value2 = readDelta4(begin); begin += 4;
            count = readDelta4(begin); begin += 4;
            (*writer).value1=value1;
            (*writer).value2=value2;
            (*writer).count=count;
            ++writer;
        }

        // Update the entries
        f_pos=triples_f;
        f_posLimit=writer;
    }
    else if(dataType==2)
    {
        double value1 = readDouble(begin); begin += sizeof(double);
        unsigned value2 = readDelta4(begin); begin += 4;
        unsigned count = readDelta4(begin); begin += 4;
        Triple_d* writer = &triples_d[0];
        (*writer).value1 = value1;
        (*writer).value2 = value2;
        (*writer).count = count;
        ++writer;

        // Decompress the remainder of the page
        while (begin < end) {
            value1 = readFloat(begin); begin += sizeof(double);
            value2 = readDelta4(begin); begin += 4;
            count = readDelta4(begin); begin += 4;
            (*writer).value1=value1;
            (*writer).value2=value2;
            (*writer).count=count;
            ++writer;
        }

        // Update the entries
        d_pos=triples_d;
        d_posLimit=writer;
    }
    return begin;
}

static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
    return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool greater(float a1,unsigned a2,float b1,unsigned b2) {
    return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool greater(double a1,unsigned a2,double b1,unsigned b2) {
    return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool less(unsigned a1, unsigned a2, unsigned b1, unsigned b2) {
    return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}

static inline bool less(float a1, unsigned a2, float b1, unsigned b2) {
    return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}

static inline bool less(double a1, unsigned a2, double b1, unsigned b2) {
    return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}
/*
 * find the first entry >= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find(unsigned value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
    int middle;

    while (left != right) {
        middle = left + ((right - left) / 2);
        if (::greater(value1, value2, pos[middle].value1, pos[middle].value2)) {
            left = middle + 1;
        } else if ((!middle) || ::greater(value1, value2, pos[middle - 1].value1, pos[middle -1].value2)) {
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        return false;
    } else {
        pos = &pos[middle];
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find(float value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = f_posLimit - f_pos;
    int middle;

    while (left != right) {
        middle = left + ((right - left) / 2);
        if (::greater(value1, value2, f_pos[middle].value1, f_pos[middle].value2)) {
            left = middle + 1;
        } else if ((!middle) || ::greater(value1, value2, f_pos[middle - 1].value1,f_pos[middle -1].value2)) {
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        if(right>0&&f_pos[right]==0){
            f_pos = &f_pos[right-1];
        }else
            f_pos = &f_pos[right];
        return false;
    } else {
        f_pos = &f_pos[middle];
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find(double value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = d_posLimit - d_pos;
    int middle;

    while (left != right) {
        middle = left + ((right - left) / 2);
        if (::greater(value1, value2, d_pos[middle].value1, d_pos[middle].value2)) {
            left = middle + 1;
        } else if ((!middle) || ::greater(value1, value2, d_pos[middle - 1].value1, d_pos[middle -1].value2)) {
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        if(right>0&&d_pos[right]==0){
            d_pos = &d_pos[right-1];
        }else
            d_pos = &d_pos[right];
        return false;
    } else {
        d_pos = &d_pos[middle];
        return true;
    }
}
/*
 * find the last entry <= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find_last(unsigned value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (::less(value1, value2, pos[middle].value1, pos[middle].value2)) {
            right = middle;
        } else if ((!middle) || ::less(value1, value2, pos[middle + 1].value1, pos[middle + 1].value2)) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        pos = &pos[middle];
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find_last(float value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = f_posLimit - f_pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (::less(value1, value2, f_pos[middle].value1, f_pos[middle].value2)) {
            right = middle;
        } else if ((!middle) || ::less(value1, value2, f_pos[middle + 1].value1, f_pos[middle + 1].value2)) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        f_pos = &f_pos[middle];
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find_last(double value1, unsigned value2)
{
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = d_posLimit - d_pos;
    int middle = 0;

    while (left < right) {
        middle = left + ((right - left) / 2);
        if (::less(value1, value2, d_pos[middle].value1, d_pos[middle].value2)) {
            right = middle;
        } else if ((!middle) || ::less(value1, value2, d_pos[middle + 1].value1, d_pos[middle + 1].value2)) {
            break;
        } else {
            left = middle + 1;
        }
    }

    if(left == right) {
        return false;
    } else {
        d_pos = &d_pos[middle];
        return true;
    }
}

int TwoConstantStatisticsBuffer::findPredicate(unsigned value1,Triple*pos,Triple* posLimit){
    int low = 0, high= posLimit - pos,mid;
    while (low <= high) { //当前查找区间R[low..high]非空
        mid = low + ((high - low)/2);
        if (pos[mid].value1 == value1)
            return mid; //查找成功返回
        if (pos[mid].value1 > value1)
            high = mid - 1; //继续在R[low..mid-1]中查扄1�7
        else
            low = mid + 1; //继续在R[mid+1..high]中查扄1�7
    }
    return -1; //当low>high时表示查找区间为空，查找失败

}

int TwoConstantStatisticsBuffer::findPredicate(float value1,Triple_f* pos,Triple_f* posLimit){
    int low = 0, high= posLimit - pos,mid;
    while (low <= high) { //当前查找区间R[low..high]非空
        mid = low + ((high - low)/2);
        if (pos[mid].value1 == value1)
            return mid; //查找成功返回
        if (pos[mid].value1 > value1)
            high = mid - 1; //继续在R[low..mid-1]中查扄1�7
        else
            low = mid + 1; //继续在R[mid+1..high]中查扄1�7
    }
    return -1; //当low>high时表示查找区间为空，查找失败

}

int TwoConstantStatisticsBuffer::findPredicate(double value1,Triple_d *pos,Triple_d* posLimit){
    int low = 0, high= posLimit - pos,mid;
    while (low <= high) { //当前查找区间R[low..high]非空
        mid = low + ((high - low)/2);
        if (pos[mid].value1 == value1)
            return mid; //查找成功返回
        if (pos[mid].value1 > value1)
            high = mid - 1; //继续在R[low..mid-1]中查扄1�7
        else
            low = mid + 1; //继续在R[mid+1..high]中查扄1�7
    }
    return -1; //当low>high时表示查找区间为空，查找失败

}

Status TwoConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2)
{
    pos = index, posLimit = index + indexPos;
    find(v1, v2);
    if(::greater(pos->value1, pos->value2, v1, v2))
        pos--;

    unsigned start = pos->count; pos++;
    unsigned end = pos->count;
    if(pos == (index + indexPos))
        end = usedSpace;

    const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
    decode(begin, limit,0);
    find(v1, v2);
    if(pos->value1 == v1 && pos->value2 == v2) {
        v1 = pos->count;
        return OK;
    }

    v1 = 0;
    return NOT_FOUND;
}

Status TwoConstantStatisticsBuffer::getStatis(float& v1, unsigned v2)
{
    f_pos = index_f, f_posLimit = index_f + indexPos;
    find(v1, v2);
    if(::greater(f_pos->value1, f_pos->value2, v1, v2))
        f_pos--;

   unsigned start = f_pos->count; f_pos++;
    unsigned end = f_pos->count;
    if(f_pos == (index_f + indexPos))
        end = usedSpace;

    const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
    decode(begin, limit,1);
    find(v1, v2);
    if(f_pos->value1 == v1 && f_pos->value2 == v2) {
        v1 = f_pos->count;
        return OK;
    }

    v1 = 0;
    return NOT_FOUND;
}

Status TwoConstantStatisticsBuffer::getStatis(double& v1, unsigned v2)
{
    d_pos = index_d, d_posLimit = index_d + indexPos;
    find(v1, v2);
    if(::greater(d_pos->value1, d_pos->value2, v1, v2))
        d_pos--;

    unsigned start = d_pos->count; d_pos++;
    unsigned end = d_pos->count;
    if(d_pos == (index_d + indexPos))
        end = usedSpace;

    const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
    decode(begin, limit,2);
    find(v1, v2);
    if(d_pos->value1 == v1 && d_pos->value2 == v2) {
        v1 = d_pos->count;
        return OK;
    }

    v1 = 0;
    return NOT_FOUND;
}
//lastSubject, lastPredicate, count1 或者   lastObject, lastPredicate, count1
Status TwoConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3)
{
//	static bool first = true;
    unsigned len = 0;

    // otherwise store the compressed value
    //当v1等于lastId并且v2与v1只差小于32并且v3小于5
    if ( v1 == lastId && (v2 - lastPredicate) < 32 && v3 < 5) {
        len = 1;
    } else if(v1 == lastId) {  //当v1等于lastId
        len = 1 + getLen(v2 - lastPredicate) + getLen(v3 - 1);
    } else {
        len = 1+ getLen(v1-lastId)+ getLen(v2)+getLen(v3 - 1);
    }
    //如果这是第一次插入或者加入的长度大于分配的缓冲长度
    if ( first == true || ( usedSpace + len ) > buffer->getSize() ) {
        //已经使用的空间
        usedSpace = writer - (uchar*)buffer->getBuffer();
        //扩容
        buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
        //重新确定可写入的位置
        writer = (uchar*)buffer->getBuffer() + usedSpace;
        //将v1,v2,v3写入到buffer中
        writeUint32(writer, v1); writer += 4;
        writeUint32(writer, v2); writer += 4;
        writeUint32(writer, v3); writer += 4;
        //如果indexPos再偏移一步大于indexSize
        if((indexPos + 1) >= indexSize) {
#ifdef DEBUF
            cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
            //重新分配
            index = (Triple*)realloc(index, indexSize * sizeof(Triple) + MemoryBuffer::pagesize * sizeof(Triple));
            //重新确定大小
            indexSize += MemoryBuffer::pagesize;
        }
        //确定subject或者object
        index[indexPos].value1 = v1;
        //确定谓语
        index[indexPos].value2 = v2;
        //记录偏移
        index[indexPos].count = usedSpace; //record offset
        //位置加1
        indexPos++;
        //将first置为false
        first = false;
    } else {
        if (v1 == lastId && v2 - lastPredicate < 32 && v3 < 5) {
            *writer++ = ((v3 - 1) << 5) | (v2 - lastPredicate);
        } else if (v1 == lastId) {
            *writer++ = 0x80 | (getLen(v1 - lastId) * 25 + getLen(v2 - lastPredicate) * 5 + getLen(v3 - 1));
            writer = writeDelta0(writer, v2 - lastPredicate);
            writer = writeDelta0(writer, v3 - 1);
        } else {
            *writer++ = 0x80 | (getLen(v1 - lastId) * 25 + getLen(v2) * 5 + getLen(v3 - 1));
            writer = writeDelta0(writer, v1 - lastId);
            writer = writeDelta0(writer, v2);
            writer = writeDelta0(writer, v3 - 1);
        }
    }

    lastId = v1; lastPredicate = v2;

    usedSpace = writer - (uchar*)buffer->getBuffer();
    return OK;
}

Status TwoConstantStatisticsBuffer::addStatis(float v1, unsigned v2, unsigned v3)
{
//	static bool first = true;
    unsigned len = 0;

    len=sizeof(float)+ sizeof(unsigned)*2;
    //如果这是第一次插入或者加入的长度大于分配的缓冲长度
    if ( first == true || ( usedSpace + len ) > buffer->getSize() ) {
        if(( usedSpace + len ) > buffer->getSize()) {
            //已经使用的空间
            usedSpace = writer - (uchar *) buffer->getBuffer();
            //扩容
            buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
            //重新确定可写入的位置
            writer = (uchar *) buffer->getBuffer() + usedSpace;
        }
        //将v1,v2,v3写入到buffer中
        writer=writeFloat(writer, v1);
       //cout<<"Float"<<readFloat(writer- sizeof(float))<<endl;
        writeUint32(writer, v2); writer += 4;
        //cout<<"v2:"<<readDelta4(writer-4)<<endl;
        writeUint32(writer, v3); writer += 4;
        //cout<<"v3:"<<readDelta4(writer-4)<<endl;
        //如果indexPos再偏移一步大于indexSize
        if((indexPos + 1) >= indexSize) {
#ifdef DEBUF
            cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
            //重新分配
            index_f = (Triple_f*)realloc(index, indexSize * sizeof(Triple_f) + MemoryBuffer::pagesize * sizeof(Triple_f));
            //重新确定大小
            indexSize += MemoryBuffer::pagesize;
        }
        index_f[indexPos].value1 = v1;
        //确定谓语
        index_f[indexPos].value2 = v2;
        //记录偏移
        index_f[indexPos].count = usedSpace; //record offset
        //位置加1
        indexPos++;
        //将first置为false
        first = false;
    } else{
        //将v1,v2,v3写入到buffer中
        writer=writeFloat(writer, v1);
        writeUint32(writer, v2); writer += 4;
        writeUint32(writer, v3); writer += 4;
    }

    //确定subject或者object


    usedSpace = writer - (uchar*)buffer->getBuffer();
    return OK;
}

Status TwoConstantStatisticsBuffer::addStatis(double v1, unsigned v2, unsigned v3)
{
    unsigned len = 0;

    len=sizeof(double)+ sizeof(unsigned)*2;
    //如果这是第一次插入或者加入的长度大于分配的缓冲长度
    if ( first == true || ( usedSpace + len ) > buffer->getSize() ) {
        //已经使用的空间
        usedSpace = writer - (uchar*)buffer->getBuffer();
        //扩容
        buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
        //重新确定可写入的位置
        writer = (uchar*)buffer->getBuffer() + usedSpace;
        //将v1,v2,v3写入到buffer中
        writer=writeDouble(writer, v1); //writer += sizeof(double);
        writeUint32(writer, v2); writer += 4;
        writeUint32(writer, v3); writer += 4;
        //如果indexPos再偏移一步大于indexSize
        if((indexPos + 1) >= indexSize) {
#ifdef DEBUF
            cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
            //重新分配
            index_d = (Triple_d*)realloc(index, indexSize * sizeof(Triple_d) + MemoryBuffer::pagesize * sizeof(Triple_d));
            //重新确定大小
            indexSize += MemoryBuffer::pagesize;
        }
        //确定subject或者object
        index_d[indexPos].value1 = v1;
        //确定谓语
        index_d[indexPos].value2 = v2;
        //记录偏移
        index_d[indexPos].count = usedSpace; //record offset
        //位置加1
        indexPos++;
        //将first置为false
        first = false;
    } else{
        writer=writeDouble(writer, v1); //writer += sizeof(double);
        writeUint32(writer, v2); writer += 4;
        writeUint32(writer, v3); writer += 4;
    }



    usedSpace = writer - (uchar*)buffer->getBuffer();
    return OK;
}

Status TwoConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer,StatisticsType type, unsigned dataType)
{
    //string fileName;

    char* writer;
    if(indexBuffer == NULL) {
        if(type==StatisticsType::SUBJECTPREDICATE_STATIS)
        {
            indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
        }
        else if(type==StatisticsType::OBJECTPREDICATE_STATIS)
        {
            if(dataType==0)
            {
                indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
            }
            else if(dataType==1)
            {
                indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple_f) + 2 * sizeof(unsigned));
            }
            else if(dataType==2)
            {
                indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple_d) + 2 * sizeof(unsigned));
            }
        }
        

        writer = indexBuffer->get_address();
    } else {
        size_t size = indexBuffer->getSize();
        if(dataType==0)
        {
            indexBuffer->resize(indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
        }
        else if(dataType==1)
        {
            indexBuffer->resize(indexPos * sizeof(Triple_f) + 2 * sizeof(unsigned));
        }
        else if(dataType==2)
        {
            indexBuffer->resize(indexPos * sizeof(Triple_d) + 2 * sizeof(unsigned));
        }
        writer = indexBuffer->get_address() + size;
    }

    cout<<"usedSpace"<<usedSpace<<endl;
    writer = writeData(writer, usedSpace);
    writer = writeData(writer, indexPos);
    if(dataType==0)
        memcpy(writer, (char*)index, indexPos * sizeof(Triple));
    else if(dataType==1)
        memcpy(writer, (char*)index_f, indexPos * sizeof(Triple_f));
    else if(dataType==2)
        memcpy(writer, (char*)index_d, indexPos * sizeof(Triple_d));
#ifdef DEBUG
    if(dataType==0){
         for(int i = 0; i < 3; i++)
	    {
		    cout<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl;
	    }
    }
    else if(dataType==1){
        for(int i = 0; i < 3; i++)
	    {
		    cout<<index_f[i].value1<<" : "<<index_f[i].value2<<" : "<<index_d[i].count<<endl;
	    }
    }
    else if(dataType==2){
        for(int i = 0; i < 3; i++)
	    {
		    cout<<index_d[i].value1<<" : "<<index_d[i].value2<<" : "<<index_d[i].count<<endl;
	    }
    }
	cout<<"indexPos: "<<indexPos<<endl;
#endif
    if(dataType==0)
        free(index);
    else if(dataType==1)
        free(index_f);
    else if(dataType==2)
        free(index_d);
    return OK;
}
//dataType为0时表示int，为1表示float,为2表示double
TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::load(StatisticsType type, const string path, char*& indexBuffer, unsigned dataType)
{
    TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(path, type,dataType);

    indexBuffer = (char*)readData(indexBuffer, (unsigned &)statBuffer->usedSpace);
    cout<<"usedSpace:"<<statBuffer->usedSpace<<endl;
    indexBuffer = (char*)readData(indexBuffer, (unsigned &)statBuffer->indexPos);
    cout<<"indexPos:"<<statBuffer->indexPos<<endl;
#ifdef DEBUG
    cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
    // load index;
    if(dataType==0)
    {
        statBuffer->index = (Triple*)indexBuffer;
        //
        indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);
    }
    else if(dataType==1)
    {
        statBuffer->index_f = (Triple_f*)indexBuffer;
        //
        indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple_f);
    }
    else if(dataType==2)
    {
        statBuffer->index_d = (Triple_d*)indexBuffer;
        indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple_d);
    }
#ifdef DEBUG
    if(dataType==0){
         for(int i = 0; i < 3; i++)
	    {
		    cout<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl;
	    }
    }
    else if(dataType==1){
        for(int i = 0; i < 3; i++)
	    {
		    cout<<index_f[i].value1<<" : "<<index_f[i].value2<<" : "<<index_d[i].count<<endl;
	    }
    }
    else if(dataType==2){
        for(int i = 0; i < 3; i++)
	    {
		    cout<<index_d[i].value1<<" : "<<index_d[i].value2<<" : "<<index_d[i].count<<endl;
	    }
    }
#endif

    return statBuffer;
}

unsigned TwoConstantStatisticsBuffer::getLen(unsigned v)
{
    if (v>=(1<<24))
        return 4;
    else if (v>=(1<<16))
        return 3;
    else if (v>=(1<<8))
        return 2;
    else if(v > 0)
        return 1;
    else
        return 0;
}

Status TwoConstantStatisticsBuffer::getPredicatesByID(unsigned id,EntityIDBuffer* entBuffer, ID minID, ID maxID) {
    Triple* pos, *posLimit;
    pos = index;
    posLimit = index + indexPos;
    find(id, pos, posLimit);
    //cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
    assert(pos >= index && pos < posLimit);
    Triple* startChunk = pos;
    Triple* endChunk = pos;
    while (startChunk->value1 > id && startChunk >= index) {
        startChunk--;
    }
    while (endChunk->value1 <= id && endChunk < posLimit) {
        endChunk++;
    }

    const unsigned char* begin, *limit;
    Triple* chunkIter = startChunk;

    while (chunkIter < endChunk) {
        //		cout << "------------------------------------------------" << endl;
        begin = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
        chunkIter++;
        if (chunkIter == index + indexPos)
            limit = (uchar*) buffer->get_address() + usedSpace;
        else
            limit = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

        Triple* triples = new Triple[3 * MemoryBuffer::pagesize];
        decode(begin, limit, triples, pos, posLimit);

        int mid = findPredicate(id, pos, posLimit), loc = mid;
        //		cout << mid << "  " << loc << endl;


        if (loc == -1)
            continue;
        entBuffer->insertID(pos[loc].value2);
        //	cout << "result:" << pos[loc].value2<< endl;
        while (pos[--loc].value1 == id && loc >= 0) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        loc = mid;
        while (pos[++loc].value1 == id && loc < posLimit - pos) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        delete triples;
    }

    //	entBuffer->print();
    return OK;
}

Status TwoConstantStatisticsBuffer::getPredicatesByID(float id,EntityIDBuffer* entBuffer, float minID, float maxID) {
    Triple_f* pos, *posLimit;
    pos = index_f;
    posLimit = index_f + indexPos;
    find(id, pos, posLimit);
    //cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
    assert(pos >= index_f && pos < posLimit);
    Triple_f* startChunk = pos;
    Triple_f* endChunk = pos;
    while (startChunk->value1 > id && startChunk >= index_f) {
        startChunk--;
    }
    while (endChunk->value1 <= id && endChunk < posLimit) {
        endChunk++;
    }

    const unsigned char* begin, *limit;
    Triple_f* chunkIter = startChunk;

    while (chunkIter < endChunk) {
        //		cout << "------------------------------------------------" << endl;
        begin = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
        chunkIter++;
        if (chunkIter == index_f + indexPos)
            limit = (uchar*) buffer->get_address() + usedSpace;
        else
            limit = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

        Triple_f* triples = new Triple_f[3 * MemoryBuffer::pagesize];
        decode(begin, limit, triples, pos, posLimit);

        int mid = findPredicate(id, pos, posLimit), loc = mid;
        //		cout << mid << "  " << loc << endl;


        if (loc == -1)
            continue;
        entBuffer->insertID(pos[loc].value2);
        //	cout << "result:" << pos[loc].value2<< endl;
        while (pos[--loc].value1 == id && loc >= 0) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        loc = mid;
        while (pos[++loc].value1 == id && loc < posLimit - pos) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        delete triples;
    }

    //	entBuffer->print();
    return OK;
}

Status TwoConstantStatisticsBuffer::getPredicatesByID(double id,EntityIDBuffer* entBuffer, double minID, double maxID) {
    Triple_d* pos, *posLimit;
    pos = index_d;
    posLimit = index_d + indexPos;
    find(id, pos, posLimit);
    //cout << "findchunk:" << pos->value1 << "  " << pos->value2 << endl;
    assert(pos >= index_d && pos < posLimit);
    Triple_d* startChunk = pos;
    Triple_d* endChunk = pos;
    while (startChunk->value1 > id && startChunk >= index_d) {
        startChunk--;
    }
    while (endChunk->value1 <= id && endChunk < posLimit) {
        endChunk++;
    }

    const unsigned char* begin, *limit;
    Triple_d* chunkIter = startChunk;

    while (chunkIter < endChunk) {
        //		cout << "------------------------------------------------" << endl;
        begin = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("1: %x  %x  %u\n",begin, buffer->get_address() ,chunkIter->count);
        chunkIter++;
        if (chunkIter == index_d + indexPos)
            limit = (uchar*) buffer->get_address() + usedSpace;
        else
            limit = (uchar*) buffer->get_address() + chunkIter->count;
        //		printf("2: %x  %x  %u\n",limit, buffer->get_address() ,chunkIter->count);

        Triple_d* triples = new Triple_d[3 * MemoryBuffer::pagesize];
        decode(begin, limit, triples, pos, posLimit);

        int mid = findPredicate(id, pos, posLimit), loc = mid;
        //		cout << mid << "  " << loc << endl;


        if (loc == -1)
            continue;
        entBuffer->insertID(pos[loc].value2);
        //	cout << "result:" << pos[loc].value2<< endl;
        while (pos[--loc].value1 == id && loc >= 0) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        loc = mid;
        while (pos[++loc].value1 == id && loc < posLimit - pos) {
            entBuffer->insertID(pos[loc].value2);
            //			cout << "result:" << pos[loc].value2<< endl;
        }
        delete triples;
    }

    //	entBuffer->print();
    return OK;
}

bool TwoConstantStatisticsBuffer::find(unsigned value1,Triple*& pos,Triple*& posLimit)
{//find by the value1
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
    int middle=0;

    while (left < right) {
        middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
        if (value1 > pos[middle].value1) {
            left = middle +1;
        } else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        pos = &pos[middle];
        return false;
    } else {
        pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find(float value1,Triple_f*& pos,Triple_f*& posLimit)
{//find by the value1
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
    int middle=0;

    while (left < right) {
        middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
        if (value1 > pos[middle].value1) {
            left = middle +1;
        } else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        pos = &pos[middle];
        return false;
    } else {
        pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
        return true;
    }
}

bool TwoConstantStatisticsBuffer::find(double value1,Triple_d*& pos,Triple_d*& posLimit)
{//find by the value1
    //const Triple* l = pos, *r = posLimit;
    int left = 0, right = posLimit - pos;
//	cout << "right:" << right << endl;
    int middle=0;

    while (left < right) {
        middle = left + ((right - left) / 2);
//		cout << "first:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
        if (value1 > pos[middle].value1) {
            left = middle +1;
        } else if ((!middle) || value1 > pos[middle - 1].value1) {
//			cout << "break1:" << pos[middle].value1 << "  " << value1 << "  "<< pos[middle - 1].value1 << endl;
            break;
        } else {
            right = middle;
        }
    }

    if(left == right) {
        pos = &pos[middle];
        return false;
    } else {
        pos = &pos[middle];
//		cout << "pos[middle]:" << pos[middle].value1 << "  " << pos[middle].value2 << endl;
        return true;
    }
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple*triples,Triple* &pos,Triple* &posLimit)
{
//	printf("decode   %x  %x\n",begin,end);
    unsigned value1 = readDelta4(begin); begin += 4;
    unsigned value2 = readDelta4(begin); begin += 4;
    unsigned count = readDelta4(begin); begin += 4;
    Triple* writer = &triples[0];
    (*writer).value1 = value1;
    (*writer).value2 = value2;
    (*writer).count = count;
//	cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
    ++writer;

    // Decompress the remainder of the page
    while (begin < end) {
        // Decode the header byte
        unsigned info = *(begin++);
        // Small gap only?
        if (info < 0x80) {
            if (!info){
                break;
            }
            count = (info >> 5) + 1;
            value2 += (info & 31);
            (*writer).value1 = value1;
            (*writer).value2 = value2;
            (*writer).count = count;
//			cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
            ++writer;
            continue;
        }
        // Decode the parts
        switch (info&127) {
            case 0: count=1; break;
            case 1: count=readDelta1(begin)+1; begin+=1; break;
            case 2: count=readDelta2(begin)+1; begin+=2; break;
            case 3: count=readDelta3(begin)+1; begin+=3; break;
            case 4: count=readDelta4(begin)+1; begin+=4; break;
            case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
            case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
            case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
            case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
            case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
            case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
            case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
            case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
            case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
            case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
            case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
            case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
            case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
            case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
            case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
            case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
            case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
            case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
            case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
            case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
            case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
            case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
            case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
            case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
            case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
            case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
            case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
            case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
            case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
            case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
            case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
            case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
            case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
            case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
            case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
            case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
            case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
            case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
            case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
            case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
            case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
            case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
            case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
            case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
            case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
            case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
            case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
            case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
            case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
            case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
            case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
            case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
            case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
            case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
            case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
            case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
            case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
            case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
            case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
            case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
            case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
            case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
            case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
            case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
            case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
            case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
            case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
            case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
            case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
            case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
            case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
            case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
            case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
            case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
            case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
            case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
            case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
            case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
            case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
            case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
            case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
            case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
            case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
            case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
            case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
            case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
            case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
            case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
            case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
            case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
            case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
            case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
            case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
            case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
            case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
            case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
            case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
            case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
            case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
            case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
            case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
            case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
            case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
            case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
            case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
            case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
            case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
            case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
            case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
            case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
            case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
            case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
            case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
            case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
            case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
            case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
            case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
            case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
            case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
            case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
        }
        (*writer).value1=value1;
        (*writer).value2=value2;
        (*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
        ++writer;
    }

    // Update the entries
    pos=triples;
    posLimit=writer;

    return begin;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple_f *triples_f,Triple_f* &f_pos,Triple_f* &f_posLimit)
{
//	printf("decode   %x  %x\n",begin,end);
    float value1 = readFloat(begin); begin += sizeof(float);
    unsigned value2 = readDelta4(begin); begin += 4;
    unsigned count = readDelta4(begin); begin += 4;
    Triple_f* writer = &triples_f[0];
    (*writer).value1 = value1;
    (*writer).value2 = value2;
    (*writer).count = count;
//	cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
    ++writer;

    // Decompress the remainder of the page
    while (begin < end) {
        value1 = readFloat(begin); begin += sizeof(float);
        value2 = readDelta4(begin); begin += 4;
        count = readDelta4(begin); begin += 4;
        Triple_f* writer = &triples_f[0];
        (*writer).value1=value1;
        (*writer).value2=value2;
        (*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
        ++writer;
    }

    // Update the entries
    f_pos=triples_f;
    f_posLimit=writer;

    return begin;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple_d *triples_d,Triple_d* &d_pos,Triple_d* &d_posLimit)
{
//	printf("decode   %x  %x\n",begin,end);
    double value1 = readDouble(begin); begin += sizeof(double);
    unsigned value2 = readDelta4(begin); begin += 4;
    unsigned count = readDelta4(begin); begin += 4;
    Triple_d* writer = &triples_d[0];
    (*writer).value1 = value1;
    (*writer).value2 = value2;
    (*writer).count = count;
//	cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
    ++writer;

    // Decompress the remainder of the page
    while (begin < end) {
        value1 = readDouble(begin); begin += sizeof(double);
        value2 = readDelta4(begin); begin += 4;
        count = readDelta4(begin); begin += 4;
        Triple_d* writer = &triples_d[0];
        (*writer).value1=value1;
        (*writer).value2=value2;
        (*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
        ++writer;
    }

    // Update the entries
    d_pos=triples_d;
    d_posLimit=writer;

    return begin;
}