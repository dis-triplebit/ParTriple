//
// Created by youyujie on 2019/4/29.
//

#ifndef FILERECV_STATISTICSBUFFER_H
#define FILERECV_STATISTICSBUFFER_H
class HashIndex;
class EntityIDBuffer;
class MMapBuffer;

#include "TripleBit.h"

class StatisticsBuffer {
public:
    //有四种类型的统计信息
    enum StatisticsType { SUBJECT_STATIS, OBJECT_STATIS, SUBJECTPREDICATE_STATIS, OBJECTPREDICATE_STATIS };
    StatisticsBuffer();
    virtual ~StatisticsBuffer();
    /// add a statistics record;
    virtual Status addStatis(unsigned v1, unsigned v2, unsigned v3) = 0;
    virtual Status addStatis(float v1, unsigned v2, unsigned v3) = 0;
    virtual Status addStatis(double v1, unsigned v2, unsigned v3) = 0;
    /// get a statistics record;
    virtual Status getStatis(unsigned& v1, unsigned v2) = 0;
    virtual Status getStatis(float& v1, unsigned v2) = 0;
    virtual Status getStatis(double& v1, unsigned v2) = 0;
    /// save the statistics record to file;
    //virtual Status save(ofstream& file) = 0;
    /// load the statistics record from file;
    //virtual StatisticsBuffer* load(ifstream& file) = 0;
protected:
    const unsigned HEADSPACE;
};

class OneConstantStatisticsBuffer : public StatisticsBuffer {
public:
    struct Triple {
        ID value1;
        unsigned count;
    };
    struct Triple_f {
        float value1;
        unsigned count;
    };

    struct Triple_d {
        double value1;
        unsigned count;
    };

private:
    StatisticsType type;
    MMapBuffer* buffer;
    const unsigned char* reader;
    unsigned char* writer;

    /// index for query;
    //是否需要改成double?
    vector<unsigned> index;
    unsigned indexSize;
    unsigned nextHashValue;
    unsigned lastId;
    unsigned usedSpace;
    //unsigned dataType;
    const unsigned ID_HASH;

    Triple* triples, *pos, *posLimit;
    Triple_f* f_triples, *f_pos, *f_posLimit;
    Triple_d* d_triples, *d_pos, *d_posLimit;
    bool first;
public:
    //dataType为0表示为int时,为1表示为float时,为2表示为double时
    OneConstantStatisticsBuffer(const string path, StatisticsType type,unsigned dataType);
    virtual ~OneConstantStatisticsBuffer();
    Status addStatis(unsigned v1, unsigned v2, unsigned v3 = 0);
    Status addStatis(float v1, unsigned v2, unsigned v3 = 0);
    Status addStatis(double v1, unsigned v2, unsigned v3 = 0);
    Status getStatis(unsigned& v1, unsigned v2 = 0);
    //得到的返回值存储在v1中，需要将v1强转为int
    Status getStatis(float& v1, unsigned v2 = 0);
    //得到的返回值存储在v1中，需要将v1强转为int
    Status getStatis(double& v1, unsigned v2 = 0);
    //type为0时表示Subject或者Object，dataType为0表示Object为int时,为1表示Object为float时,为2表示Object为double时
    Status save(MMapBuffer*& indexBuffer,StatisticsType type,unsigned dataType);
    static OneConstantStatisticsBuffer* load(StatisticsType type, const string path, char*& indexBuffer,unsigned dataType);
    /// get the subject or object ids from minID to maxID;
    Status getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID);
    Status getIDs(EntityIDBuffer* entBuffer, float minID, float maxID);
    Status getIDs(EntityIDBuffer* entBuffer, double minID, double maxID);
    //dataType为0表示Object为int时,为1表示Object为float时,为2表示Object为double时
    unsigned int getEntityCount(unsigned dataType);
private:
    /// write a id to buffer; isID indicate the id really is a ID, maybe is a count.
    void writeId(unsigned id, char*& ptr, bool isID);
    /// read a id from buffer;
    const char* readId(unsigned& id, const char* ptr, bool isID);
    /// judge the buffer is full;
    bool isPtrFull(unsigned len);
    /// get the value length in bytes;
    unsigned getLen(unsigned v);
    //dataType为0时表示整数,为1时表示float,为2时表示double
    const unsigned char* decode(const unsigned char* begin, const unsigned char* end, unsigned dataType);
    bool find(unsigned value);
    bool find_last(unsigned value);
    bool find(float value);
    bool find_last(float value);
    bool find(double value);
    bool find_last(double value);

//    virtual char* writeData(char* writer, unsigned data)
//    {
//        memcpy(writer, &data, sizeof(unsigned));
//        return writer+sizeof(unsigned);
//    }
//
//    virtual const char* readData(const char* reader, unsigned & data){
//        memcpy(&data, reader, sizeof(unsigned));
//        return reader+sizeof(unsigned);
//    }

};

class TwoConstantStatisticsBuffer : public StatisticsBuffer {
public:
    struct Triple{
        ID value1;
        ID value2;
        ID count;
    };
    struct Triple_f{
        float value1;
        ID value2;
        ID count;
    };
    struct Triple_d{
        double value1;
        ID value2;
        ID count;
    };
private:
    StatisticsType type;
    MMapBuffer* buffer;
    const unsigned char* reader;
    unsigned char* writer;

    Triple* index;
    Triple_f* index_f;
    Triple_d* index_d;

    unsigned lastId, lastPredicate;
    unsigned usedSpace;
    unsigned currentChunkNo;
    unsigned indexPos, indexSize;

    Triple triples[3 * 4096];
    Triple_f triples_f[3 * 4096];
    Triple_d triples_d[4 * 4096];
    Triple* pos, *posLimit;
    Triple_f* f_pos, *f_posLimit;
    Triple_d* d_pos, *d_posLimit;
    bool first;
public:
    //dataType为0表示为int时,为1表示为float时,为2表示为double时
    TwoConstantStatisticsBuffer(const string path, StatisticsType type, unsigned dataType);
    virtual ~TwoConstantStatisticsBuffer();
    /// add a statistics record;
    Status addStatis(unsigned v1, unsigned v2, unsigned v3);
    Status addStatis(float v1, unsigned v2, unsigned v3);
    Status addStatis(double v1, unsigned v2, unsigned v3);
    /// get a statistics record;
    Status getStatis(unsigned& v1, unsigned v2);
    Status getStatis(float& v1, unsigned v2);
    Status getStatis(double& v1, unsigned v2);
    /// get the buffer position by a id, used in query
    Status getPredicatesByID(unsigned id, EntityIDBuffer* buffer, ID minID, ID maxID);
    Status getPredicatesByID(float id, EntityIDBuffer* buffer, float minID, float maxID);
    Status getPredicatesByID(double id, EntityIDBuffer* buffer, double minID, double maxID);
    /// save the statistics buffer;dataType为0表示为int时,为1表示为float时,为2表示为double时
    Status save(MMapBuffer*& indexBuffer,unsigned dataType);
    /// load the statistics buffer;
    static TwoConstantStatisticsBuffer* load(StatisticsType type, const string path, char*& indxBuffer, unsigned dataType);
private:
    /// get the value length in bytes;
    unsigned getLen(unsigned v);
    /// decode a chunk
    const uchar* decode(const uchar* begin, const uchar* end, unsigned dataType);
    /// decode id and predicate in a chunk
    const uchar* decodeIdAndPredicate(const uchar* begin, const uchar* end, unsigned dataType);
    ///
    bool find(unsigned value1, unsigned value2);
    int findPredicate(unsigned,Triple*,Triple*);

    bool find(float value1, unsigned value2);
    int findPredicate(float,Triple_f*,Triple_f*);

    bool find(double value1, unsigned value2);
    int findPredicate(double,Triple_d*,Triple_d*);
    ///
    bool find_last(unsigned value1, unsigned value2);
    bool find_last(float value1, unsigned value2);
    bool find_last(double value1, unsigned value2);
    bool find(unsigned,Triple* &,Triple* &);
    bool find(float,Triple_f* &,Triple_f* &);
    bool find(double,Triple_d* &,Triple_d* &);
    const uchar* decode(const uchar* begin, const uchar* end,Triple*,Triple*& ,Triple*&);
    const uchar* decode(const uchar* begin, const uchar* end,Triple_f*,Triple_f*& ,Triple_f*&);
    const uchar* decode(const uchar* begin, const uchar* end,Triple_d*,Triple_d*& ,Triple_d*&);
//
//    virtual char* writeData(char* writer, unsigned data)
//    {
//        memcpy(writer, &data, sizeof(unsigned));
//        return writer+sizeof(unsigned);
//    }
//
//    virtual const char* readData(const char* reader, unsigned & data){
//        memcpy(&data, reader, sizeof(unsigned));
//        return reader+sizeof(unsigned);
//    }

};

#endif //FILERECV_StatisticsBuffer_H
