/*
 * ChunkManager.cpp
 *
 *  Created on: 2010-4-12
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "TempFile.h"
#include "TempMMapBuffer.h"

unsigned int ChunkManager::bufferCount = 0;

//#define WORD_ALIGN 1

BitmapBuffer::BitmapBuffer(const string _dir) :
        dir(_dir) {
    startColID = 1;
    string filename(dir);
    filename.append("/temp1");
    // init file size 4MB
    temp1 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    filename.assign(dir.begin(), dir.end());
    filename.append("/temp2");
    temp2 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    filename.assign(dir.begin(), dir.end());
    filename.append("/temp3");
    temp3 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    filename.assign(dir.begin(), dir.end());
    filename.append("/temp4");
    temp4 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    filename.assign(dir.begin(), dir.end());
    filename.append("/temp5");
    temp5 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    filename.assign(dir.begin(), dir.end());
    filename.append("/temp6");
    temp6 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

    usedPage1 = usedPage2 = usedPage3 = usedPage4 = usedPage5 = usedPage6 = 0;
}

BitmapBuffer::~BitmapBuffer() {
    for (map<ID, ChunkManager *>::iterator iter = predicate_managers[0].begin();
         iter != predicate_managers[0].end(); iter++) {
        if (iter->second != 0) {
            delete iter->second;
            iter->second = NULL;
        }
    }

    for (map<ID, ChunkManager *>::iterator iter = predicate_managers[1].begin();
         iter != predicate_managers[1].end(); iter++) {
        if (iter->second != 0) {
            delete iter->second;
            iter->second = NULL;
        }
    }
}

Status BitmapBuffer::insertPredicate(ID id, unsigned char type) {
    predicate_managers[type][id] = new ChunkManager(id, type, this);
    return OK;
}

size_t BitmapBuffer::getTripleCount() {
    size_t tripleCount = 0;
    map<ID, ChunkManager *>::iterator begin, limit;
    for (begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++) {
        tripleCount = tripleCount + begin->second->getTripleCount();
    }

    tripleCount = 0;
    for (begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++) {
        tripleCount = tripleCount + begin->second->getTripleCount();
    }

    return tripleCount;
}

/*
 *	@param id: predicate id
 *       type: 0 means so, 1 means os
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager *BitmapBuffer::getChunkManager(ID id, unsigned char type) {
    //there is no predicate_managers[id]
    if (!predicate_managers[type].count(id)) {
        //the first time to insert
        insertPredicate(id, type);
    }
    return predicate_managers[type][id];
}

/*
 *	@param typeID: 0 for triple being sorted by subject; 1 for triple being sorted by object
 *         objType: objType;
 */
// TODO: this function may need to be modified if the way id coded changed
Status BitmapBuffer::insertTriple(ID predicateID, ID xID, Element yID, unsigned objType, unsigned char typeID) {
    unsigned char len;

    len = sizeof(xID);
    switch (objType % objTypeNum) {
        case 0:
            len += sizeof(yID.id);
            break;
        case 1:
            len += sizeof(yID.f);
            break;
        default:
            len += sizeof(yID.d);
    }

    getChunkManager(predicateID, typeID)->insertXY(xID, yID, len, objType);

    //	cout<<getChunkManager(1, 0)->meta->length[0]<<" "<<getChunkManager(1, 0)->meta->tripleCount[0]<<endl;
    return OK;
}

void BitmapBuffer::flush() {
    temp1->flush();
    temp2->flush();
    temp3->flush();
    temp4->flush();
    temp5->flush();
    temp6->flush();
}

void BitmapBuffer::generateXY(ID &subjectID, ID &objectID) {
    ID temp;

    if (subjectID > objectID) {
        temp = subjectID;
        subjectID = objectID;
        objectID = temp - objectID;
    } else {
        objectID = objectID - subjectID;
    }
}

unsigned char BitmapBuffer::getBytes(ID id) {
    if (id <= 0xFF) {
        return 1;
    } else if (id <= 0xFFFF) {
        return 2;
    } else if (id <= 0xFFFFFF) {
        return 3;
    } else if (id <= 0xFFFFFFFF) {
        return 4;
    } else {
        return 0;
    }
}

/**
 * get page number by type and flag
 * @param type triples stored by so or os
 * @param flag objType(valued from [0, 5])
 * @param pageNo return by reference
 * @return the first unused page address
 */
// TODO: temp and usedPage
char *BitmapBuffer::getPage(unsigned char type, unsigned char flag, size_t &pageNo) {
    char *rt;
    bool tempresize = false;

    //cout<<__FUNCTION__<<" begin"<<endl;

    if (type == 0) {  //如果按S排序
        if (flag == 0) { //objType = string
            if (usedPage1 * MemoryBuffer::pagesize >= temp1->getSize()) {
                // expand 1024*pagesize = 4MB when usedPage1 increment to limit
                temp1->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage1;
            rt = temp1->get_address() + usedPage1 * MemoryBuffer::pagesize;
            usedPage1++;
        } else if (flag == 1) { //objType = float
            if (usedPage2 * MemoryBuffer::pagesize >= temp2->getSize()) {
                temp2->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage2;
            rt = temp2->get_address() + usedPage2 * MemoryBuffer::pagesize;
            usedPage2++;
        } else { // objType = double
            if (usedPage3 * MemoryBuffer::pagesize >= temp3->getSize()) {
                temp3->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage3;
            rt = temp3->get_address() + usedPage3 * MemoryBuffer::pagesize;
            usedPage3++;
        }

    } else {   //按O排序
        if (flag == 0) {
            if (usedPage4 * MemoryBuffer::pagesize >= temp4->getSize()) {
                temp4->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage4;
            rt = temp4->get_address() + usedPage4 * MemoryBuffer::pagesize;
            usedPage4++;
        } else if (flag == 1) {
            if (usedPage5 * MemoryBuffer::pagesize >= temp5->getSize()) {
                temp5->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage5;
            rt = temp5->get_address() + usedPage5 * MemoryBuffer::pagesize;
            usedPage5++;
        } else {
            if (usedPage6 * MemoryBuffer::pagesize >= temp6->getSize()) {
                temp6->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
                tempresize = true;
            }
            pageNo = usedPage6;
            rt = temp6->get_address() + usedPage6 * MemoryBuffer::pagesize;
            usedPage6++;
        };
    }

    if (tempresize == true) {
        // tempx was expanded and update chunkManger
        if (type == 0) {
            map<ID, ChunkManager *>::iterator iter, limit;
            iter = predicate_managers[0].begin();
            limit = predicate_managers[0].end();
            MMapBuffer *temp = NULL;
            switch (flag % objTypeNum) {
                case 0:
                    temp = temp1;
                    break;
                case 1:
                    temp = temp2;
                    break;
                default:
                    temp = temp3;
            }
            for (; iter != limit; iter++) {
                if (iter->second == NULL)
                    continue;
                iter->second->meta = (ChunkManagerMeta *) (temp->get_address()
                                                           +
                                                           iter->second->usedPage[flag][0]/*page num where ChunkManagerMeta stored*/
                                                           * MemoryBuffer::pagesize);
                if (iter->second->usedPage[flag].size() == 1) {
                    iter->second->meta->endPtr[flag] = temp->get_address()
                                                       + iter->second->usedPage[flag].back() * MemoryBuffer::pagesize
                                                       + MemoryBuffer::pagesize
                                                       - (iter->second->meta->length[flag] -
                                                          iter->second->meta->usedSpace[flag] -
                                                          sizeof(ChunkManagerMeta));
                } else {
                    iter->second->meta->endPtr[flag] = temp->get_address()
                                                       + iter->second->usedPage[flag].back() * MemoryBuffer::pagesize
                                                       + MemoryBuffer::pagesize
                                                       - (iter->second->meta->length[flag] -
                                                          iter->second->meta->usedSpace[flag] -
                                                          sizeof(ChunkManagerMeta));
                }
            }
        } else if (type == 1) {
            map<ID, ChunkManager *>::iterator iter, limit;
            iter = predicate_managers[1].begin();
            limit = predicate_managers[1].end();
            MMapBuffer *temp = NULL;
            switch (flag % objTypeNum) {
                case 0:
                    temp = temp4;
                    break;
                case 1:
                    temp = temp5;
                    break;
                default:
                    temp = temp6;
            }
            for (; iter != limit; iter++) {
                if (iter->second == NULL)
                    continue;
                iter->second->meta = (ChunkManagerMeta *) (temp->get_address()
                                                           +
                                                           iter->second->usedPage[flag][0]/*page num where ChunkManagerMeta stored*/
                                                           * MemoryBuffer::pagesize);
                if (iter->second->usedPage[flag].size() == 1) {
                    iter->second->meta->endPtr[flag] = temp->get_address()
                                                       + iter->second->usedPage[flag].back() * MemoryBuffer::pagesize
                                                       + MemoryBuffer::pagesize
                                                       - (iter->second->meta->length[flag] -
                                                          iter->second->meta->usedSpace[flag] -
                                                          sizeof(ChunkManagerMeta));
                } else {
                    iter->second->meta->endPtr[flag] = temp->get_address()
                                                       + iter->second->usedPage[flag].back() * MemoryBuffer::pagesize
                                                       + MemoryBuffer::pagesize
                                                       - (iter->second->meta->length[flag] -
                                                          iter->second->meta->usedSpace[flag] -
                                                          sizeof(ChunkManagerMeta));
                }
            }
        }
    }

    //cout<<__FUNCTION__<<" end"<<endl;

    return rt;
}

// TODO: need to be changed
unsigned char BitmapBuffer::getLen(ID id) {
    return sizeof(id);
}

// Created by peng on 2019-04-24 09:33:41.
// this function will be called only once when executing buildTriplebitFromN3
// TODO: need to be changed
void BitmapBuffer::save() {
    string filename = dir + "/BitmapBuffer_predicate";
    string predicateFile(filename);

    size_t predicateBufferSize = predicate_managers[0].size() * 2
                                 * (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2);
    MMapBuffer *predicateBuffer = new MMapBuffer(predicateFile.c_str(), predicateBufferSize);

    size_t bufferPageNum = usedPage1 + usedPage2 + usedPage3
                           + usedPage4 + usedPage5 + usedPage6;
    // Created by peng on 2019-04-19 15:25:51.
    // directly alloc all the memory that the buffer need.
    filename = dir + "/BitmapBuffer";
    MMapBuffer *buffer = new MMapBuffer(filename.c_str(), bufferPageNum * MemoryBuffer::pagesize);

    char *predicateWriter = predicateBuffer->get_address();
    char *bufferWriter = buffer->get_address();
    size_t chunkManagerOffset = 0;
    for (SOType i = 0; i < 2; ++i) {
        map<ID, ChunkManager *> &m = predicate_managers[i];
        map<ID, ChunkManager *>::iterator iter;
        for (iter = m.begin(); iter != m.end(); ++iter) {
            // Created by peng on 2019-04-19 15:40:30.
            // firstly save predicate info
            *(ID *) predicateWriter = iter->first;
            predicateWriter += sizeof(ID);
            // i indicate order type(so or os)
            *(SOType *) predicateWriter = i;
            predicateWriter += sizeof(SOType);
            *(size_t *) predicateWriter = chunkManagerOffset;
            predicateWriter += sizeof(size_t) * 2;
            size_t increment = iter->second->save(bufferWriter, i) * MemoryBuffer::pagesize;
            chunkManagerOffset += increment;
            bufferWriter += increment;
        }
    }

    buffer->flush();
    predicateBuffer->flush();

    //这里之前有个疑惑就是temp1-4的buffer在discard之后ChunckManager中的ChunckManagerMeta中startPtr和endPtr
    //的指向问题,也就是ChunckManagerMeta最终指向的内存地址是什么,下面425-428行对指针重新定位
    //以S排序的关联矩阵的metadata计算
    predicateWriter = predicateBuffer->get_address();
    for (int i = 0; i < 2; ++i) {
        map<ID, ChunkManager *> &m = predicate_managers[i];
        map<ID, ChunkManager *>::iterator iter;
        for (iter = m.begin(); iter != m.end(); ++iter) {
            ID id = *((ID *) predicateWriter);
            assert(iter->first == id);
            chunkManagerOffset = *(size_t *) (predicateWriter + sizeof(ID) + sizeof(SOType));
            predicateWriter += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
            char *base = buffer->get_address() + chunkManagerOffset;
            iter->second->meta = (ChunkManagerMeta *) base;
            ChunkManagerMeta *meta = iter->second->meta;
            for (int j = 0; j < objTypeNum; ++j) {
                meta->startPtr[j] = base + (j == 0 ? sizeof(ChunkManagerMeta) : 0);
                meta->endPtr[j] = meta->startPtr[j] + meta->usedSpace[j];
                base += meta->length[j];
                MetaData *metaData;
                if (meta->usedSpace[j] + (j == 0 ? sizeof(ChunkManagerMeta) : 0) <= MemoryBuffer::pagesize) {
                    metaData = (MetaData *) meta->startPtr[j];
                    metaData->usedSpace = meta->usedSpace[j];
                } else {
                    size_t usedLastPage = (meta->usedSpace[j] + (j == 0 ? sizeof(ChunkManagerMeta) : 0))
                                          % MemoryBuffer::pagesize;
                    if (usedLastPage == 0) {
                        metaData = (MetaData *) (meta->endPtr[j] - MemoryBuffer::pagesize);
                        metaData->usedSpace = MemoryBuffer::pagesize;
                    } else {
                        metaData = (MetaData *) (meta->endPtr[j] - usedLastPage);
                        metaData->usedSpace = usedLastPage;
                    }
                }
            }
        }
    }

    buffer->flush();
    temp1->discard();
    temp2->discard();
    temp3->discard();
    temp4->discard();
    temp5->discard();
    temp6->discard();

    // Created by peng on 2019-04-19 18:18:51.
    // TODO: @youyujie, the code below included in youyujie's work.
    //build index;
    MMapBuffer *bitmapIndex = NULL;
    predicateWriter = predicateBuffer->get_address();
#ifdef MYDEBUG
    cout<<"build hash index for subject"<<endl;
#endif
    //给每个chunckManage后的chunk块创建索引
    for (map<ID, ChunkManager *>::iterator iter = predicate_managers[0].begin();
         iter != predicate_managers[0].end(); iter++) {
        if (iter->second) {
#ifdef MYDEBUG
            cout<<iter->first<<endl;
#endif
            //索引建立2018年11月6日19:27:02
            iter->second->buildChunkIndex();
            chunkManagerOffset = iter->second->getChunkIndex(0)->save(bitmapIndex);
            iter->second->getChunkIndex(1)->save(bitmapIndex);
            iter->second->getChunkIndex(2)->save(bitmapIndex);
            predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType) + sizeof(size_t);
            *((size_t *) predicateWriter) = chunkManagerOffset;
            predicateWriter = predicateWriter + sizeof(size_t);
        }
    }

#ifdef MYDEBUG
    cout<<"build hash index for object"<<endl;
#endif
    for (map<ID, ChunkManager *>::iterator iter = predicate_managers[1].begin();
         iter != predicate_managers[1].end(); iter++) {
        if (iter->second) {
#ifdef MYDEBUF
            cout<<iter->first<<endl;
#endif
            iter->second->buildChunkIndex();
            chunkManagerOffset = iter->second->getChunkIndex(0)->save(bitmapIndex);
            iter->second->getChunkIndex(1)->save(bitmapIndex);
            iter->second->getChunkIndex(2)->save(bitmapIndex);
            predicateWriter = predicateWriter + sizeof(ID) + sizeof(SOType) + sizeof(size_t);
            *((size_t *) predicateWriter) = chunkManagerOffset;
            predicateWriter = predicateWriter + sizeof(size_t);
        }
    }

    delete bitmapIndex;
    delete buffer;
    delete predicateBuffer;
}

// TODO: need to be changed
BitmapBuffer *BitmapBuffer::load(MMapBuffer *bitmapImage,
                                 MMapBuffer *&bitmapIndexImage,
                                 MMapBuffer *bitmapPredicateImage) {
    // bitmapImage: file BitmapBuffer
    // bitmapIndexImage: file BitmapBuffer_index
    // bitmapPredicateImage: file BitmapBuffer_predicate
    BitmapBuffer *buffer = new BitmapBuffer();
    char *predicateReader = bitmapPredicateImage->get_address();

    ID id;
    SOType soType;
    size_t offset = 0, indexOffset = 0, predicateOffset = 0;
    size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

    while (predicateOffset < sizePredicateBuffer) {
        id = *((ID *) predicateReader);
        predicateReader += sizeof(ID);
        soType = *((SOType *) predicateReader);
        predicateReader += sizeof(SOType);
        offset = *((size_t *) predicateReader);
        predicateReader += sizeof(size_t);
        //TripleBit/BitmapBuffer.cpp:323: predicateWriter += sizeof(size_t) * 2;
        // second size_t stores indexOffset
        indexOffset = *((size_t *) predicateReader);
        predicateReader += sizeof(size_t);
        // Created by peng on 2019-04-19 18:22:36.
        // TODO: @youyujie, load index is your work.
        if (soType == 0) {
            ChunkManager *manager = ChunkManager::load(id, 0, bitmapImage->get_address(), offset);
            manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX,
                                                         LineHashIndex::INT, bitmapIndexImage->get_address(),
                                                         indexOffset);
            manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX,
                                                         LineHashIndex::FLOAT, bitmapIndexImage->get_address(),
                                                         indexOffset);
            manager->chunkIndex[2] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX,
                                                         LineHashIndex::DOUBLE, bitmapIndexImage->get_address(),
                                                         indexOffset);
            buffer->predicate_managers[0][id] = manager;
        } else if (soType == 1) {
            ChunkManager *manager = ChunkManager::load(id, 1, bitmapImage->get_address(), offset);
            manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX,
                                                         LineHashIndex::INT, bitmapIndexImage->get_address(),
                                                         indexOffset);
            manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX,
                                                         LineHashIndex::FLOAT, bitmapIndexImage->get_address(),
                                                         indexOffset);
            manager->chunkIndex[2] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX,
                                                         LineHashIndex::DOUBLE, bitmapIndexImage->get_address(),
                                                         indexOffset);
            buffer->predicate_managers[1][id] = manager;
        }
        predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
    }

    return buffer;
}

// TODO: need to be changed
void BitmapBuffer::endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld) {

    size_t bufferOffsetPage = 0, lastOffsetPage = 0;
    size_t predicateOffset = 0;
    size_t predicateSize = bitmapPredicateImage->get_length();

    // Created by peng on 2019-04-22 16:48:26.
    // temp buffer, initialized pagesize in origin code.
    // Then increment pagesize after copying one page to buffer.

    // Created by peng on 2019-04-23 13:29:54.
    // TODO: buffer expand unfinished
    // Updated by peng on 2019-04-24 09:27:41.
    // But now I want to directly alloc enough memory that can store all pages(chunks)
    size_t bufferSize = bitmapOld->getSize()
                        + TempMMapBuffer::getInstance().getUsedPage() * MemoryBuffer::pagesize;
    string bitmapName = dir + "/BitmapBuffer_Temp";
    MMapBuffer *buffer = new MMapBuffer(bitmapName.c_str(), bufferSize);
    char *predicateReader = bitmapPredicateImage->get_address();

    while (predicateOffset < predicateSize) {
        char *bufferWriter = buffer->get_address() + bufferOffsetPage * MemoryBuffer::pagesize;

        ID id = *((ID *) predicateReader);
        predicateReader += sizeof(ID);
        SOType soType = *((SOType *) predicateReader);
        predicateReader += sizeof(SOType);
        *((size_t *) predicateReader) = bufferWriter - buffer->get_address();
        predicateReader += sizeof(size_t);
        predicateReader += sizeof(size_t); //skip the indexoffset

        // Created by peng on 2019-04-23 11:11:56.
        // update chunkManager data
        ChunkManager *chunkManager = predicate_managers[soType][id];
        for (int j = 0; j < objTypeNum; ++j) {
            lastOffsetPage = bufferOffsetPage;
            int currentChunkNum = 0;
            int totalChunkNum = chunkManager->getChunkNumber(j);
            uchar *chunkBegin = chunkManager->getStartPtr(j);
            if (j == 0) chunkBegin -= sizeof(ChunkManagerMeta);
            while (currentChunkNum < totalChunkNum) {
                chunkBegin += currentChunkNum * MemoryBuffer::pagesize;
                MetaData *metaData = (MetaData *) (chunkBegin +
                                                   (j == 0 && currentChunkNum == 0 ? sizeof(ChunkManagerMeta) : 0));
                memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
                bufferOffsetPage += 1;
                bufferWriter += MemoryBuffer::pagesize;
                MetaData *newMetaData = (MetaData *) (bufferWriter +
                                                      (j == 0 && currentChunkNum == 0 ? sizeof(ChunkManagerMeta) : 0));
                newMetaData->haveNextPage = false;
                newMetaData->NextPageNo = 0;
                while (metaData->haveNextPage) {
                    char *tempChunkBegin = TempMMapBuffer::getInstance().getAddress()
                                           + metaData->NextPageNo * MemoryBuffer::pagesize;
                    metaData = (MetaData *) tempChunkBegin;
                    if (metaData->usedSpace == sizeof(MetaData)) break;
                    memcpy(bufferWriter, tempChunkBegin, MemoryBuffer::pagesize);
                    newMetaData = (MetaData *) bufferWriter;
                    newMetaData->haveNextPage = false;
                    newMetaData->NextPageNo = 0;
                    bufferWriter += MemoryBuffer::pagesize;
                    bufferOffsetPage += 1;
                }
                currentChunkNum += 1;
            }
            // Created by peng on 2019-04-23 11:13:23.
            // update ChunkManagerMeta data
            char *bufferWriterBegin = buffer->get_address() + lastOffsetPage * MemoryBuffer::pagesize;
            char *bufferWriterEnd = buffer->get_address() + bufferOffsetPage * MemoryBuffer::pagesize;

            if (bufferOffsetPage == lastOffsetPage + 1) {
                ChunkManagerMeta *meta = (ChunkManagerMeta *) (bufferWriterBegin);
                MetaData *metaData = (MetaData *) (bufferWriterBegin + sizeof(ChunkManagerMeta));
                meta->usedSpace[j] = metaData->usedSpace;
                meta->length[j] = MemoryBuffer::pagesize;
            } else {
                ChunkManagerMeta *meta = (ChunkManagerMeta *) (bufferWriterBegin);
                MetaData *metaData = (MetaData *) (bufferWriterEnd - MemoryBuffer::pagesize);
                meta->usedSpace[j] = bufferWriterEnd - bufferWriterBegin
                                     - (j == 0 ? sizeof(ChunkManagerMeta) : 0) - MemoryBuffer::pagesize
                                     + metaData->usedSpace;
                meta->length[j] = bufferWriterEnd - bufferWriterBegin;
                assert(meta->length[j] % MemoryBuffer::pagesize == 0);
            }
            buffer->flush();
        }
        //not update the LineHashIndex
        predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
    }

    size_t chunkManagerOffset = 0;
    predicateOffset = 0;
    predicateReader = bitmapPredicateImage->get_address();
    while (predicateOffset < predicateSize) {
        ID id = *((ID *) predicateReader);
        predicateReader += sizeof(ID);
        SOType soType = *((SOType *) predicateReader);
        predicateReader += sizeof(SOType);
        chunkManagerOffset = *((size_t *) predicateReader);
        predicateReader += sizeof(size_t);
        predicateReader += sizeof(size_t);

        char *base = buffer->get_address() + chunkManagerOffset;
        ChunkManagerMeta *meta = (ChunkManagerMeta *) base;
        for (int j = 0; j < objTypeNum; ++j) {
            meta->startPtr[j] = base + (j == 0 ? sizeof(ChunkManagerMeta) : 0);
            meta->endPtr[j] = meta->startPtr[j] + meta->usedSpace[j];
            base += meta->length[j];
        }
        predicate_managers[soType][id]->meta = meta;
        // Created by peng on 2019-04-23 13:23:25.
        // TODO: @youyujie, your work, do it!!!
        predicate_managers[soType][id]->buildChunkIndex();
        predicate_managers[soType][id]->updateChunkIndex();

        predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
    }
    buffer->flush();

    string bitmapNameOld = dir + "/BitmapBuffer";
//	MMapBuffer *bufferOld = new MMapBuffer(bitmapNameOld.c_str(), 0);
    bitmapOld->discard();
    if (rename(bitmapName.c_str(), bitmapNameOld.c_str()) != 0) {
        perror("rename bitmapName error!");
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void getTempFilename(string &filename, unsigned pid, unsigned _type) {
    filename.clear();
    filename.append(DATABASE_PATH);
    filename.append("temp_");
    char temp[5];
    sprintf(temp, "%d", pid);
    filename.append(temp);
    sprintf(temp, "%d", _type);
    filename.append(temp);
}

ChunkManager::ChunkManager(unsigned pid, unsigned _type, BitmapBuffer *_bitmapBuffer) :
        bitmapBuffer(_bitmapBuffer) {
    for (int i = 0; i < objTypeNum; ++i) {
        usedPage[i].resize(0);
    }
    size_t pageNo = 0;
    meta = NULL;
    for (int i = 0; i < objTypeNum; ++i) {
        ptrs[i] = bitmapBuffer->getPage(_type, i, pageNo);
        usedPage[i].push_back(pageNo);
        if (i == 0) {
            meta = (ChunkManagerMeta *) ptrs[i];
            memset((char *) meta, 0, sizeof(ChunkManagerMeta));
            meta->endPtr[i] = meta->startPtr[i] = ptrs[i] + sizeof(ChunkManagerMeta);
        } else {
            meta->startPtr[i] = meta->endPtr[i] = ptrs[i];
        }
        //meta->length[type-1]的初始大小应该是1*MemoryBuffer::pagesize,即4KB
        meta->length[i] = usedPage[i].size() * MemoryBuffer::pagesize;
        meta->usedSpace[i] = 0;
        meta->tripleCount[i] = 0;
    }
    meta->pid = pid;
    meta->type = _type;

    // TODO: @youyujie, the code beneath should be modified by youyujie
    if (meta->type == 0) {
        chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::INT);
        chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::FLOAT);
        chunkIndex[2] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::DOUBLE);
    } else {
        chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX, LineHashIndex::INT);
        chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX, LineHashIndex::FLOAT);
        chunkIndex[2] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX, LineHashIndex::DOUBLE);
    }
}

ChunkManager::~ChunkManager() {
    ///free the buffer;
    for (int i = 0; i < objTypeNum; ++i) {
        ptrs[i] = NULL;
    }

    if (chunkIndex[0] != NULL)
        delete chunkIndex[0];
    chunkIndex[0] = NULL;
    if (chunkIndex[1] != NULL)
        delete chunkIndex[1];
    chunkIndex[1] = NULL;
    if (chunkIndex[2] != NULL)
        delete chunkIndex[2];
    chunkIndex[2] = NULL;
}

// TODO: function need to be changed if storage changed
static void getInsertChars(char *temp, unsigned x, Element y, unsigned objType) {
    switch (objType) {
        case 0:
            memcpy(temp, &x, sizeof(x));
            memcpy(temp + sizeof(x), &y.id, sizeof(y.id));
            break;
        case 1:
            memcpy(temp, &x, sizeof(x));
            memcpy(temp + sizeof(x), &y.f, sizeof(y.f));
            break;
        case 2:
            memcpy(temp, &x, sizeof(x));
            memcpy(temp + sizeof(x), &y.d, sizeof(y.d));
            break;
        case 3:
            memcpy(temp, &y.id, sizeof(y.id));
            memcpy(temp + sizeof(y.id), &x, sizeof(x));
            break;
        case 4:
            memcpy(temp, &y.f, sizeof(y.f));
            memcpy(temp + sizeof(y.f), &x, sizeof(x));
            break;
        case 5:
            memcpy(temp, &y.d, sizeof(y.d));
            memcpy(temp + sizeof(y.d), &x, sizeof(x));
            break;
    }
}

// TODO: function need to be changed if storage changed
void ChunkManager::insertXY(unsigned x, Element y, unsigned len, unsigned char type)
//x:xID, y:yID, len:len(xID + yID), (type: objType);
{
    char temp[15];
    // Created by peng on 2019-04-22 09:57:40.
    // I think we don't need it now.
    // origin: 标志位设置,以128为进制单位,分解x,y,最高位为0表示x,1表示y
    // getInsertChars(temp, x, y);
    getInsertChars(temp, x, y, type);
    unsigned char aType = type;
    type = type % objTypeNum;
    //如果当前空间不够存放新的<x,y>对
    if (isPtrFull(type, len) == true) {
        if (meta->length[type] == MemoryBuffer::pagesize) {//第一个chunk,在第一个chunk被写满(存放不下下一个元组的时候，回溯指针，写metadata的信息)
            //将指针回溯到MetaData(即head区域)写入usedSpace信息
            MetaData *metaData = (MetaData *) (meta->endPtr[type] - meta->usedSpace[type]);
            metaData->usedSpace = meta->usedSpace[type];
        } else {//不是第一个chunk
            //这个usedpage计算最后一个chunk使用了多少字节，length[0]存放的是当前谓词,x<=y的数据链表的已申请buffer大小
            size_t usedPage = MemoryBuffer::pagesize
                              - (meta->length[type] - meta->usedSpace[type] -
                                 (type == 0 ? sizeof(ChunkManagerMeta) : 0));
            //MetaData地址=尾指针-最后一个4KB字节使用的字节，即指向了最后一个4KB字节的首地址，也就是head区域
            MetaData *metaData = (MetaData *) (meta->endPtr[type] - usedPage);
            metaData->usedSpace = usedPage;
        }
        //重新分配大小,修改了meta->length,增加一个4KB,meta->endptr指向下一个4KB的首地址
        resize(type);
        //为下一个4KB创建head信息，下一个chunk的metadata首地址是meta->endPtr[]
        MetaData *metaData = (MetaData *) (meta->endPtr[type]);

        // Created by peng on 2019-04-24 20:18:25.
        // origin minID stores the minimum id of chunk, but now the
        // object have three types, so the type of minID should be
        // changed from ID to double(double can hold ID and float)
        if (aType < objTypeNum) metaData->minID = x;
        else
            switch (aType % objTypeNum) {
                case 0:
                    metaData->minID = y.id;
                    break;
                case 1:
                    metaData->minID = y.f;
                    break;
                default:
                    metaData->minID = y.d;
            }

        metaData->haveNextPage = false;
        metaData->NextPageNo = 0;
        metaData->usedSpace = 0;

        memcpy(meta->endPtr[type] + sizeof(MetaData), temp, len);
        meta->endPtr[type] = meta->endPtr[type] + sizeof(MetaData) + len;
        meta->usedSpace[type] = meta->length[type] - MemoryBuffer::pagesize
                                - (type == 0 ? sizeof(ChunkManagerMeta) : 0) + sizeof(MetaData) + len;
        tripleCountAdd(type);
    } else if (meta->usedSpace[type] == 0) { //如果usedspace==0，即第一个chunk块，则创建head区域
        MetaData *metaData = (MetaData *) (meta->startPtr[type]);
        memset((char *) metaData, 0, sizeof(MetaData));//将head区域初始化为0

        // Created by peng on 2019-04-24 20:18:25.
        // origin minID stores the minimum id of chunk, but now the
        // object have three types, so the type of minID should be
        // changed from ID to double(double can hold ID and float)
        if (aType < objTypeNum) metaData->minID = x;
        else
            switch (aType % objTypeNum) {
                case 0:
                    metaData->minID = y.id;
                    break;
                case 1:
                    metaData->minID = y.f;
                    break;
                default:
                    metaData->minID = y.d;
            }

        metaData->haveNextPage = false;
        metaData->NextPageNo = 0;
        metaData->usedSpace = 0;

        memcpy(meta->endPtr[type] + sizeof(MetaData), temp, len); //将数据拷贝到head区域的后面len个字节中去
        meta->endPtr[type] = meta->endPtr[type] + sizeof(MetaData) + len;//重新定位endPtr[type-1]的位置
        meta->usedSpace[type] = sizeof(MetaData) + len; //更新usedSpace的大小,包括MetaData的大小在内。
        tripleCountAdd(type);
    } else {    //如果不是新的块，则直接将数据拷贝到endPtr[type-1]的后len个字节中去。
        memcpy(meta->endPtr[type], temp, len);

        meta->endPtr[type] = meta->endPtr[type] + len;
        meta->usedSpace[type] = meta->usedSpace[type] + len;
        tripleCountAdd(type);
    }
}

Status ChunkManager::resize(unsigned char type) {
    // TODO
    size_t pageNo = 0;
    ptrs[type] = bitmapBuffer->getPage(meta->type, type, pageNo);
    usedPage[type].push_back(pageNo);
    meta->length[type] = usedPage[type].size() * MemoryBuffer::pagesize;
    meta->endPtr[type] = ptrs[type];

    bufferCount++;
    return OK;
}

/// build the hash index for query;
Status ChunkManager::buildChunkIndex() {
    chunkIndex[0]->buildIndex(0);
    chunkIndex[1]->buildIndex(1);
    chunkIndex[2]->buildIndex(2);
    return OK;
}

// Created by peng on 2019-04-19 11:58:15.
// TODO: @youyujie, maybe youyujie need to modify the code below
/// update the hash index for Query
Status ChunkManager::updateChunkIndex() {
    chunkIndex[0]->updateLineIndex();
    chunkIndex[1]->updateLineIndex();
    chunkIndex[2]->updateLineIndex();

    return OK;
}

bool ChunkManager::isPtrFull(unsigned char type, unsigned len) {
    len = len + (type == 0 ? sizeof(ChunkManagerMeta) : 0);
    return meta->usedSpace[type] + len >= meta->length[type];
}

ID ChunkManager::getChunkNumber(unsigned char type) {
    return (meta->length[type]) / (MemoryBuffer::pagesize);
}

ChunkManager *ChunkManager::load(unsigned pid, unsigned type, char *buffer, size_t &offset) {
    ChunkManagerMeta *meta = (ChunkManagerMeta *) (buffer + offset);
    if (meta->pid != pid || meta->type != type) {
        MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERROR);
        cout << meta->pid << ": " << meta->type << endl;
        return NULL;
    }

    ChunkManager *manager = new ChunkManager();
    char *base = buffer + offset;
    manager->meta = meta;

    for (int i = 0; i < objTypeNum; ++i) {
        manager->meta->startPtr[i] = base + (i == 0 ? sizeof(ChunkManagerMeta) : 0);
        manager->meta->endPtr[i] = manager->meta->startPtr[i] + manager->meta->usedSpace[i];
        base += manager->meta->length[i];
    }
    offset = base - buffer;
    return manager;
}

size_t ChunkManager::save(char *buffer, SOType type) {
    size_t increment = 0;
    MMapBuffer *temp[3];
    temp[0] = this->bitmapBuffer->temp1;
    temp[1] = this->bitmapBuffer->temp2;
    temp[2] = this->bitmapBuffer->temp3;
    if (type) {
        temp[0] = this->bitmapBuffer->temp4;
        temp[1] = this->bitmapBuffer->temp5;
        temp[2] = this->bitmapBuffer->temp6;
    }
    for (int i = 0; i < objTypeNum; ++i) {
        vector<size_t> v = usedPage[i];
        increment += v.size();
        for (int j = 0; j < v.size(); ++j) {
            memcpy(buffer, temp[i]->get_address() + v[j] * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
            buffer += MemoryBuffer::pagesize;
        }
    }
    return increment;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

Chunk::Chunk(unsigned char type, ID xMax, ID xMin, ID yMax, ID yMin, char *startPtr, char *endPtr) {
    this->type = type;
    this->xMax = xMax;
    this->xMin = xMin;
    this->yMax = yMax;
    this->yMin = yMin;
    count = 0;
    this->startPtr = startPtr;
    this->endPtr = endPtr;
}

Chunk::~Chunk() {
    this->startPtr = 0;
    this->endPtr = 0;
}

/*
 *	write x id; set the 7th bit to 0 to indicate it is a x byte;
 */
void Chunk::writeXId(ID id, char *&ptr) {
    // Write a id
    while (id >= 128) {
        unsigned char c = static_cast<unsigned char> (id & 127);
        *ptr = c;
        ptr++;
        id >>= 7;
    }
    *ptr = static_cast<unsigned char> (id & 127);
    ptr++;
}

/*
 *	write y id; set the 7th bit to 1 to indicate it is a y byte;
 */
void Chunk::writeYId(ID id, char *&ptr) {
    while (id >= 128) {
        unsigned char c = static_cast<unsigned char> (id | 128);
        *ptr = c;
        ptr++;
        id >>= 7;
    }
    *ptr = static_cast<unsigned char> (id | 128);
    ptr++;
}

static inline unsigned int readUInt(const uchar *reader) {
    return (reader[0] << 24 | reader[1] << 16 | reader[2] << 8 | reader[3]);
}

const uchar *Chunk::readXId(const uchar *reader, ID &id) {
    // Read an x id
    id = *(ID *) reader;
    reader += sizeof(ID);
    return reader;
    /*
    switch (objType) {
        case 0:
        case 1:
        case 2:
        case 3:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
        case 4:
            id = *(float *) reader;
            reader += sizeof(float);
            break;
        case 5:
            id = *(double *) reader;
            reader += sizeof(double);
            break;
    }
    return reader;
     */
}
const uchar *Chunk::readXId(const uchar *reader, float &id) {
    // Read an x id
    id = *(float *) reader;
    reader += sizeof(float);
    return reader;
    /*
    switch (objType) {
        case 0:
        case 1:
        case 2:
        case 3:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
        case 4:
            id = *(float *) reader;
            reader += sizeof(float);
            break;
        case 5:
            id = *(double *) reader;
            reader += sizeof(double);
            break;
    }
    return reader;
     */
}
const uchar *Chunk::readXId(const uchar *reader, double &id) {
    // Read an x id
    id = *(double *) reader;
    reader += sizeof(double);
    return reader;
    /*
    switch (objType) {
        case 0:
        case 1:
        case 2:
        case 3:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
        case 4:
            id = *(float *) reader;
            reader += sizeof(float);
            break;
        case 5:
            id = *(double *) reader;
            reader += sizeof(double);
            break;
    }
    return reader;
     */
}
const uchar *Chunk::readXId(const uchar *reader, double &id, unsigned objType) {
    // Read an x id
    switch (objType) {
        case 0:
        case 1:
        case 2:
        case 3:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
        case 4:
            id = *(float *) reader;
            reader += sizeof(float);
            break;
        case 5:
            id = *(double *) reader;
            reader += sizeof(double);
            break;
    }
    return reader;
}

const uchar *Chunk::readXYId(const uchar *reader, ID &xid, ID &yid) {

    readYId(readXId(reader, xid), yid);
    return reader;
}
const uchar *Chunk::readXYId(const uchar *reader, ID &xid, float &yid) {

    readYId(readXId(reader, xid), yid);
    return reader;
}
const uchar *Chunk::readXYId(const uchar *reader, ID &xid, double &yid) {

    readYId(readXId(reader, xid), yid);
    return reader;
}
const uchar *Chunk::readXYId(const uchar *reader, float &xid, ID &yid) {

    readYId(readXId(reader, xid), yid);
    return reader;
}
const uchar *Chunk::readXYId(const uchar *reader, double &xid, ID &yid) {

    readYId(readXId(reader, xid), yid);
    return reader;
}
const uchar *Chunk::readXYId(const uchar *reader, double &xid, double &yid, unsigned objType) {

    readYId(readXId(reader, xid, objType), yid, objType);
    return reader;
}

const uchar *Chunk::readYId(const uchar *reader, ID &id) {
    // Read an y id
    id = *(ID *) reader;
    reader += sizeof(ID);
    return reader;
}
const uchar *Chunk::readYId(const uchar *reader, float &id) {
    // Read an y id
    id = *(float *) reader;
    reader += sizeof(float);
    return reader;
}
const uchar *Chunk::readYId(const uchar *reader, double &id) {
    // Read an y id
    id = *(double *) reader;
    reader += sizeof(double);
    return reader;
}
const uchar *Chunk::readYId(const uchar *reader, double &id, unsigned objType) {
    // Read an y id
    switch (objType) {
        case 0:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
        case 1:
            id = *(float *) reader;
            reader += sizeof(float);
            break;
        case 2:
            id = *(double *) reader;
            reader += sizeof(double);
            break;
        case 3:
        case 4:
        case 5:
            id = *(ID *) reader;
            reader += sizeof(ID);
            break;
    }
    return reader;
}

uchar *Chunk::deleteXId(uchar *reader, unsigned objType)
/// Delete a subject id (just set the id to 0)
{
    switch (objType) {
        case 0:
        case 1:
        case 2:
        case 3:
            *(ID *) reader = 0;
            reader += sizeof(ID);
            break;
        case 4:
            *(float *) reader = 0;
            reader += sizeof(float);
            break;
        case 5:
            *(double *) reader = 0;
            reader += sizeof(double);
            break;
    }
    return reader;
}

uchar *Chunk::deleteYId(uchar *reader, unsigned objType)
/// Delete an object id (just set the id to 0)
{
    switch (objType) {
        case 0:
            *(ID *) reader = 0;
            reader += sizeof(ID);
            break;
        case 1:
            *(float *) reader = 0;
            reader += sizeof(float);
            break;
        case 2:
            *(double *) reader = 0;
            reader += sizeof(double);
            break;
        case 3:
        case 4:
        case 5:
            *(ID *) reader = 0;
            reader += sizeof(ID);
            break;
    }
    return reader;
}

/**
 * skip an id(x or y)
 * @param reader
 * @param flag 0: skip x, 1: skip y
 * @return
 */
const uchar *Chunk::skipId(const uchar *reader, unsigned char flag, unsigned objType) {
    // Skip an id
    if (flag) {
        switch (objType) {
            case 1:
                reader += sizeof(float);
                break;
            case 2:
                reader += sizeof(double);
                break;
            case 0:
            case 3:
            case 4:
            case 5:
                reader += sizeof(ID);
                break;
        }
    } else {
        switch (objType) {
            case 0:
            case 1:
            case 2:
            case 3:
                reader += sizeof(ID);
                break;
            case 4:
                reader += sizeof(float);
                break;
            case 5:
                reader += sizeof(double);
                break;
        }
    }
    return reader;
}

const uchar *Chunk::skipForward(const uchar *reader, unsigned objType) {
    // skip a x,y forward;
    return skipId(skipId(reader, 0, objType), 1, objType);
}

// Created by peng on 2019-04-24 22:48:45.
// TODO: remain to be changed
const uchar *Chunk::skipBackward(const uchar *reader, unsigned objType) {
    // skip backward to the last x,y;
    int decrement = 0;
    switch (objType) {
        case 0:
        case 3:
            decrement = sizeof(ID) * 2;
            break;
        case 1:
        case 4:
            decrement = sizeof(ID) + sizeof(float);
            break;
        case 2:
        case 5:
            decrement = sizeof(ID) + sizeof(double);
            break;
        default:
            cout << "Chunk::skipBackward switch default error" << endl;
    }
    double x, y;
    do {
        reader -= decrement;
        readXYId(reader, x, y, objType);
    } while (x == 0 && y == 0);
    return reader;
}

// Created by peng on 2019-04-24 22:49:11.
// TODO: remain to be changed
const uchar *Chunk::skipBackward(const uchar *reader, const uchar *begin, unsigned objType) {
    //if is the begin of One Chunk
    if (objType % objTypeNum == 0) {
        if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) % MemoryBuffer::pagesize == 0) {
            if ((reader - begin - sizeof(MetaData) + sizeof(ChunkManagerMeta)) == MemoryBuffer::pagesize) {
                reader = begin;
                MetaData *metaData = (MetaData *) reader;
                reader = reader + metaData->usedSpace;
                return skipBackward(reader, objType);
            }
            reader = begin - sizeof(ChunkManagerMeta)
                     + MemoryBuffer::pagesize
                       * ((reader - begin + sizeof(ChunkManagerMeta)) / MemoryBuffer::pagesize - 1);
            MetaData *metaData = (MetaData *) reader;
            reader = reader + metaData->usedSpace;
            return skipBackward(reader, objType);
        } else if (reader <= begin + sizeof(MetaData)) {
            // Created by peng on 2019-04-25 13:43:09.
            // empty chunk
            // TODO: how to generate return value? It should discuss with who need to access this function.
            return begin - 1;
        } else {
            //if is not the begin of one Chunk
            return skipBackward(reader, objType);
        }
    } else {
        if (reader <= begin + sizeof(MetaData)) {
            return begin - 1;
        } else if ((reader - begin - sizeof(MetaData)) % MemoryBuffer::pagesize == 0) {
            reader = begin + MemoryBuffer::pagesize * ((reader - begin) / MemoryBuffer::pagesize - 1);
            MetaData *metaData = (MetaData *) reader;
            reader = reader + metaData->usedSpace;
            return skipBackward(reader, objType);
        } else {
            //if is not the begin of one Chunk
            return skipBackward(reader, objType);
        }
    }
}
