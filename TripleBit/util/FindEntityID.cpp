//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "FindEntityID.h"
#include "SortMergeJoin.h"
#include "../MemoryBuffer.h"
#include "../URITable.h"
#include "../PredicateTable.h"
#include "../TripleBitRepository.h"
#include "../BitmapBuffer.h"
#include "../EntityIDBuffer.h"
#include "../HashIndex.h"
#include "../StatisticsBuffer.h"

FindEntityID::FindEntityID(TripleBitRepository* repo) {
	// TODO Auto-generated constructor stub
	bitmap = repo->getBitmapBuffer();
	wayfile = repo->getWayfile();
	UriTable = repo->getURITable();
	preTable = repo->getPredicateTable();
	spStatBuffer = (TwoConstantStatisticsBuffer*) repo->getStatisticsBuffer(
			StatisticsBuffer::SUBJECTPREDICATE_STATIS);
	opStatBuffer = (TwoConstantStatisticsBuffer*) repo->getStatisticsBuffer(
			StatisticsBuffer::OBJECTPREDICATE_STATIS);
	sStatBuffer = (OneConstantStatisticsBuffer*) repo->getStatisticsBuffer(
			StatisticsBuffer::SUBJECT_STATIS);
	oStatBuffer = (OneConstantStatisticsBuffer*) repo->getStatisticsBuffer(
			StatisticsBuffer::OBJECT_STATIS);

	XTemp = new EntityIDBuffer();
	XYTemp = new EntityIDBuffer();
	tempBuffer1 = new EntityIDBuffer();
	tempBuffer2 = new EntityIDBuffer();
	pthread_mutex_init(&mergeBufferMutex, NULL);
}

FindEntityID::~FindEntityID() {
	// TODO Auto-generated destructor stub
	if (XTemp != NULL)
		delete XTemp;
	XTemp = NULL;

	if (XYTemp != NULL)
		delete XYTemp;
	XYTemp = NULL;

	if (tempBuffer1 != NULL)
		delete tempBuffer1;
	tempBuffer1 = NULL;

	if (tempBuffer2 != NULL)
		delete tempBuffer2;
	tempBuffer2 = NULL;

	pthread_mutex_destroy(&mergeBufferMutex);
}

Status FindEntityID::findSubjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
	if (minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
//	cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(minID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}

		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;
				if (x < minID) {

				} else if (x <= maxID) {
					XTemp->insertID(x);
					//cout << x << " ";
					if (XTemp->getSize() > maxNum) {
						return TOO_MUCH;
					}
				} else {
					break;
				}
				base += (size_t) ceil(
						(double) (len + 10) / BucketManager::pagesize)
						* BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				for (int i = 0; i < size; i++) {
					x = *(ID*) (reader + idoff);
					if (x == 0)
						break;
					idoff += sizeof(ID);
					idoff += 2;
					if (x < minID) {
						continue;
					} else if (x <= maxID) {
						XTemp->insertID(x);
						//cout << x << " ";
						if (XTemp->getSize() > maxNum) {
							return TOO_MUCH;
						}
					} else {
						break;
					}
				}
				base += BucketManager::pagesize;
			}
			if (x > maxID)
				break;
			reader = base;
		}
		buffer->operator =(XTemp);

	}
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
	if (minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	unsigned offset;
	Status s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID,
			1, offset);
	//cout<<"excute S<O start"<<endl;
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		reader = startPtr;
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			//cout<<x<<" ";
			if (x == 0)
				break;
			else if (x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
				if (XTemp->getSize() > maxNum) {
					//cout<<"too much1"<<endl;
					return TOO_MUCH;
				}
			} else {
				break;
			}
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}
	if (XTemp->getSize() >= 2)
		XTemp->uniqe();
	//cout<<"excute S<O over"<<endl;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 2,
			offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
		reader = startPtr;
		for (; reader < limit;) {
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			key = x + y;
			if (key < minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(key);
				if (XYTemp->getSize() > maxNum) {
					//cout<<"too much2"<<endl;
					return TOO_MUCH;
				}
				//cout<<key<<" ";
			} else {
				break;
			}
		}
	}
	if (XTemp->getSize() >= 2)
		XYTemp->uniqe();
	//cout<<"excute S>=O over"<<endl;

	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif

	return OK;
}
Status FindEntityID::findSubjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findSubjectIDByPredicate_XY(predicateID, buffer, minID, maxID,
				maxNum);
	} else
		return findSubjectIDByPredicate_Adj(predicateID, buffer, minID, maxID,
				maxNum);

}
Status FindEntityID::findSubjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findSubjectIDByPredicate_XY(predicateID, buffer);
	} else
		return findSubjectIDByPredicate_Adj(predicateID, buffer);

}
Status FindEntityID::findSubjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
//	cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__;
//	cerr << "this" << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit, *reader;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	reader = startPtr;
	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}
	XTemp->uniqe();
	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
	reader = startPtr;
	for (; reader < limit;) {
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		XYTemp->insertID(x + y);
	}
	XYTemp->uniqe();
	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);

	startPtr = manager->getStartPtr();
	limit = manager->getEndPtr();
	base = startPtr - sizeof(BucketManagerMeta);
	reader = startPtr;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if (x == 0)
				break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			XTemp->insertID(x);
			//cout << x << " ";
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize)
					* BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				idoff += 2;
				XTemp->insertID(x);
				//cout << x << " ";

			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif

	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findObjectIDAndSubjectIDByPredicate_XY(predicateID, buffer);
	} else
		return findObjectIDAndSubjectIDByPredicate_Adj(predicateID, buffer);
}
Status FindEntityID::findObjectIDAndSubjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);
	for (; startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);
	for (; startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}
	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif

	return OK;
}
Status FindEntityID::findObjectIDAndSubjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);

	startPtr = manager->getStartPtr();
	limit = manager->getEndPtr();

	base = startPtr - sizeof(BucketManagerMeta);

	reader = startPtr;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	ID* p;
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if (x == 0)
				break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;

			if (len != 0)
				BucketManager::readBody(x, reader + idoff, len, XTemp);
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(x);
			 XTemp->insertID(p[j]);
			 }
			 ent->empty();*/
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize)
					* BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				BucketManager::getTwo(reader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {

					exit(-1);
				}
				BucketManager::readBody(x, reader + lastedgeoff,
						edgeoff + 1 - lastedgeoff, XTemp);
				lastedgeoff = edgeoff + 1;
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(x);
				 XTemp->insertID(p[j]);
				 }
				 ent->empty();*/
			}
			base += BucketManager::pagesize;
		}
		reader = base;

	}
	buffer->operator =(XTemp);

#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif

	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG1
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findSubjectIDAndObjectIDByPredicate_XY(predicateID, buffer);
	} else
		return findSubjectIDAndObjectIDByPredicate_Adj(predicateID, buffer);
}
Status FindEntityID::findSubjectIDAndObjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	for (; startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}
	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDAndObjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer) {

#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);

	startPtr = manager->getStartPtr();
	limit = manager->getEndPtr();

	base = startPtr - sizeof(BucketManagerMeta);

	reader = startPtr;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	ID* p;
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if (x == 0)
				break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;

			if (len != 0)
				BucketManager::readBody(x, reader + idoff, len, XTemp);
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(x);
			 XTemp->insertID(p[j]);
			 }
			 ent->empty();*/
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize)
					* BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				BucketManager::getTwo(reader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {
					//cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}
				BucketManager::readBody(x, reader + lastedgeoff,
						edgeoff + 1 - lastedgeoff, XTemp);
				lastedgeoff = edgeoff + 1;
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(x);
				 XTemp->insertID(p[j]);
				 //cout << x << " " << p[j] << "		";
				 }
				 ent->empty();*/
			}
			base += BucketManager::pagesize;
		}
		reader = base;

	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}

Status FindEntityID::findObjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findObjectIDByPredicate_XY(predicateID, buffer, minID, maxID);
	} else
		return findObjectIDByPredicate_Adj(predicateID, buffer, minID, maxID);

}
Status FindEntityID::findObjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {
	if (minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned offset;

	Status s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID,
			1, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				break;
			else if (x <= maxID)
				XTemp->insertID(x);
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}
	if (XTemp->getSize() >= 2)
		XTemp->uniqe();
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 2,
			offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			startPtr = Chunk::readYId(startPtr, y);
			if (x + y > maxID)
				break;
			else if (x + y <= maxID)
				XYTemp->insertID(x + y);
		}
	}
	if (XYTemp->getSize() >= 2)
		XYTemp->uniqe();
	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {

	if (minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(minID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;
				if (x < minID) {

				} else if (x <= maxID) {
					XTemp->insertID(x);
					//cout << x << " ";
				} else {
					break;
				}
				base += (size_t) ceil(
						(double) (len + 10) / BucketManager::pagesize)
						* BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				for (int i = 0; i < size; i++) {
					x = *(ID*) (reader + idoff);
					if (x == 0)
						break;
					idoff += sizeof(ID);
					idoff += 2;
					if (x < minID) {
						continue;
					} else if (x <= maxID) {
						XTemp->insertID(x);
						//cout << x << " ";
					} else {
						break;
					}
				}
				base += BucketManager::pagesize;
			}
			if (x > maxID)
				break;
			reader = base;
		}
	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}
Status FindEntityID::findObjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG1
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findObjectIDByPredicate_XY(predicateID, buffer);
	} else
		return findObjectIDByPredicate_Adj(predicateID, buffer);
}
Status FindEntityID::findObjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}
	XTemp->uniqe();
	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		startPtr = Chunk::readYId(startPtr, y);
		XYTemp->insertID(x + y);
	}
	XYTemp->uniqe();
	buffer->mergeBuffer(XTemp, XYTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);

	startPtr = manager->getStartPtr();
	limit = manager->getEndPtr();
	base = startPtr - sizeof(BucketManagerMeta);
	reader = startPtr;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;
	//cout<<" the oid is";
	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if (x == 0)
				break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			XTemp->insertID(x);
			//cout << x << " ";
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize)
					* BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				idoff += 2;
				XTemp->insertID(x);
				//cout << x << " ";

			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}
	cout << endl;
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << "us" << endl;
#endif
	return OK;
}

Status FindEntityID::findSubjectIDByPredicateAndObject(ID predicateID,
		ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findSubjectIDByPredicateAndObject_XY(predicateID, objectID,
				buffer);
	} else
		return findSubjectIDByPredicateAndObject_Adj(predicateID, objectID,
				buffer);
}
Status FindEntityID::findSubjectIDByPredicateAndObject_XY(ID predicateID,
		ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	Status s;
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XYTemp->setIDCount(1);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2,
			offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != objectID)
				break;
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDByPredicateAndObject_Adj(ID predicateID,
		ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(objectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == objectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, XTemp);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(p[j]);
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == objectID) {
					low = mid;
					break;
				} else if (midvalue < objectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == objectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, XTemp);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}
Status FindEntityID::findSubjectIDByPredicateAndObject(ID predicateID,
		ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findSubjectIDByPredicateAndObject_XY(predicateID, objectID,
				buffer, minID, maxID);
	} else
		return findSubjectIDByPredicateAndObject_Adj(predicateID, objectID,
				buffer, minID, maxID);
}
Status FindEntityID::findSubjectIDByPredicateAndObject_XY(ID predicateID,
		ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XYTemp->setIDCount(1);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key < minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(key);
			} else if (key > maxID) {
				break;
			}
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2,
			offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != objectID)
				break;
			if (x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDByPredicateAndObject_Adj(ID predicateID,
		ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID) {

	if (minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDByPredicateAndObject(predicateID, objectID,
				buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(objectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == objectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, minID, maxID,
							XTemp);
				/*	entsize = ent->getSize();
				 p = ent->getBuffer();

				 for (int j = 0; j < entsize; j++) {
				 if (p[j] < minID) {
				 continue;
				 } else if (p[j] <= maxID) {
				 XTemp->insertID(p[j]);

				 } else
				 break;
				 }*/

			}
		} else {
			//採用了順序查找（後續換成二分查找）
//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == objectID) {
					low = mid;
					break;
				} else if (midvalue < objectID)
					low = mid + 1;
				else
					high = mid - 1;
			}
			reader += mid * 6;
			if (midvalue == objectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, minID, maxID,
						XTemp);

			} else {
				return NOT_FOUND;
			}
			/*entsize = ent->getSize();
			 p = ent->getBuffer();

			 for (int j = 0; j < entsize; j++) {
			 //cout << p[j] << " ";
			 if (p[j] < minID) {
			 continue;
			 } else if (p[j] <= maxID) {
			 XTemp->insertID(p[j]);
			 //cout << p[j] << " ";
			 } else
			 break;
			 }*/

		}
		//ent->empty();

	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

Status FindEntityID::findObjectIDByPredicateAndSubject(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findObjectIDByPredicateAndSubject_XY(predicateID, subjectID,
				buffer, minID, maxID);
	} else
		return findObjectIDByPredicateAndSubject_Adj(predicateID, subjectID,
				buffer, minID, maxID);
}
Status FindEntityID::findObjectIDByPredicateAndSubject_XY(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	register ID x, y, key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	Status s;

	unsigned int typeID;
	unsigned int offset;

	typeID = 1;

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key < minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(x + y);
			} else {
				break;
			}
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != subjectID)
				break;
			if (x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
			} else {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDByPredicateAndSubject_Adj(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID) {

	if (minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDByPredicateAndSubject(predicateID, subjectID,
				buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(subjectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == subjectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, minID, maxID,
							XTemp);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 if (p[j] < minID) {
				 continue;
				 } else if (p[j] <= maxID) {
				 XTemp->insertID(p[j]);
				 } else
				 break;
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == subjectID) {
					low = mid;
					break;
				} else if (midvalue < subjectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == subjectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, minID, maxID,
						XTemp);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 if (p[j] < minID) {
			 continue;
			 } else if (p[j] <= maxID) {
			 XTemp->insertID(p[j]);
			 } else
			 break;
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDByPredicateAndSubject(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findObjectIDByPredicateAndSubject_XY(predicateID, subjectID,
				buffer);
	} else
		return findObjectIDByPredicateAndSubject_Adj(predicateID, subjectID,
				buffer);
}
Status FindEntityID::findObjectIDByPredicateAndSubject_XY(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	Status s;

	unsigned int typeID;
	unsigned int offset;

	typeID = 1;

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != subjectID)
				break;
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	gettimeofday(&end, NULL);
#ifdef DEBUG
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDByPredicateAndSubject_Adj(ID predicateID,
		ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(subjectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == subjectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, XTemp);
				entsize = ent->getSize();
				p = ent->getBuffer();
				for (int j = 0; j < entsize; j++) {
					XTemp->insertID(p[j]);
					//	cout << p[j] << " ";
				}
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == subjectID) {
					low = mid;
					break;
				} else if (midvalue < subjectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == subjectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, XTemp);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);

#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer, ID min, ID max, unsigned maxNum) {

#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findSubjectIDAndObjectIDByPredicate_XY(predicateID, buffer, min,
				max, maxNum);
	} else
		return findSubjectIDAndObjectIDByPredicate_Adj(predicateID, buffer, min,
				max, maxNum);
	return OK;
}
Status FindEntityID::findSubjectIDAndObjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer, ID min, ID max, unsigned maxNum) {
	if (min == 0 && max == UINT_MAX)
		return this->findSubjectIDAndObjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	unsigned offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 0);

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > max)
				goto END;

			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);
			if (XTemp->getSize() > maxNum) {
				return TOO_MUCH;
			}
		}
	}

	END: typeIDMin = 2;

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);
		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > max)
				goto END1;

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);
			if (XYTemp->getSize() > maxNum) {
				return TOO_MUCH;
			}
		}
	}
	END1: buffer->mergeBuffer(XTemp, XYTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDAndObjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
	if (minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDAndObjectIDByPredicate(predicateID, buffer);

#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(minID, offset);

	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;
		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;

				if (x < minID) {

				} else if (x <= maxID) {
					if (len != 0)
						BucketManager::readBody(x, reader + idoff, len, XTemp);
					/*entsize = ent->getSize();
					 p = ent->getBuffer();
					 for (int j = 0; j < entsize; j++) {
					 XTemp->insertID(x);
					 XTemp->insertID(p[j]);
					 }
					 ent->empty();*/
				} else {
					break;
				}

				base += (size_t) ceil(
						(double) (len + 10) / BucketManager::pagesize)
						* BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				for (int i = 0; i < size; i++) {
					x = *(ID*) (reader + idoff);
					if (x == 0)
						break;
					idoff += sizeof(ID);
					BucketManager::getTwo(reader + idoff, edgeoff);
					if (edgeoff + 1 <= lastedgeoff) {
						exit(-1);
					}
					idoff += 2;
					if (x < minID) {

					} else if (x <= maxID) {
						BucketManager::readBody(x, reader + lastedgeoff,
								edgeoff + 1 - lastedgeoff, XTemp);
						/*entsize = ent->getSize();
						 p = ent->getBuffer();
						 for (int j = 0; j < entsize; j++) {
						 XTemp->insertID(x);
						 XTemp->insertID(p[j]);
						 }
						 ent->empty();*/
					} else {
						break;
					}
					lastedgeoff = edgeoff + 1;
				}

				base += BucketManager::pagesize;
			}
			if (x > maxID)
				break;
			reader = base;
		}
	}
	buffer->operator =(XTemp);

#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << "us" << endl;
#endif
	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findObjectIDAndSubjectIDByPredicate_XY(predicateID, buffer,
				minID, maxID);
	} else
		return findObjectIDAndSubjectIDByPredicate_Adj(predicateID, buffer,
				minID, maxID);
}
Status FindEntityID::findObjectIDAndSubjectIDByPredicate_XY(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {
	if (minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDAndSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	unsigned offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 1);

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID) {

				goto END;
			}
			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);

			if (XTemp->getSize() > maxNum) {
				return TOO_MUCH;
			}
		}
	}

	END: typeIDMin = 2;

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > maxID) {

				goto END1;
			}

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);

			if (XYTemp->getSize() > maxNum) {
				return TOO_MUCH;
			}
		}
	}
	END1: buffer->mergeBuffer(XTemp, XYTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDAndSubjectIDByPredicate_Adj(ID predicateID,
		EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum) {

	if (minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDAndSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(minID, offset);
	int size = 0, entsize = 0;
	ID* p;
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;

		int len, idoff, edgeoff, lastedgeoff;

		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;

				if (x < minID) {

				} else if (x <= maxID) {
					if (len != 0)
						BucketManager::readBody(x, reader + idoff, len, XTemp);

				} else {
					break;
				}

				base += (size_t) ceil(
						(double) (len + 10) / BucketManager::pagesize)
						* BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				for (int i = 0; i < size; i++) {
					x = *(ID*) (reader + idoff);
					if (x == 0)
						break;
					idoff += sizeof(ID);
					BucketManager::getTwo(reader + idoff, edgeoff);
					if (edgeoff + 1 <= lastedgeoff) {
						exit(-1);
					}
					idoff += 2;
					if (x < minID) {

					} else if (x <= maxID) {
						BucketManager::readBody(x, reader + lastedgeoff,
								edgeoff + 1 - lastedgeoff, XTemp);

					} else {
						break;
					}
					lastedgeoff = edgeoff + 1;

				}
				base += BucketManager::pagesize;
			}
			if (x > maxID)
				break;
			reader = base;
		}
	}

	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findSubjectIDAndPredicateIDByPredicateAndObject_XY(predicateID,
				objectID, buffer);
	} else
		return findSubjectIDAndPredicateIDByPredicateAndObject_Adj(predicateID,
				objectID, buffer);
}
Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject_XY(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			XYTemp->insertID(key);
			XYTemp->insertID(predicateID);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != objectID)
				break;
			XTemp->insertID(x);
			XTemp->insertID(predicateID);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject_Adj(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(objectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == objectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, XTemp,
							predicateID);

				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(p[j]);
				 XTemp->insertID(predicateID);
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == objectID) {
					low = mid;
					break;
				} else if (midvalue < objectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == objectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, XTemp,
						predicateID);

			} else
				return NOT_FOUND;

			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 XTemp->insertID(predicateID);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject(
		ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findSubjectIDAndPredicateIDByPredicateAndObject_XY(predicateID,
				objectID, buffer, minID, maxID);
	} else
		return findSubjectIDAndPredicateIDByPredicateAndObject_Adj(predicateID,
				objectID, buffer, minID, maxID);
}
Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject_XY(
		ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG
//	cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key < minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(key);
				XYTemp->insertID(predicateID);
			} else if (key > maxID) {
				break;
			}
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != objectID)
				break;
			if (x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject_Adj(
		ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(objectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == objectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, minID, maxID,
							XTemp, predicateID);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 if (p[j] < minID) {
				 continue;
				 } else if (p[j] <= maxID) {
				 XTemp->insertID(p[j]);
				 XTemp->insertID(predicateID);

				 } else
				 break;
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == objectID) {
					low = mid;
					break;
				} else if (midvalue < objectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == objectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, minID, maxID,
						XTemp, predicateID);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 if (p[j] < minID) {
			 continue;
			 } else if (p[j] <= maxID) {
			 XTemp->insertID(p[j]);
			 XTemp->insertID(predicateID);
			 //cout << p[j] << " " << predicateID << "		";
			 } else
			 break;
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObject(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[1];
	if (way == 0) {
		return findPredicateIDAndSubjectIDByPredicateAndObject_XY(predicateID,
				objectID, buffer);
	} else
		return findPredicateIDAndSubjectIDByPredicateAndObject_Adj(predicateID,
				objectID, buffer);
}
Status FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObject_XY(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			XYTemp->insertID(predicateID);
			XYTemp->insertID(key);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2,
			offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y != objectID)
				break;
			XTemp->insertID(predicateID);
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObject_Adj(
		ID predicateID, ID objectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 1);
	unsigned offset;
	Status s = manager->getChunkPosByID(objectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == objectID) {
				if (len != 0)
					BucketManager::readBody(predicateID, reader + idoff, len,
							XTemp);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(predicateID);
				 XTemp->insertID(p[j]);
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == objectID) {
					low = mid;
					break;
				} else if (midvalue < objectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == objectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(predicateID, startPtr + offset, len,
						XTemp);

			} else
				return NOT_FOUND;
			/*	entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(predicateID);
			 XTemp->insertID(p[j]);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}

Status FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubject(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findPredicateIDAndObjectIDByPredicateAndSubject_XY(predicateID,
				subjectID, buffer);
	} else
		return findPredicateIDAndObjectIDByPredicateAndSubject_Adj(predicateID,
				subjectID, buffer);
}
Status FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubject_XY(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(predicateID);
			XYTemp->insertID(x + y);
			//cout << "******" << predicateID << "	" << x + y << endl;
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if (x + y != subjectID)
				break;
			XTemp->insertID(predicateID);
			XTemp->insertID(x);
			//cout << "******" << predicateID << "	" << x << endl;
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubject_Adj(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif

	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(subjectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == subjectID) {
				if (len != 0)
					BucketManager::readBody(predicateID, reader + idoff, len,
							XTemp);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 XTemp->insertID(predicateID);
				 XTemp->insertID(p[j]);
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == subjectID) {
					low = mid;
					break;
				} else if (midvalue < subjectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == subjectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(predicateID, startPtr + offset, len,
						XTemp);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(predicateID);
			 XTemp->insertID(p[j]);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;

}

Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findObjectIDAndPredicateIDByPredicateAndSubject_XY(predicateID,
				subjectID, buffer);
	} else
		return findObjectIDAndPredicateIDByPredicateAndSubject_Adj(predicateID,
				subjectID, buffer);
}
Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject_XY(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
			XYTemp->insertID(predicateID);
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if (x + y != subjectID)
				break;
			XTemp->insertID(x);
			XTemp->insertID(predicateID);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject_Adj(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(subjectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == subjectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, XTemp,
							predicateID);
				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {

				 XTemp->insertID(p[j]);
				 XTemp->insertID(predicateID);
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == subjectID) {
					low = mid;
					break;
				} else if (midvalue < subjectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == subjectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, XTemp,
						predicateID);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 XTemp->insertID(predicateID);
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG1
	cout << __FUNCTION__ << endl;
#endif
	unsigned *array = (unsigned *) wayfile->getBuffer();
	unsigned way = array[0];
	if (way == 0) {
		return findObjectIDAndPredicateIDByPredicateAndSubject_XY(predicateID,
				subjectID, buffer, minID, maxID);
	} else
		return findObjectIDAndPredicateIDByPredicateAndSubject_Adj(predicateID,
				subjectID, buffer, minID, maxID);
}
Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject_XY(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	register ID x, y, key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if (x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key < minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(key);
				XYTemp->insertID(predicateID);
			} else if (key > maxID) {
				break;
			}
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID,
			typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2)
				+ offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if (x + y != subjectID)
				break;
			if (x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}
Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject_Adj(
		ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID,
		ID maxID) {
#ifdef DEBUG
	//cout << __FUNCTION__ << endl;
	cerr << __FUNCTION__ << endl;
#endif
	struct timeval start, end;
	gettimeofday(&start, NULL);
	XTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);

	register ID x;
	const uchar* startPtr, *limit, *reader, *base;
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	BucketManager* manager = bitmap->getBucketManager(predicateID, 0);
	unsigned offset;
	Status s = manager->getChunkPosByID(subjectID, offset);
	if (s == OK) {
		startPtr = manager->getStartPtr() + offset;
		limit = manager->getEndPtr();
		if (offset == 0) {
			base = startPtr - sizeof(BucketManagerMeta);
		} else {
			base = startPtr;
		}
		reader = startPtr;
		int size = 0, entsize = 0;
		int len, idoff, edgeoff, lastedgeoff;
		ID* p;

		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (x == subjectID) {
				if (len != 0)
					BucketManager::readBody(reader + idoff, len, minID, maxID,
							XTemp, predicateID);

				/*entsize = ent->getSize();
				 p = ent->getBuffer();
				 for (int j = 0; j < entsize; j++) {
				 if (p[j] < minID)
				 continue;
				 else if (p[j] <= maxID) {
				 XTemp->insertID(p[j]);
				 XTemp->insertID(predicateID);
				 } else
				 break;
				 }*/
			}
		} else {
			//採用了順序查找（後續換成二分查找）
			/*idoff = 2;
			 lastedgeoff = 2 + size * (2 + sizeof(ID));
			 for (int i = 0; i < size; i++) {

			 x = *(ID*) (reader + idoff);
			 if (x == 0)
			 break;
			 idoff += sizeof(ID);
			 BucketManager::getTwo(reader + idoff, edgeoff);
			 if (edgeoff + 1 <= lastedgeoff) {
			 cout << edgeoff + 1 << "  " << lastedgeoff << endl;
			 exit(-1);
			 }
			 idoff += 2;
			 if (x == objectID) {
			 BucketManager::readBody(reader + lastedgeoff,
			 edgeoff + 1 - lastedgeoff, ent);
			 lastedgeoff = edgeoff + 1;
			 entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 XTemp->insertID(p[j]);
			 }
			 break;
			 }
			 }*/
			//採用二分查找
			offset = 0;
			reader += 2;
			int low, high, mid;
			ID midvalue;
			low = 0;
			high = size - 1;
			while (low <= high) {
				mid = (low + high) / 2;
				midvalue = *(ID*) (reader + mid * 6);
				if (midvalue == subjectID) {
					low = mid;
					break;
				} else if (midvalue < subjectID)
					low = mid + 1;
				else
					high = mid - 1;
			}

			reader += mid * 6;
			if (midvalue == subjectID) {
				BucketManager::getTwo((reader + 4), len);
				if (mid != 0) {
					BucketManager::getTwo((reader - 2), edgeoff);
					offset = offset + edgeoff + 1;
					len = len - edgeoff;
				} else {
					edgeoff = 2 + size * 6;
					offset = offset + edgeoff;
					len = len + 1 - edgeoff;
				}
				BucketManager::readBody(startPtr + offset, len, minID, maxID,
						XTemp, predicateID);

			} else
				return NOT_FOUND;
			/*entsize = ent->getSize();
			 p = ent->getBuffer();
			 for (int j = 0; j < entsize; j++) {
			 if (p[j] < minID)
			 continue;
			 else if (p[j] <= maxID) {
			 XTemp->insertID(p[j]);
			 XTemp->insertID(predicateID);
			 } else
			 break;
			 }*/
		}

		//ent->empty();

	}
	buffer->operator =(XTemp);
#ifdef DEBUG
	gettimeofday(&end, NULL);
	cerr << " time elapsed: "
			<< ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec
					- start.tv_usec) << " us" << endl;
#endif
	return OK;
}

void FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask(
		ID predicateID, ID objectID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findSubjectIDAndPredicateIDByPredicateAndObject(predicateID, objectID,
			curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask(
		ID predicateID, ID objectID, ID minID, ID maxID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findSubjectIDAndPredicateIDByPredicateAndObject(predicateID, objectID,
			curBuffer, minID, maxID);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObjectTask(
		ID predicateID, ID objectID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findPredicateIDAndSubjectIDByPredicateAndObject(predicateID, objectID,
			curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask(
		ID predicateID, ID subjectID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findObjectIDAndPredicateIDByPredicateAndSubject(predicateID,
			subjectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask(
		ID predicateID, ID subjectID, ID minID, ID maxID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findObjectIDAndPredicateIDByPredicateAndSubject(predicateID,
			subjectID, curBuffer, minID, maxID);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubjectTask(
		ID predicateID, ID subjectID) {

	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findPredicateIDAndObjectIDByPredicateAndSubject(predicateID,
			subjectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID,
		EntityIDBuffer* buffer) {

	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findSubjectIDAndPredicateIDByPredicateAndObject(preBuffer[i],
				objectID, tempBuffer1);
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask,
						this, preBuffer[i], objectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {

	if (minID == 0 && maxID == UINT_MAX)
		return findSubjectIDAndPredicateIDByObject(objectID, buffer);

	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findSubjectIDAndPredicateIDByPredicateAndObject(preBuffer[i],
				objectID, tempBuffer1, minID, maxID);
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask,
						this, preBuffer[i], objectID, minID, maxID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByObject(ID objectID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findPredicateIDAndSubjectIDByPredicateAndObject(preBuffer[i],
				objectID, tempBuffer1);
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObjectTask,
						this, preBuffer[i], objectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID,
		EntityIDBuffer *buffer) {

	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findObjectIDAndPredicateIDByPredicateAndSubject(preBuffer[i],
				subjectID, tempBuffer1);
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask,
						this, preBuffer[i], subjectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID,
		EntityIDBuffer *buffer, ID minID, ID maxID) {

	if (minID == 0 && maxID == UINT_MAX)
		return findObjectIDAndPredicateIDBySubject(subjectID, buffer);

	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findObjectIDAndPredicateIDByPredicateAndSubject(preBuffer[i],
				subjectID, tempBuffer1, minID, maxID);
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask,
						this, preBuffer[i], subjectID, minID, maxID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findPredicateIDAndObjectIDBySubject(ID subjectID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {

	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize(), i = 0;
	if (size == 0)
		return NOT_FOUND;
	/*cout << " presize is" << size << "	" << preBuffer[0] << "	" << preBuffer[1]
			<< endl;*/
	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do {
		this->findPredicateIDAndObjectIDByPredicateAndSubject(preBuffer[i],
				subjectID, tempBuffer1);
		//cout<<"+++++++++++tempbuffer1 size is "<<tempBuffer1->getSize()<<" and s:p is "<<subjectID<<":"<<preBuffer[i]<<endl;
		i++;
	} while (tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(
						&FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubjectTask,
						this, preBuffer[i], subjectID));
		//cout<<" and s:p is "<<subjectID<<":"<<preBuffer[i]<<endl;

	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	/*ID *te = buffer->getBuffer();
	for (int j = 0; j < buffer->getSize(); j++) {
		cout << " and s:p:o is " << subjectID << ":" << te[j * 2] << ":"
				<< te[j * 2 + 1] << endl;
	}*/
	return OK;
}

Status FindEntityID::findPredicateIDBySubjectAndObject(ID subjectID,
		ID objectID, EntityIDBuffer* buffer) {

	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s;
	if ((s = spStatBuffer->getPredicatesByID(subjectID, XTemp, 0, UINT_MAX))
			!= OK)
		return s;
	if ((s = opStatBuffer->getPredicatesByID(objectID, XYTemp, 0, UINT_MAX))
			!= OK)
		return s;

	SortMergeJoin join;
	join.Join(XTemp, XYTemp, 1, 1, false);
	buffer = XTemp;

	return s;
}

Status FindEntityID::findPredicateIDByObject(ID objectID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s = opStatBuffer->getPredicatesByID(objectID, buffer, minID, maxID);
	return s;
}

Status FindEntityID::findPredicateIDBySubject(ID subjectID,
		EntityIDBuffer* buffer, ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);//一开始是无序，后续才游戏
	return spStatBuffer->getPredicatesByID(subjectID, buffer, minID, maxID);
}

Status FindEntityID::findObjectIDBySubject(ID subjectID,
		EntityIDBuffer *buffer) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	if (size == 0)
		return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,
				entBuffer);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,
				curBuffer);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}

	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer,
		ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	if (size == 0)
		return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,
				entBuffer, minID, maxID);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,
				curBuffer, minID, maxID);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}
	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID,
		EntityIDBuffer* buffer) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	if (size == 0)
		return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,
				entBuffer);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,
				curBuffer);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}

	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID, EntityIDBuffer* buffer,
		ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;

	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);

	size_t size = preBuffer.getSize();
	if (size == 0)
		return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,
				entBuffer, minID, maxID);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,
				curBuffer, minID, maxID);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}
	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findSubject(EntityIDBuffer* buffer, ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);

//return sStatBuffer->getIDs(buffer, minID, maxID);
	return sStatBuffer->getEntityIDs(buffer, minID, maxID);
}

/**
 * the predicate's id is continuous.
 */
Status FindEntityID::findPredicate(EntityIDBuffer* buffer, ID minID, ID maxID) {

	for (ID id = minID; id <= maxID; id++)
		buffer->insertID(id);
	return OK;
}

Status FindEntityID::findObject(EntityIDBuffer* buffer, ID minID, ID maxID) {

	buffer->setIDCount(1);
	buffer->setSortKey(0);

//return oStatBuffer->getIDs(buffer, minID, maxID);
	return oStatBuffer->getEntityIDs(buffer, minID, maxID);
}

//FINDSOBYNONE
Status FindEntityID::findSOByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSOByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSOByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findSOByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();
	}
	return OK;
}

Status FindEntityID::findSOByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}

	return OK;
}

//FINDOSBYNONE
Status FindEntityID::findOSByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOSByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOSByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findOSByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();
	}
	return OK;
}

Status FindEntityID::findOSByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}

	return OK;
}

//FINDSPBYNONE
Status FindEntityID::findSPByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSPByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSPByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findSPByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
#ifndef DEBUG
		cout << "before sort" << endl;
		int s = buffer->getSize();
		int co = buffer->getIDCount();
		for (int j = 0; j < s; j++) {

			for (int i = 0; i < co; i++) {
				cout << (buffer->getBuffer())[j * co + i] << "	";
			}
			cout << endl;
		}
#endif
		buffer->sort(1);
#ifndef DEBUG
		cout << "after sort" << endl;
		for (int j = 0; j < s; j++) {

			for (int i = 0; i < co; i++) {
				cout << (buffer->getBuffer())[j * co + i] << "	";
			}
			cout << endl;
		}
#endif
		buffer->uniqe();
	}

	return OK;
}

Status FindEntityID::findSPByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == PREDICATE) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

//FINDPSBYNONE
Status FindEntityID::findPSByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findPSByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findPSByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findPSByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();
	}
	return OK;
}

Status FindEntityID::findPSByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == PREDICATE) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

//FINDOPBYNONE
Status FindEntityID::findOPByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOPByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOPByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findOPByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();
	}
	return OK;
}

Status FindEntityID::findOPByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == PREDICATE) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

//FINDPOBYNONE
Status FindEntityID::findPOByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}

		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findPOByKnowBufferTask,
									this, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findPOByKnowBufferTask,
									this, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findPOByKnowBufferTask, this,
								p, 1, knowElement, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();
	}
	return OK;
}
Status FindEntityID::findSPOByKnowBuffer(EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) //wonder
		{
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	//buffer->setIDCount(3);
	//buffer->setSortKey(1); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		//得到已知knowbuffer的缩减版
		for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
			key = *(p + i * IDCount + sortKey);
			if (key > lastKey) {
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		p = transBuffer.getBuffer();
		findSPOByKnowBufferTask(p, size, knowElement, buffer);
		//buffer->uniqe();
		//并行里又有并行，不能运行
		/*
		 size_t size = transBuffer.getSize();
		 unsigned chunkCount = THREAD_NUMBER;
		 size_t chunkSize = size / chunkCount;
		 size_t chunkLeft = size % chunkCount;
		 p = transBuffer.getBuffer();
		 if (chunkSize) {
		 EntityIDBuffer tempBuffer[chunkCount];

		 for (unsigned i = 0; i < chunkCount; i++) {
		 if (i < chunkLeft) {
		 CThreadPool::getInstance().AddTask(
		 boost::bind(&FindEntityID::findSPOByKnowBufferTask,
		 this, p, chunkSize + 1, knowElement,
		 &tempBuffer[i]));
		 p += (chunkSize + 1);
		 } else {
		 CThreadPool::getInstance().AddTask(
		 boost::bind(&FindEntityID::findSPOByKnowBufferTask,
		 this, p, chunkSize + 1, knowElement,
		 &tempBuffer[i]));
		 p += chunkSize;
		 }

		 }
		 cout<<"start to  wait"<<endl;
		 CThreadPool::getInstance().Wait();
		 cout<<" wait....over"<<endl;
		 for (unsigned i = 0; i < chunkCount; i++) {
		 cout<<"append tempbuffer"<<i<<endl;
		 buffer->appendBuffer(&tempBuffer[i]);
		 }
		 } else {
		 EntityIDBuffer tempBuffer[chunkLeft];
		 for (unsigned i = 0; i < chunkLeft; i++) {
		 CThreadPool::getInstance().AddTask(
		 boost::bind(&FindEntityID::findSPOByKnowBufferTask, this,
		 p, 1, knowElement, &tempBuffer[i]));
		 p += 1;
		 }
		 CThreadPool::getInstance().Wait();
		 for (unsigned i = 0; i < chunkLeft; i++) {
		 buffer->appendBuffer(&tempBuffer[i]);
		 }
		 }*/
		//buffer->sort(1);
	}
	//cout<<" wait sync over"<<endl;
	return OK;
}

Status FindEntityID::findPOByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == PREDICATE) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}
Status FindEntityID::findSPOByKnowBufferTask(ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) //wonder
		{
	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(3);
	//resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDAndObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);

			unsigned tempSize = tempBuffer.getSize();
			ID *q = tempBuffer.getBuffer();
			/*cout << "tempbuffsize is " << tempSize << " and idcount is"
					<< tempBuffer.getIDCount() << endl;*/
			for (j = 0; j < tempSize; j++) {
				//cout << key << "	";
				resultBuffer->insertID(key);
				resultBuffer->insertID(q[j * 2]);
				//cout << q[j * 2] << "	";
				resultBuffer->insertID(q[j * 2 + 1]);
				//cout << q[j * 2 + 1] << endl;

			}
			tempBuffer.empty();
		}

	} else if (knowElement == PREDICATE) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDAndObjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j * 2]);
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j * 2 + 1]);
			}
			tempBuffer.empty();
		}

	} else if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDAndObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j * 2]);
				resultBuffer->insertID((tempBuffer)[j * 2 + 1]);
				resultBuffer->insertID(key);
			}
			tempBuffer.empty();
		}

	}
	/*cout << "before merge" << endl;
	int s = resultBuffer->getSize();
	int co = resultBuffer->getIDCount();
	for (int j = 0; j < s; j++) {

		for (int i = 0; i < co; i++) {
			cout << (resultBuffer->getBuffer())[j * co + i] << "	";
		}
		cout << endl;
	}*/
	return OK;
}

Status FindEntityID::findSOByKnowBuffer(ID preID, EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		if (IDCount > 1) {
			for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
				key = *(p + i * IDCount + sortKey);
				if (key > lastKey) {
					transBuffer.insertID(key);
					lastKey = key;
				}
			}
		} else {
			transBuffer = (knowBuffer);
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSOByKnowBufferTask1,
									this, preID, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSOByKnowBufferTask1,
									this, preID, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findSOByKnowBufferTask1,
								this, preID, p, 1, knowElement,
								&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();

	}
	return OK;
}

Status FindEntityID::findSOByKnowBufferTask1(ID preID, ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicateAndSubject(preID, key, &tempBuffer, 0,
			UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicateAndObject(preID, key, &tempBuffer, 0,
			UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

Status FindEntityID::findOSByKnowBuffer(ID preID, EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (knowBuffer->sorted == false)
		return ERROR;
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		if (IDCount > 1) {
			for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
				key = *(p + i * IDCount + sortKey);
				if (key > lastKey) {
					transBuffer.insertID(key);
					lastKey = key;
				}
			}
		} else {
			transBuffer = (knowBuffer);
		}

		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOSByKnowBufferTask1,
									this, preID, p, chunkSize + 1, knowElement,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findOSByKnowBufferTask1,
									this, preID, p, chunkSize, knowElement,
									&tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findOSByKnowBufferTask1,
								this, preID, p, 1, knowElement,
								&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();

	}
	return OK;
}

Status FindEntityID::findOSByKnowBufferTask1(ID preID, ID* p, size_t length,
		EntityType knowElement, EntityIDBuffer* resultBuffer) {

	if (length == 0)
		return OK;
	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == OBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicateAndObject(preID, key, &tempBuffer, 0,
			UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	} else if (knowElement == SUBJECT) {
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicateAndSubject(preID, key, &tempBuffer, 0,
			UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

Status FindEntityID::findSByKnowBuffer(ID preID, EntityIDBuffer* buffer,
		EntityIDBuffer* knowBuffer, EntityType knowElement) {
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	buffer->setIDCount(2);
	buffer->setSortKey(10); //unsorted
	if (!knowBuffer->sorted) {
		return ERROR;
	}
	if (knowBuffer->getSize() == 0) {
		return ERROR;
	}
	if (knowElement == SUBJECT) {
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		if (IDCount > 1) {
			for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
				key = *(p + i * IDCount + sortKey);
				if (key > lastKey) {
					buffer->insertID(key);
					lastKey = key;
				}
			}
		} else {
			(*buffer) = (knowBuffer);
		}

		return ERROR;
	} else {
		EntityIDBuffer transBuffer;
		ID key = 0, lastKey = 0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		if (IDCount > 1) {
			for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
				key = *(p + i * IDCount + sortKey);
				if (key > lastKey) {
					transBuffer.insertID(key);
					lastKey = key;
				}
			}
		} else {
			transBuffer = (knowBuffer);
		}
		transBuffer.print();
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if (chunkSize) {
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft) {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSByKnowBufferTask1,
									this, preID, p, chunkSize + 1,
									&tempBuffer[i]));
					p += (chunkSize + 1);
				} else {
					CThreadPool::getInstance().AddTask(
							boost::bind(&FindEntityID::findSByKnowBufferTask1,
									this, preID, p, chunkSize, &tempBuffer[i]));
					p += chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkCount; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		} else {
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(
						boost::bind(&FindEntityID::findSByKnowBufferTask1, this,
								preID, p, 1, &tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for (unsigned i = 0; i < chunkLeft; i++) {
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
		buffer->uniqe();

	}
	return OK;
}
Status FindEntityID::findSByKnowBufferTask1(ID preID, ID* p, size_t length,
		EntityIDBuffer* resultBuffer) {

	EntityIDBuffer tempBuffer;
	unsigned i = 0, j = 0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	for (i = 0; i < length; i++) {
		ID key = *(p + i);
		findSubjectIDByPredicateAndObject(preID, key, &tempBuffer, 0, UINT_MAX);
		unsigned tempSize = tempBuffer.getSize();
		for (j = 0; j < tempSize; j++) {
			resultBuffer->insertID((tempBuffer)[j]);
		}
	}
	return OK;
}
