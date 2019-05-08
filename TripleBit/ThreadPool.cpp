/*
 * ThreadPool.cpp
 *
 *  Created on: 2010-6-18
 *      Author: liupu
 */

#include "ThreadPool.h"
#include "TripleBit.h"
#include <string>
#include <iostream>

using namespace std;
CThreadPool* CThreadPool::instance = NULL;
CThreadPool* CThreadPool::instance1 = NULL;
CThreadPool *CThreadPool::workPool = NULL;
CThreadPool *CThreadPool::chunkPool = NULL;
CThreadPool *CThreadPool::partitionPool = NULL;
CThreadPool *CThreadPool::printPool = NULL;
CThreadPool *CThreadPool::scan_joinPool = NULL;
CThreadPool *CThreadPool::fullJoinInstance = NULL;
CThreadPool *CThreadPool::star_para = NULL;
CThreadPool *CThreadPool::joinpara1= NULL;
CThreadPool *CThreadPool::joinpara2 = NULL;
CThreadPool::CThreadPool(int threadNum) {
	this->threadNum = threadNum;
	shutDown = false;
	NodeTask *nodeTask = new NodeTask;
	head = tail = nodeTask;
	pthread_mutex_init(&headMutex, NULL);
	pthread_mutex_init(&tailMutex, NULL);
	pthread_mutex_init(&pthreadIdleMutex, NULL);
	pthread_mutex_init(&pthreadBusyMutex, NULL);
	pthread_cond_init(&pthreadCond, NULL);
	pthread_cond_init(&pthreadEmpty, NULL);
	pthread_cond_init(&pthreadBusyEmpty, NULL);
	create();
}

CThreadPool::~CThreadPool() {
	stopAll();
	if (head != NULL) {
		delete head;
		head = NULL;
	}
//	if(tail != NULL){
//		delete tail;
//		tail = NULL;
//	}
	pthread_mutex_destroy(&headMutex);
	pthread_mutex_destroy(&tailMutex);
	pthread_mutex_destroy(&pthreadIdleMutex);
	pthread_mutex_destroy(&pthreadBusyMutex);
	pthread_cond_destroy(&pthreadCond);
	pthread_cond_destroy(&pthreadEmpty);
	pthread_cond_destroy(&pthreadBusyEmpty);
}

int CThreadPool::moveToIdle(pthread_t tid) {
	pthread_mutex_lock(&pthreadBusyMutex);
	vector<pthread_t>::iterator busyIter = vecBusyThread.begin();
	while (busyIter != vecBusyThread.end()) {
		if (tid == *busyIter) {
			break;
		}
		busyIter++;
	}
	vecBusyThread.erase(busyIter);

	if (vecBusyThread.size() == 0) {
		pthread_cond_broadcast(&pthreadBusyEmpty); //多个线程阻塞在条件变量上。
	}

	pthread_mutex_unlock(&pthreadBusyMutex);

	pthread_mutex_lock(&pthreadIdleMutex);
	vecIdleThread.push_back(tid);
	pthread_mutex_unlock(&pthreadIdleMutex);
	return 0;
}

int CThreadPool::moveToBusy(pthread_t tid) {
	pthread_mutex_lock(&pthreadIdleMutex);
	vector<pthread_t>::iterator idleIter = vecIdleThread.begin();
	while (idleIter != vecIdleThread.end()) {
		if (tid == *idleIter) {
			break;
		}
		idleIter++;
	}
	vecIdleThread.erase(idleIter);
	pthread_mutex_unlock(&pthreadIdleMutex);

	pthread_mutex_lock(&pthreadBusyMutex);
	vecBusyThread.push_back(tid);
	pthread_mutex_unlock(&pthreadBusyMutex);
	return 0;
}

void* CThreadPool::threadFunc(void * threadData) {
	pthread_t tid = pthread_self();
	int rnt;
	ThreadPoolArg* arg = (ThreadPoolArg*) threadData;
	CThreadPool* pool = arg->pool;
	while (1) {
		rnt = pthread_mutex_lock(&pool->headMutex);
		if (rnt != 0) {
			cout << "Get mutex error" << endl;
		}

		while (pool->isEmpty() && pool->shutDown == false) {
			pthread_cond_wait(&pool->pthreadCond, &pool->headMutex);
		}

		if (pool->shutDown == true) {
			pthread_mutex_unlock(&pool->headMutex);
			pthread_exit(NULL);
		}

		pool->moveToBusy(tid);
		Task task = pool->Dequeue();

		if (pool->isEmpty()) {
			pthread_cond_broadcast(&pool->pthreadEmpty);
		}
		pthread_mutex_unlock(&pool->headMutex);
		task();
		pool->moveToIdle(tid);
	}
	return (void*) 0;
}

void CThreadPool::Enqueue(const Task &task) {
	NodeTask *nodeTask = new NodeTask(task);
	pthread_mutex_lock(&tailMutex);
	tail->next = nodeTask;
	tail = nodeTask;
	pthread_mutex_unlock(&tailMutex);
}

CThreadPool::Task CThreadPool::Dequeue() {
	NodeTask *node, *newNode;
//	pthread_mutex_lock(&headMutex);
	node = head;
	newNode = head->next;
	Task task = newNode->value;
	head = newNode;
//	pthread_mutex_unlock(&headMutex);
	delete node;
	return task;
}

int CThreadPool::AddTask(const Task &task) {
	Enqueue(task);
	pthread_cond_broadcast(&pthreadCond);

	return 0;
}

int CThreadPool::create() {
	struct ThreadPoolArg* arg = new ThreadPoolArg;
	pthread_mutex_lock(&pthreadIdleMutex);
	for (int i = 0; i < threadNum; i++) {
		pthread_t tid = 0;
		arg->pool = this;
		pthread_create(&tid, NULL, threadFunc, arg);
		vecIdleThread.push_back(tid);
	}
	pthread_mutex_unlock(&pthreadIdleMutex);
	//delete arg;
	return 0;
}

int CThreadPool::stopAll() {
	shutDown = true;
	pthread_mutex_unlock(&headMutex);
	pthread_cond_broadcast(&pthreadCond);
	vector<pthread_t>::iterator iter = vecIdleThread.begin();
	while (iter != vecIdleThread.end()) {
		pthread_join(*iter, NULL);
		iter++;
	}

	iter = vecBusyThread.begin();
	while (iter != vecBusyThread.end()) {
		pthread_join(*iter, NULL); //阻塞线程直到指定线程终止
		iter++;
	}

	return 0;
}

int CThreadPool::Wait() {
	pthread_mutex_lock(&headMutex);
	while (!isEmpty()) {
		pthread_cond_wait(&pthreadEmpty, &headMutex);
	}
	pthread_mutex_unlock(&headMutex);
	pthread_mutex_lock(&pthreadBusyMutex);
	while (vecBusyThread.size() != 0) {
		pthread_cond_wait(&pthreadBusyEmpty, &pthreadBusyMutex); //阻塞线程直到条件受信
	}
	pthread_mutex_unlock(&pthreadBusyMutex);
	return 0;
}
CThreadPool& CThreadPool::getInstance() {
	if (instance == NULL) {
		instance = new CThreadPool(WORK_THREAD_NUMBER);
	}
	return *instance;
}
CThreadPool& CThreadPool::getInstance1() {
	if (instance1 == NULL) {
		instance1 = new CThreadPool(WORK_THREAD_NUMBER);
		//instance1 = new CThreadPool(1);
	}
	return *instance1;
}
CThreadPool &CThreadPool::getWorkPool() {
	if (workPool == NULL) {
		workPool = new CThreadPool(WORK_THREAD_NUMBER);
	}
	return *workPool;
}

CThreadPool &CThreadPool::getChunkPool() {
	if (chunkPool == NULL) {
		chunkPool = new CThreadPool(CHUNK_THREAD_NUMBER);
	}
	return *chunkPool;
}

CThreadPool &CThreadPool::getPartitionPool() {
	if (partitionPool == NULL) {
		partitionPool = new CThreadPool(CHUNK_THREAD_NUMBER);
	}
	return *partitionPool;
}
CThreadPool &CThreadPool::getPrintPool() {
	if (printPool == NULL) {
		printPool = new CThreadPool(WORK_THREAD_NUMBER);
	}
	return *printPool;
}
CThreadPool &CThreadPool::getScan_JoinPool() {
	if (scan_joinPool == NULL) {
		scan_joinPool = new CThreadPool(WORK_THREAD_NUMBER);
	}
	return *scan_joinPool;
}
CThreadPool& CThreadPool::getFullJoinInstance() {
	if (fullJoinInstance == NULL) {
		fullJoinInstance = new CThreadPool(WORK_THREAD_NUMBER);
	}
	return *fullJoinInstance;
}
CThreadPool& CThreadPool::getStar_para() {
	if (star_para == NULL) {
		star_para = new CThreadPool(3);
	}
	return *star_para;
}
CThreadPool& CThreadPool::getjoinpara1() {
	if (joinpara1 == NULL) {
		joinpara1 = new CThreadPool(3);
	}
	return *joinpara1;
}
CThreadPool& CThreadPool::getjoinpara2() {
	if (joinpara2 == NULL) {
		joinpara2 = new CThreadPool(3);
	}
	return *joinpara2;
}
void CThreadPool::createAllPool() {
	//getWorkPool();
	getChunkPool();
	getPartitionPool();
	getPrintPool();
	//getScan_JoinPool();
	//getStar_para();
}
void CThreadPool::deleteReadPool() {
	if (chunkPool != NULL) {
		delete chunkPool;
		chunkPool = NULL;
	}
	if (partitionPool != NULL) {
		delete partitionPool;
		partitionPool = NULL;
	}
}
void CThreadPool::deleteAllPool() {
	if (workPool != NULL) {
		delete workPool;
		workPool = NULL;
	}
	if (chunkPool != NULL) {
		delete chunkPool;
		chunkPool = NULL;
	}
	if (partitionPool != NULL) {
		delete partitionPool;
		partitionPool = NULL;
	}
	if (printPool != NULL) {
		delete printPool;
		printPool = NULL;
	}
	if (instance != NULL) {
		delete instance;
		instance = NULL;
	}
	if (instance1 != NULL) {
		delete instance1;
		instance1 = NULL;
	}
	if (scan_joinPool != NULL) {
		delete scan_joinPool;
		scan_joinPool = NULL;
	}
	if (star_para != NULL) {
		delete star_para;
		star_para = NULL;
	}
	if (fullJoinInstance != NULL) {
		delete fullJoinInstance;
		fullJoinInstance = NULL;
	}
}

void CThreadPool::waitAllPoolComplete() {
	//cout << "start wait pool" << endl;
	//getWorkPool().Wait();
	getChunkPool().Wait();
	getPartitionPool().Wait();
	getPrintPool().Wait();
	getInstance().Wait();
	//getInstance1().Wait();
	//getStar_para().Wait();
	//getFullJoinInstance().Wait();
}
