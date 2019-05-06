/*
 * ThreadPool.h
 *
 *  Created on: 2010-6-18
 *      Author: liupu
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <vector>
#include <pthread.h>
#include <iostream>
#include <boost/function.hpp>

using namespace std;
#define TASK_QUEUE_INIT_SIZE (1024 * 1024)

class CThreadPool{
private:
	CThreadPool(int threadNum);

public:
	typedef boost::function<void()> Task;

	class NodeTask{
	public:
		Task value;//func name
		NodeTask *next;
		NodeTask():value(0), next(NULL){}
		NodeTask(const Task &val):value(val), next(NULL){}
	};

public:

	NodeTask *head, *tail;

private:
	int threadNum;
	bool shutDown;
	vector<pthread_t> vecIdleThread, vecBusyThread;
	pthread_mutex_t headMutex, tailMutex;
	pthread_mutex_t pthreadIdleMutex, pthreadBusyMutex;
	pthread_cond_t pthreadCond, pthreadEmpty, pthreadBusyEmpty;

	static CThreadPool *workPool, *chunkPool, *partitionPool,*printPool,*scan_joinPool,*fullJoinInstance,*star_para ,*joinpara1,*joinpara2;
	static CThreadPool* instance,*instance1;	//原先tripleBit

private:
	static void *threadFunc(void *threadData);
	int moveToIdle(pthread_t tid);
	int moveToBusy(pthread_t);
	int create();
	void Enqueue(const Task &task);
	Task Dequeue();
	bool isEmpty(){ return head->next == NULL; }

public:
	static CThreadPool& getInstance();//用于entityIDbuffer 排序（第一阶段应该不会出现）,用于流水线
	static CThreadPool& getInstance1();//用于findbyknown buffer
	static CThreadPool &getWorkPool();//没用了,已被getInstance()代替
	static CThreadPool &getChunkPool();
	static CThreadPool &getPartitionPool();
	static CThreadPool &getPrintPool();
	static CThreadPool &getScan_JoinPool();//性能为提升
	static CThreadPool& getFullJoinInstance();//性能为提升
	static CThreadPool& getStar_para();//性能未提升
	static CThreadPool& getjoinpara1();
	static CThreadPool& getjoinpara2();//性能未提升
	static void createAllPool();
	static void deleteReadPool();
	static void deleteAllPool();
	static void waitAllPoolComplete();
	~CThreadPool();
	int AddTask(const Task &task);
	int stopAll();
	int Wait();
};

struct ThreadPoolArg
{
	CThreadPool* pool;
};
#endif /* THREADPOOL_H_ */
