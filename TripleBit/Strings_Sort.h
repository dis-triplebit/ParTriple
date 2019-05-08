/*
 * Strings_Sort.h
 *
 *  Created on: 2014年8月21日
 *      Author: wonder
 */
#include <stdint.h>
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include "ThreadPool.h"
using namespace std;

#ifndef STRINGS_SORT_H_
#define STRINGS_SORT_H_


//#define DEBUG
#ifdef DEBUG
#define print2(fmt , args ...) do {printf("%s:%d      ",__func__,__LINE__);	 printf(fmt,##args);}while(0);
//#define print(fmt , args ...) do {printf("%s:%d      ",__func__,__LINE__);	 printf(fmt,##args);}while(0);
#else
#define print2(fmt, args...)
#endif

/* time debug*/
#define TIME_READ
#define TIME_WRITE
#define TIME_SORT
//#define TIME_SORT_SINGLE
#define TIME_MEGRE

/* time type*/
#define READ 0
#define WRITE 1
#define SORT 2
#define SORT_FOR_SUFF 20
#define MEGRE 3
#define MEGRE_FOR_SUFF 30
#define SORT_SINGLE 4
#define MAXLINE 1000


#ifdef USE_SPINLOCK
#define LOCK pthread_spin_lock(&g_spinlock);
#define UNLOCK   pthread_spin_unlock(&g_spinlock);
#define INITLOCK pthread_spin_init(&g_spinlock);
#define DESTROYLOCK pthread_spin_destroy(&g_spinlock);
#else
#define LOCK pthread_mutex_lock(&g_mutex);
#define UNLOCK  pthread_mutex_unlock(&g_mutex);
#define INITLOCK pthread_mutex_init(&g_mutex,NULL);
#define DESTROYLOCK pthread_mutex_destroy(&g_mutex);
#endif


class Strings_Sort
{
public:

	struct words {
			//char value[MAXLINE];
			int len;//前缀长度（在头节点中表示前缀个数）
			char * vp;
			int sufffixcount;//同一前缀的后缀个数
			//char * suffix;
			struct words * next;
			struct words * nextsuffix;//采用倒插法的方式插入

			//char patch[8];

		} node;
	struct merge_node {
		struct words * word_list;
		struct merge_node *next;
	};

	struct thread_args {
		struct merge_node mege;
		int begin;
		int end;
		pthread_t tid;
	};
	struct words *stringslist;
	struct merge_node * g_list ;
	struct merge_node * g_tail ;
	struct merge_node * g_sufflist ;
	struct merge_node * g_sufftail ;
	pthread_mutex_t g_mutex;

public:
	Strings_Sort();
	~Strings_Sort()
	{
		DESTROYLOCK;
	}
	static void wordscopy(Strings_Sort::words *dest, Strings_Sort::words *fakep) {
		dest->vp=new char[strlen(fakep->vp)+1];
		strncpy(dest->vp, fakep->vp, strlen(fakep->vp));
		dest->len = fakep->len;
		dest->vp[dest->len] = '\0';
		//dest->nextsuffix=fakep->nextsuffix;
		dest->sufffixcount=fakep->sufffixcount;
		//dest->vp=dest->value;
	}
	static void wordscopy_for_suffix(Strings_Sort::words *dest, Strings_Sort::words *fakep) {
			dest->vp=new char[strlen(fakep->vp)+1];
			strncpy(dest->vp, fakep->vp, strlen(fakep->vp));
			dest->sufffixcount=fakep->sufffixcount;

			dest->vp[dest->sufffixcount] = '\0';
			//dest->nextsuffix=fakep->nextsuffix;
			//dest->len = fakep->len;
			//dest->vp=dest->value;
		}
	void usetime(struct timeval *end, struct timeval *begin, int flag);

	//struct words * readfile2list(char* filename, size_t nums);

	//int writenode2file(char* filename, struct words * word_list);

	int strswap_for_suffix(struct words * a, struct words * b);
	int strswap(struct words * a, struct words * b);

	void qwordsort_for_suffix(struct words * strarray, int begin, int end);
	void qwordsort(struct words * strarray, int begin, int end);
	int sortwords_pre(struct words * word_list);
	int sortwords_suffix(struct words * word_list);
	Strings_Sort::merge_node * get_from_for_suffix();
	Strings_Sort::merge_node * get_from();

	void insert_into(struct merge_node * mege);
	void insert_into_for_suffix(struct merge_node * mege);

	struct words * merge(struct words * wd1, struct words * wd2);
	struct words * merge_for_suffix(struct words * wd1, struct words * wd2);

	void  qsort_thread_for_suffix(struct thread_args *args);
	void  qsort_thread(struct thread_args *args);

	int sortwords_multi(struct words * &wlist_head);
	int sortwords_multi_for_suffix(struct words  * &wlist_head);


};




#endif /* STRINGS_SORT_H_ */
