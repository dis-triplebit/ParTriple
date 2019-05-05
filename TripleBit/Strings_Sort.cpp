/*
 * Strings_Sort.cpp
 *
 *  Created on: 2014年8月21日
 *      Author: wonder
 */
#include "Strings_Sort.h"
#include "TripleBit.h"
#include <boost/function.hpp>

Strings_Sort::Strings_Sort() {
	stringslist = NULL;

	g_list = NULL;
	g_tail = NULL;
	g_sufflist = NULL;
	g_sufftail = NULL;
	INITLOCK
	;

}

void Strings_Sort::usetime(struct timeval *end, struct timeval *begin,
		int flag) {

	float delay;
	char buf[16];
	if ((end->tv_usec -= begin->tv_usec) < 0) {
		end->tv_sec--;
		end->tv_usec += 1000000;
	}

	end->tv_sec -= begin->tv_sec;
	delay = end->tv_sec * 1000 + end->tv_usec / 1000;
	switch (flag) {
	case READ:
		sprintf(buf, "READ:");
		break;
	case WRITE:
		sprintf(buf, "WRITE:");
		break;
	case SORT:
		sprintf(buf, "SORT:");
		break;
	case SORT_FOR_SUFF:
		sprintf(buf, "SORT_FOR_SUFF:");
		break;
	case SORT_SINGLE:
		sprintf(buf, "SORT_SINGLE:");
		break;
	case MEGRE:
		sprintf(buf, "MEGRE:");
		break;
	case MEGRE_FOR_SUFF:
		sprintf(buf, "MEGRE_FOR_SUFF:");
		break;
	default:
		sprintf(buf, "default ");

	}print2("%s usetime = %f ms\n", buf, delay);

}

/*Strings_Sort::words * Strings_Sort::readfile2list(char* filename, size_t nums) {
 print2("read fp : %s\n", filename);
 #ifdef TIME_READ
 struct timeval begin, end;
 gettimeofday(&begin, NULL);
 #endif
 int ret = -1;
 FILE * fp = NULL;
 size_t size = 0;
 struct words * head, *cur, *prev;

 int i = 0;

 fp = fopen(filename, "r+");
 if (fp == NULL)
 exit(EXIT_FAILURE);
 size = (nums + 1) * sizeof(struct words);
 //print("sizeof struct words : %lu, total size = %u\n",sizeof(struct words), size);
 head = (struct words*) calloc(1, size);
 head->len = nums;
 prev = head;
 cur = head + 1;

 while (fgets(cur->value, MAXLINE, fp) != NULL) {
 cur->len = strlen(cur->value);
 cur->value[cur->len - 1] = '\0';
 cur->vp = cur->value;
 //print("words: %s, len:%d, index:%d\n",cur->value, cur->len,i++);
 prev->next = cur;
 prev = cur;
 cur++;
 }
 fclose(fp);
 #ifdef TIME_READ
 gettimeofday(&end, NULL);
 usetime(&end, &begin, READ);
 #endif
 return head;
 }*/

/*int Strings_Sort::writenode2file(char* filename, struct words * word_list) {
 print2("write fp : %s\n", filename);
 assert(filename!=NULL && word_list != NULL);

 #ifdef TIME_WRITE
 struct timeval begin, end;
 gettimeofday(&begin, NULL);
 #endif
 FILE * fp = NULL;
 int nums = 0;
 struct words * cur;
 int i;
 fp = fopen(filename, "w+");
 if (fp == NULL)
 exit(EXIT_FAILURE);

 nums = word_list->len;
 print2("num of words : %d\n", nums);
 cur = word_list->next;
 while (cur != NULL) {
 //    print("cprev : %p, cnext : %p, cur:%p\n",cur->prev, cur->next, cur);
 fputs(cur->vp, fp);
 fputs("\n", fp);
 cur = cur->next;
 }
 fclose(fp);

 #ifdef TIME_WRITE
 gettimeofday(&end, NULL);
 usetime(&end, &begin, WRITE);
 #endif
 return 0;
 }*/

/*swap value pointer of word*/
int Strings_Sort::strswap_for_suffix(struct words * a, struct words * b) {

	int count;
	char * ch;
	ch = a->vp;
	count = a->sufffixcount;

	a->vp = b->vp;
	b->vp = ch;
	a->sufffixcount = b->sufffixcount;
	b->sufffixcount = count;
	return 0;
}

int Strings_Sort::strswap(struct words * a, struct words * b) {
	struct words * tmp = new Strings_Sort::words();
	tmp->vp = a->vp;
	tmp->len = a->len;
	tmp->sufffixcount = a->sufffixcount;
	tmp->next = a->next;
	tmp->nextsuffix = a->nextsuffix;
	a->vp = b->vp;
	b->vp = tmp->vp;

	a->len = b->len;
	b->len = tmp->len;

	a->sufffixcount = b->sufffixcount;
	b->sufffixcount = tmp->sufffixcount;

	a->nextsuffix = b->nextsuffix;
	b->nextsuffix = tmp->nextsuffix;

	return 0;
}

/*quick sort of words */
void Strings_Sort::qwordsort_for_suffix(struct words * strarray, int begin,
		int end) {
	//cout << "start to sort" << endl;
	if (begin >= end)
		return;
	struct words * item = &strarray[begin];
	int low = begin, high = end;
	//printf("a=%s,  b=%s , end-begin= %d\n", strarray[begin].vp, strarray[end].vp,end-begin);
	while (low < high) {
		while (low < high && strcmp(item->vp, strarray[high].vp) <= 0)
			high--;
		while (low < high && strcmp(item->vp, strarray[low].vp) >= 0)
			low++;
		strswap_for_suffix(&strarray[low], &strarray[high]);
	}
	if (low != begin)
		strswap_for_suffix(&strarray[low], item);
	qwordsort_for_suffix(strarray, begin, low - 1);
	qwordsort_for_suffix(strarray, low + 1, end);

}

void Strings_Sort::qwordsort(struct words * strarray, int begin, int end) {
	if (begin >= end)
		return;
	struct words * item = &strarray[begin];
	int low = begin, high = end;
	//print("a=%s,  b=%s\n",strarray[begin].vp, strarray[end].vp);
	while (low < high) {
		assert(item!=NULL);
		assert((strarray+high)!=NULL);
		while (low < high && strcmp(item->vp, strarray[high].vp) <= 0)
			high--;
		while (low < high && strcmp(item->vp, strarray[low].vp) >= 0)
			low++;
		strswap(&strarray[low], &strarray[high]);
	}
	if (low != begin)
		strswap(&strarray[low], item);
	qwordsort(strarray, begin, low - 1);
	qwordsort(strarray, low + 1, end);

}
int Strings_Sort::sortwords_pre(struct words * word_list)
{
	struct timeval begin, end;
	gettimeofday(&begin, NULL);
	assert(word_list != NULL);
	int nums = word_list->len;
	struct words *cur;
	cur = word_list;
	qwordsort(cur, 1, nums - 1);
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT_SINGLE);
	return 0;
}
/*not mulit-thread sorting words */
int Strings_Sort::sortwords_suffix(struct words * word_list) {
	assert(word_list != NULL);
	struct timeval begin, end;
	gettimeofday(&begin, NULL);
	int i;
	int nums = word_list->sufffixcount;
	struct words *cur;
	cur = word_list->nextsuffix;
	qwordsort_for_suffix(cur, 0, nums - 1);
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT_SINGLE);
	return 0;
}

Strings_Sort::merge_node * Strings_Sort::get_from_for_suffix() {
	struct merge_node * tmp = NULL;
	LOCK
	;

	if (g_sufflist != NULL) {
		tmp = g_sufflist;
		g_sufflist = tmp->next;
		tmp->next = NULL;
	}

	UNLOCK
	;
	return tmp;
}

Strings_Sort::merge_node * Strings_Sort::get_from() {
	struct merge_node * tmp = NULL;
	LOCK
	;

	if (g_list != NULL) {
		tmp = g_list;
		g_list = tmp->next;
		tmp->next = NULL;
	}

	UNLOCK
	;
	return tmp;
}

void Strings_Sort::insert_into_for_suffix(struct merge_node * mege) {
	cout << "start to insert" << endl;
	LOCK
	;

	//print("mege=%p, mege.wl=%p\n",mege,mege->word_list);
	if (g_sufflist == NULL) {
		g_sufflist = mege;
		g_sufftail = mege;
	} else {
		g_sufftail->next = mege;
		g_sufftail = mege;
	}

	UNLOCK
	;
	cout << "end to insert" << endl;
}

void Strings_Sort::insert_into(struct merge_node * mege) {
	LOCK
	;

	//print("mege=%p, mege.wl=%p\n",mege,mege->word_list);
	if (g_list == NULL) {
		g_list = mege;
		g_tail = mege;
	} else {
		g_tail->next = mege;
		g_tail = mege;
	}

	UNLOCK
	;
}

Strings_Sort::words * Strings_Sort::merge_for_suffix(Strings_Sort::words * wd1,
		Strings_Sort::words * wd2) {
	struct words * res = NULL, *prev = NULL;
	if (strcmp(wd1->vp, wd2->vp) <= 0) {
		res = prev = wd1;
		wd1 = wd1->nextsuffix;
	} else {
		res = prev = wd2;
		wd2 = wd2->nextsuffix;
	}

	while (wd1 != NULL && wd2 != NULL) {
		if (strcmp(wd1->vp, wd2->vp) <= 0) {
			prev->nextsuffix = wd1;
			prev = wd1;
			wd1 = wd1->nextsuffix;
		} else {
			prev->nextsuffix = wd2;
			prev = wd2;
			wd2 = wd2->nextsuffix;
		}
	}
	if (wd1 != NULL) {
		prev->nextsuffix = wd1;
	} else {
		prev->nextsuffix = wd2;
	}
	return res;
}

Strings_Sort::words * Strings_Sort::merge(Strings_Sort::words * wd1,
		Strings_Sort::words * wd2) {
	struct words * res = NULL, *prev = NULL;
	if (strcmp(wd1->vp, wd2->vp) <= 0) {
		res = prev = wd1;
		wd1 = wd1->next;
	} else {
		res = prev = wd2;
		wd2 = wd2->next;
	}

	while (wd1 != NULL && wd2 != NULL) {
		if (strcmp(wd1->vp, wd2->vp) <= 0) {
			prev->next = wd1;
			prev = wd1;
			wd1 = wd1->next;
		} else {
			prev->next = wd2;
			prev = wd2;
			wd2 = wd2->next;
		}
	}
	if (wd1 != NULL) {
		prev->next = wd1;
	} else {
		prev->next = wd2;
	}
	return res;
}

void Strings_Sort::qsort_thread_for_suffix(struct thread_args *args) {
	struct timeval begin;
	struct merge_node * other = NULL, *m = NULL;

#ifdef TIME_SORT_SINGLE
	gettimeofday(&begin, NULL);
#endif
	//struct thread_args *args1 = args;
	//printf("pid %d, begin:%d, end:%d\n", args->tid, args->begin, args->end);
	if (strcmp((args->mege.word_list)[args->begin].vp,
			(args->mege.word_list)[args->end].vp) == 0)
		printf("insert error\n");
	//cout<<args->mege.word_list[begin].vp<<endl;
	//struct words* wod = args->mege.word_list;
	//for (int i = args->begin; i <= args->end; i++)
	//cout << wod[i].vp << endl;
	qwordsort_for_suffix(args->mege.word_list, args->begin, args->end);

#ifdef IN_THREAD_MEGRE
	other = get_from_for_suffix();
	if(other != NULL)
	{
		print2("in thread merge\n");
		m = merge_for_suffix(other->word_list,args1->mege.word_list);
		other->word_list = m;
		insert_into_for_suffix(other);
	}
	else
#endif
	cout << " the thread sort the string over " << endl;
	insert_into_for_suffix(&(args->mege));

#ifdef TIME_SORT_SINGLE
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT_FOR_SUFF);
#endif
	cout << " the thread  really sort the string over " << endl;
	//return NULL;
}

void Strings_Sort::qsort_thread(struct thread_args *args) {
	struct timeval begin;
	struct merge_node * other = NULL, *m = NULL;

#ifdef TIME_SORT_SINGLE
	gettimeofday(&begin, NULL);
#endif
	struct thread_args *args1 = args;
	printf("pid %d, begin:%d, end:%d\n",args->tid, args1->begin, args1->end);
	qwordsort(args1->mege.word_list, args1->begin, args1->end);
	printf("pid %d sort over\n",args->tid);
#ifdef IN_THREAD_MEGRE
	other = get_from();
	if(other != NULL)
	{
		print2("in thread merge\n");
		m = merge(other->word_list,args1->mege.word_list);
		other->word_list = m;
		insert_into(other);
	}
	else
#endif
	insert_into(&(args1->mege));

#ifdef TIME_SORT_SINGLE
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT);
#endif
	//return NULL;
}

int Strings_Sort::sortwords_multi_for_suffix(struct words * &wlist_head) {

	//cout << "start to sort" << endl;
	int nums = wlist_head->sufffixcount;
	struct thread_args * targs[THREAD_NUMBER];
	struct thread_args * leftarg;

	int i = 0;
	int period = nums / THREAD_NUMBER;
	int left = nums % THREAD_NUMBER;
	struct words * word_list = wlist_head->nextsuffix;
	struct merge_node * one = NULL, *two = NULL;
	struct words *m = NULL;
	struct timeval begin, end;

#ifdef TIME_SORT
	gettimeofday(&begin, NULL);
#endif
	if (period > 0) {
		for (i = 0; i < THREAD_NUMBER; i++) {
			targs[i] = (struct thread_args *) calloc(1,
					sizeof(struct thread_args));
			if (targs[i] == NULL) {
				printf("calloc fail\n");
			}

			if (i < left) {
				word_list[(i + 1) * (period + 1) - 1].nextsuffix = NULL; //分割

				targs[i]->begin = 0;
				targs[i]->end = period;
				targs[i]->tid = i + 1;
				targs[i]->mege.word_list = &word_list[i * (period + 1)];
			} else {
				word_list[(i + 1) * period + left - 1].nextsuffix = NULL; //分割
				targs[i]->begin = 0;
				targs[i]->end = period - 1;
				targs[i]->tid = i + 1;
				targs[i]->mege.word_list = &word_list[i * period + left];

			}
			CThreadPool::getInstance().AddTask(
					boost::bind(&Strings_Sort::qsort_thread_for_suffix, this,
							targs[i]));

		}

		CThreadPool::getInstance().Wait();
	} else {

//相当于各个有序
	}
	//for(int k=targs[i]->begin;k<=targs[i]->end;i++)
	//	cout<<(targs[i]->mege.word_list)[k].vp<<endl;

	//cout << "end to sort" << endl;
#ifdef TIME_SORT
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT_FOR_SUFF);
#endif
#ifdef TIME_MEGRE
	gettimeofday(&begin, NULL);
#endif
	// merge
	while (1) {
		if (g_sufflist == NULL) {
			break;
		}
		if (one == NULL) {
			one = get_from_for_suffix();
			continue;
		} else {
			two = get_from_for_suffix();
		}
		if (one != NULL && two != NULL) {
			m = merge_for_suffix(one->word_list, two->word_list);
			one->word_list = m;
			wlist_head->nextsuffix = m;
			insert_into_for_suffix(one);
			one = two = NULL;
		}
	}
#ifdef TIME_MEGRE
	gettimeofday(&end, NULL);
	usetime(&end, &begin, MEGRE_FOR_SUFF);
#endif
	return 0;
}

/*mulit-thread sorting words */
int Strings_Sort::sortwords_multi(struct words * &wlist_head) {

	int nums = wlist_head->len-1;
	struct thread_args * targs[THREAD_NUMBER];
	struct thread_args * leftarg;

	int i = 0;
	int period = nums / THREAD_NUMBER;
	int left = nums % THREAD_NUMBER;
	struct words * word_list = wlist_head->next;
	struct merge_node * one = NULL, *two = NULL;
	struct words *m = NULL;
	struct timeval begin, end;

#ifdef TIME_SORT
	gettimeofday(&begin, NULL);
#endif
	cout<<period<<" : "<<left<<endl;
	if (period) {
		for (i = 0; i < THREAD_NUMBER; i++) {
			targs[i] = (struct thread_args *) calloc(1,
					sizeof(struct thread_args));
			if (targs[i] == NULL) {
				printf("calloc fail\n");
			}
			if (i < left) {
				word_list[(i + 1) * (period + 1) - 1].next = NULL; //分割

				targs[i]->begin = 0;
				targs[i]->end = period;
				targs[i]->tid = i + 1;
				targs[i]->mege.word_list = &word_list[i * (period + 1)];
			} else {
				word_list[(i + 1) * period + left - 1].next = NULL; //分割
				targs[i]->begin = 0;
				targs[i]->end = period - 1;
				targs[i]->tid = i + 1;
				targs[i]->mege.word_list = &word_list[i * period + left];

			}
			cout<<"thread "<<targs[i]->tid<<" is runing"<<endl;
			CThreadPool::getInstance().AddTask(
					boost::bind(&Strings_Sort::qsort_thread, this, targs[i]));
		}

		CThreadPool::getInstance().Wait();
	}
	cout<<"thread sort prefix over"<<endl;
#ifdef TIME_SORT
	gettimeofday(&end, NULL);
	usetime(&end, &begin, SORT);
#endif
#ifdef TIME_MEGRE
	gettimeofday(&begin, NULL);
#endif
	// merge
	while (1) {
		if (g_list == NULL) {
			break;
		}
		if (one == NULL) {
			one = get_from();
			continue;
		} else {
			two = get_from();
		}
		if (one != NULL && two != NULL) {
			m = merge(one->word_list, two->word_list);
			one->word_list = m;
			wlist_head->next = m;
			insert_into(one);
			one = two = NULL;
		}
	}
#ifdef TIME_MEGRE
	gettimeofday(&end, NULL);
	usetime(&end, &begin, MEGRE);
#endif
	return 0;
}
