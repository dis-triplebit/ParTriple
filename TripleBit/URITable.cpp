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
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <math.h>
#include <cstdlib>
using namespace std;
// local includes
#include "./util/Kwaymergesort.h"
// comparison functions for the string merge sort
bool alphaAsc(const string &a, const string &b) {
	return a < b;
}
bool intAsc(const string &a, const string &b) {
	/*stringstream sin;
	int ia,ib;
	sin<<a;
	sin>>ia;
	sin<<b;
	sin>>ib;*/
	int ia=std::atoi(a.c_str());
	int ib=std::atoi(b.c_str());
	return ia < ib;
}
bool doubleAsc(const string &a, const string &b) {
	/*stringstream sin;
	double da,db;
	sin<<a;
	sin>>da;
	sin<<b;
	sin>>db;*/
	double da=std::atof(a.c_str());
	double db=std::atof(b.c_str());
	return da < db;
}
bool dateAsc(const string &a, const string &b) {

	return a < b;
}
#include "URITable.h"
#include "StringIDSegment.h"
#include <string.h>
static const unsigned memoryLimit = sizeof(void*) * (1 << 27);
static void FIXLINE(char * s) {
	int len = (int) strlen(s) - 1;
	if (s[len] == '\n')
		s[len] = 0; //将一行的最后一个字符转化为非可显字符
}
ID URITable::startID = 1;

ID URITable::getMaxID() {
	return suffix_segment->getMaxID()-1;
}

URITable::URITable() {
	SINGLE.assign("single");
	prefix_segment = NULL;
	fake_prefix_segment = NULL;
	suffix_segment = NULL;
	fake_suffix_segment = NULL;
	//prefix_sortfile = NULL;
	prefix_count = 0;
	ssort = NULL;
	head = NULL;
	prev = NULL;

}

URITable::URITable(const string dir) :dir(dir), SINGLE("single") {
	// TODO Auto-generated constructor stub
	prefix_segment = StringIDSegment::create(dir, "/uri_prefix");
	fake_prefix_segment = StringIDSegment::create(dir, "/uri_fake_prefix");
	suffix_segment = StringIDSegment::create(dir, "/uri_suffix");
	fake_prefix_segment->addStringToSegment(SINGLE);
	fake_suffix_segment = StringIDSegment::create(dir, "/fake_uri_suffix");
	head = prev = new Strings_Sort::words();
	head->len = 0;
	head->sufffixcount = 0;
	head->nextsuffix = NULL;

	/*Strings_Sort::words *cur = new Strings_Sort::words();
	 strncpy(cur->value, SINGLE.c_str(), strlen( SINGLE.c_str()));
	 cur->len = strlen( SINGLE.c_str());
	 cur->value[cur->len + 1] = '\0';
	 cur->vp = cur->value;
	 cur->next = NULL;
	 prev->next = cur;
	 prev = cur;
	 prefix_count++;*/
	prefix_segment->addStringToSegment(SINGLE);
	//prefix_sortfile = fopen("prefix_for_sort.txt", "w+");
	prefix_count = 1;
	ssort = new Strings_Sort();
	/*if (prefix_sortfile == NULL) {
	 printf("open file prefix_for_sort.txt error\n");
	 exit(0);
	 }*/
	stringFile = new TempFile("stringFile", 0);
	intfile = new TempFile("intFile", 0);
	doublefile = new TempFile("doubleFile", 0);
	boolfile = new TempFile("boolFile", 0);
	datefile = new TempFile("dateFile", 0);
	urifile= new TempFile("uriFile", 0);
}

URITable::~URITable() {
	// TODO Auto-generated destructor stub
#ifdef DEBUG
	//cout << "destroy URITable" << endl;
#endif
	if (prefix_segment != NULL)
		delete prefix_segment;
	prefix_segment = NULL;
	//cout << "destroy prefix_segment" << endl;

	if (fake_prefix_segment != NULL)
		delete fake_prefix_segment;
	fake_prefix_segment = NULL;
	//cout << "destroy fake_prefix_segment" << endl;
	if (suffix_segment != NULL)
		delete suffix_segment;
	suffix_segment = NULL;
	//cout << "destroy suffix_segment" << endl;
	/*if (head != NULL)
	 delete head;
	 head = NULL;
	 cout << "destroy head" << endl;*/

}
Status URITable::get_fake_IdByURI(const char* URI, ID& id) {
	getPrefix(URI);
	if (prefix.equals(SINGLE.c_str())) {
		searchStr.clear();
		searchStr.insert(searchStr.begin(), 2); //single的ID固定为1，但不加入到与其有关的后缀字符串中
		searchStr.append(suffix.str, suffix.length);
		searchLen.str = searchStr.c_str();
		searchLen.length = searchStr.length();
		if (fake_suffix_segment->findIdByString(id, &searchLen) == false)
			return URI_NOT_FOUND;
	} else {
		char temp[10];
		ID prefixId;
		if (fake_prefix_segment->findIdByString(prefixId, &prefix) == false) {
			//cout << "prefix: " << prefix.str << endl;
			return URI_NOT_FOUND;
		} else {
			sprintf(temp, "%d", prefixId);
			searchStr.assign(suffix.str, suffix.length);
			for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
				searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1); //有个加1的操作
#else
						searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
			}

			searchLen.str = searchStr.c_str();
			searchLen.length = searchStr.length();
			if (fake_suffix_segment->findIdByString(id, &searchLen) == false)
				return URI_NOT_FOUND;
		}
	}

	searchStr.clear();
	return URI_FOUND;
}
//类似于getIdByPredicate
Status URITable::getIdByURI(const char* URI, ID& id)
{
	getPrefix(URI);
	if (prefix.equals(SINGLE.c_str())) {
		searchStr.clear();
		searchStr.insert(searchStr.begin(), 2); //开头插入一个非可显字符
		searchStr.append(suffix.str, suffix.length);
		searchLen.str = searchStr.c_str();
		searchLen.length = searchStr.length();
		if (suffix_segment->findIdByString(id, &searchLen) == false)
			return URI_NOT_FOUND;
	} else {
		char temp[10];
		ID prefixId;
		if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
			//cout << "prefix: " << prefix.str << endl;
			return URI_NOT_FOUND;
		} else {
			sprintf(temp, "%d", prefixId);
			searchStr.assign(suffix.str, suffix.length);
			for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
				searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
			}

			searchLen.str = searchStr.c_str();
			searchLen.length = searchStr.length();
			if (suffix_segment->findIdByString(id, &searchLen) == false)
				return URI_NOT_FOUND;
		}
	}

	searchStr.clear();
	return URI_FOUND;
}
void URITable::updateFakeid(string sortfile,int filetype) {
	ifstream openfile(sortfile.c_str());
	string str;

	while (getline(openfile, str)) {
		ID realID;
		insertTable((char*) str.c_str(), realID);
		//在这里进行的插入表（ID与事物映射）操作，因此可以在这里进行对事物的分类标记操作，只需要加一个参数就行了，因为在调用这个函数的时候外界对不同文件都分别调用了这个函数。
	}
}
void URITable::modify_uri_fake(Strings_Sort::words * word_list) {
	//更改最终需要的prefi_segment中的id

	Strings_Sort::words * curpre = word_list;
	Strings_Sort::words * cursuff = curpre->nextsuffix;

	// for_single
	LengthString *pre;

	unsigned int order = 1;
	int count = 1;
	ID prefixId;
	ID realID = 0;
	//display_Head(word_list);
	pre = new LengthString(SINGLE);
	assert(prefix_segment->findIdByString(prefixId, pre) == true);
	assert(prefixId == 1);
	while (cursuff != NULL) {
		insertTable(cursuff->vp, realID);
		/*cout << "suffcount is" << curpre->sufffixcount << "	realID is :"
		 << realID << "count is :" << count << "prefix:" << SINGLE.c_str()
		 << "	suffix :" << cursuff->vp << endl;*/

		cursuff = cursuff->nextsuffix;
		count++;
		//free(lastsuff);
	}
	curpre = curpre->next;
	//free(lastpre);
	delete pre;
	cout << "single alocated id finished" << endl;
	//for_non_single
	char temp[20];
	prefixId = 1;
	order = 2;
	while (curpre != NULL) {

		pre = new LengthString(curpre->vp);
		if (prefix_segment->findIdByString(prefixId, pre) == false)
			prefixId = prefix_segment->addStringToSegment(pre);
		sprintf(temp, "%d", prefixId);
		cursuff = curpre->nextsuffix;
		while (cursuff != NULL) {
			searchStr.assign(cursuff->vp, cursuff->sufffixcount);
			for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
				searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1); //suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
						searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
			}

			searchLen.str = searchStr.c_str();
			searchLen.length = searchStr.length();
			realID = suffix_segment->addStringToSegment(&searchLen);
			searchStr.clear();

			/*cout << "suffcount is" << curpre->sufffixcount << "	realID is :"
			 << realID << "count is :" << count << "prefix:"
			 << curpre->vp << "	suffix :" << cursuff->vp << endl;*/

			cursuff = cursuff->nextsuffix;
			count++;
			//free(lastsuff);
		}
		//cout << "preid:" << prefixId << "the order is " << order << endl;
		order++;
		curpre = curpre->next;
		//free(lastpre);
		delete pre;

	}
	/*
	 * getPrefix(URI);
	 char temp[20];
	 ID prefixId;

	 prefixId = 1;
	 if (prefix_segment->findIdByString(prefixId, &prefix) == false)
	 prefixId = prefix_segment->addStringToSegment(&prefix);
	 sprintf(temp, "%d", prefixId);

	 searchStr.assign(suffix.str, suffix.length);
	 for (size_t i = 0; i < strlen(temp); i++) {
	 #ifdef USE_C_STRING
	 searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1); //suffix.insert(suffix.begin() + i, temp[i] - '0');
	 #else
	 searchStr.insert(searchStr.begin() + i, temp[i] - '0');
	 #endif
	 }

	 searchLen.str = searchStr.c_str();
	 searchLen.length = searchStr.length();
	 id = suffix_segment->addStringToSegment(&searchLen);
	 searchStr.clear();
	 return OK;*/

	/*	cur = word_list->next;
	 LengthString *pre;
	 unsigned int order = 2;
	 while (cur != NULL) {

	 ID prefixId;
	 pre = new LengthString(cur->vp);
	 assert(fake_prefix_segment->findIdByString(prefixId, pre) == true);
	 if (prefix_segment->findIdByString(prefixId, pre) == false)
	 prefix_segment->setString_with_ID_ToSegment(pre, order);

	 cout << "fake id is :" << prefixId << "order is :" << order
	 << " the prefix is:" << cur->vp << endl;
	 order++;
	 cur = cur->next;
	 delete pre;
	 }*/
	cout << "modify the fake prefix id over" << endl;
	pre = NULL;

}
static int cmpString(string s1, string s2) {
	return strcmp(s1.c_str(), s2.c_str());
}
// Spool items to disk
static char* spool(char* ofs, TempFile& out, const vector<string>& items)
{
	int size = 0;
	for (vector<string>::const_iterator iter = items.begin(), limit =items.end(); iter != limit; ++iter) {
		size = strlen((*iter).c_str());
		out.write(size, (*iter).c_str());
		out.write(1, "\n");
		ofs += size;
	}
	return ofs;
}
void URITable::delteFakefile() {
	fake_prefix_segment->remove(dir, "/uri_fake_prefix");
	fake_suffix_segment->remove(dir, "/fake_uri_suffix");
}
void URITable::k_way_sortURI() {

	/*if(fake_prefix_segment!=NULL)
	 delete fake_prefix_segment;
	 if(fake_suffix_segment!=NULL)
	 delete fake_suffix_segment;*/
	delteFakefile();
	int bufferSize = (1<<27); 
	// allow the sorter to use 1000Kb (base 10) of memory for sorting.
	// once full, it will dump to a temp file and grab another chunk.
	bool compressOutput = false;    // not yet supported
	string tempPath(DATABASE_PATH); // allows you to write the intermediate files anywhere you want.
	
	{
		string inFile = "stringFile.0";
		ofstream outfile("sortstringFile");
		// sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
		//按升序对文件进行词典排序(akin to UNIX sort, "sort FILE")
		KwayMergeSort<string> *sorter = new KwayMergeSort<string>(inFile,
				&outfile, alphaAsc, bufferSize, compressOutput, tempPath);
		sorter->Sort();
		outfile.close();
	}

	{
		string inFile = "intFile.0";
		ofstream outfile("sortintFile");
		// sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
		KwayMergeSort<string> *sorter = new KwayMergeSort<string>(inFile,
				&outfile, intAsc, bufferSize, compressOutput, tempPath);
		sorter->Sort();
		outfile.close();
	}

	{
		string inFile = "doubleFile.0";
		ofstream outfile("sortdoubleFile");
		// sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
		KwayMergeSort<string> *sorter = new KwayMergeSort<string>(inFile,
				&outfile, doubleAsc, bufferSize, compressOutput, tempPath);
		sorter->Sort();
		outfile.close();
	}

	{
		string inFile = "boolFile.0";
		ofstream outfile("sortboolFile");
		// sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
		KwayMergeSort<string> *sorter = new KwayMergeSort<string>(inFile,
				&outfile, alphaAsc, bufferSize, compressOutput, tempPath);
		sorter->Sort();
		outfile.close();
	}

	{
		string inFile = "dateFile.0";
		ofstream outfile("sortdateFile");
		// sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
		KwayMergeSort<string> *sorter = new KwayMergeSort<string>(inFile,
				&outfile, dateAsc, bufferSize, compressOutput, tempPath);
		sorter->Sort();
		outfile.close();
	}
	//URIFile不排序？
}
void URITable::sortStrings() {

	//败笔

	string inputfile = "stringFile.0";
	MMapBuffer *stringfile = new MMapBuffer("stringFile.0", 0);
	size_t size = stringfile->getSize();
	FILE * inf = fopen(inputfile.c_str(), "r");
	if (inf == NULL) {
		cerr << "Could not load :" << inputfile << " error: " << strerror(errno) << std::endl;
	}
	assert(inf != NULL);
	cerr << "Reading string to sort" << std::endl;

	int maxlen = 1000;
	char * s = (char*) malloc(maxlen);

	size_t bytesread = 0;
	size_t linenum = 0;
	vector<vector<string> > runs;
	TempFile out(inputfile, 1);
	TempFile intermediate(inputfile);
	char* ofs = 0;
	cout << "string file size is " << size << endl;
	while (bytesread < size) {
		vector<string> stringV;

		while (fgets(s, maxlen, inf) != NULL) {
			if (s == NULL)
				continue;
			linenum++;
			FIXLINE(s);
			if (s == "")
				continue;
			bytesread += strlen(s) + 1;
			string str(s);
			stringV.push_back(str);

			if (bytesread > memoryLimit)
				break;
		}
		for (int i = 0; i < 10; i++) {
			cout << stringV[i] << endl;
		}
		cout << endl;
		cout << "stringV size is " << stringV.size() << endl;
		for (int i = 0; i < 26000; i++) {
			stringV.pop_back();
		}
		cout << "stringV size is " << stringV.size() << endl;
		std::sort(stringV.begin(), stringV.end(), cmpString);

		cout << "bytes read is " << bytesread << endl;
		if ((bytesread >= size) && (runs.empty())) {
			spool(0, out, stringV); //最终./test读取结束后将保存到items中的最后一小部分写入out
			break;
		}
		// No, spool to intermediate file
		char* newOfs = spool(ofs, intermediate, stringV); //将items写入out（./sortedBySubject.1）中，
		runs.push_back(stringV);
		ofs = newOfs;

	}
	fclose(inf);
	out.close();
	intermediate.close();
	if (!runs.empty()) {

	}
}
void URITable::sortPrefix() {

	int num = prefix_count;
	if (num == 1)
		return;
	cout << "prefix count is " << num << endl;
	//ssort->stringslist= ssort->readfile2list(fp, num);
	//display_Head(head);
	ssort->stringslist = getHead();

	if (!ssort->stringslist) {
		printf("translate error\n");
		exit(0);
	} else {
		printf("translate over \n");
	}

	pthread_mutex_init(&g_mutex, NULL);
	//sort multi thread and merge
	ssort->sortwords_multi(ssort->stringslist);

	//ssort->sortwords_pre(ssort->stringslist);
	pthread_mutex_destroy(&g_mutex);
	printf("sort words over \n");

	//display_Head(ssort->stringslist);
	modify_uri_fake(ssort->stringslist);
}
Status URITable::get_fake_Prefix(const char* URI) { //wonder
	/*
	 if (!firsttime && ((strlen(URI) + fileoff) < (1 << 30))) {
	 stringFile[fileindex]->write(strlen(URI),URI);
	 fileoff += strlen(URI);
	 } else {
	 if (fileindex >= 0) {
	 stringFile[fileindex]->close();

	 }
	 firsttime = false;
	 fileindex++;
	 stringFile.push_back(new TempFile("stringFile", fileindex));
	 stringFile[fileindex]->write(strlen(URI),URI);
	 fileoff += strlen(URI);
	 }
	 */
	stringFile->write(strlen(URI), URI);
	stringFile->write(1, "\n");
	getPrefix(URI);
	ID prefixId;

	if (fake_prefix_segment->findIdByString(prefixId, &prefix) == true) {
		//前缀已经插入，但后缀没有，prefixId代表该前缀在链表当中的位置（无法随机访问）
		ID fakeid;
		insert_fake_Table(URI, fakeid);
		Strings_Sort::words * p = head->next;
		assert(prefixId <= (prefix_count + 1));
		if (prefixId == 1) {
			//前缀为空
			//Strings_Sort::words *cur = new Strings_Sort::words();
			//ur->vp = "SINGLE";
			//cur->len = prefix.length;
			//cur->vp[cur->len] = '\0';
			Strings_Sort::words *cursuff = new Strings_Sort::words();
			cursuff->vp = new char[suffix.length + 1];
			strncpy(cursuff->vp, suffix.str, suffix.length);
			cursuff->sufffixcount = suffix.length;
			cursuff->vp[cursuff->sufffixcount] = '\0';
			cursuff->nextsuffix = head->nextsuffix;
			head->nextsuffix = cursuff;
			head->sufffixcount++;

		} else {
			int i = 2;
			while (i < prefixId && p != NULL) {
				p = p->next;
				i++;
			}
			Strings_Sort::words *cursuff = new Strings_Sort::words();
			cursuff->vp = new char[suffix.length + 1];
			strncpy(cursuff->vp, suffix.str, suffix.length);
			cursuff->sufffixcount = suffix.length;
			cursuff->vp[cursuff->sufffixcount] = '\0';
			cursuff->nextsuffix = p->nextsuffix;
			p->nextsuffix = cursuff;
			p->sufffixcount++;
		}

	} else {
		//	前缀后缀均没有插入
		ID fakeid;
		insert_fake_Table(URI, fakeid);
		//prefixId = fake_prefix_segment->addStringToSegment(&prefix);
		/*char *insert_prefix = new char[prefix.length+1];
		 strncpy(insert_prefix,URI,prefix.length);
		 insert_prefix[prefix.length]='\0';

		 Strings_Sort::words *cur=new Strings_Sort::words();
		 fputs(insert_prefix,prefix_sortfile);
		 fputs("\n",prefix_sortfile);*/
//插入前缀
		Strings_Sort::words *cur = new Strings_Sort::words();
		cur->vp = new char[prefix.length + 1];
		strncpy(cur->vp, URI, prefix.length);
		cur->len = prefix.length;
		cur->vp[cur->len] = '\0';
		//cur->vp = cur->value;
		cur->next = NULL;
		prev->next = cur;
		prev = cur;

		prefix_count++;
//插入后缀
		Strings_Sort::words *cursuff = new Strings_Sort::words();
		cursuff->vp = new char[suffix.length + 1];
		strncpy(cursuff->vp, suffix.str, suffix.length);
		cursuff->sufffixcount = suffix.length;
		cursuff->vp[cursuff->sufffixcount] = '\0';
		cursuff->nextsuffix = NULL;
		prev->nextsuffix = cursuff;
		prev->sufffixcount = 1;

	}

	return OK;
}
Status URITable::closeTempfile()
{
	stringFile->close();
	intfile->close();
	doublefile->close();
	boolfile->close();
	datefile->close();
	urifile->close();
	return OK;
}
Status URITable::removeTempfile()
{
	stringFile->discard();
	intfile->discard();
	doublefile->discard();
	boolfile->discard();
	datefile->discard();
	urifile->discard();
	::remove("sortstringFile");
	::remove("sortintFile");
	::remove("sortdoubleFile");
	::remove("sortboolFile");
	::remove("sortdateFile");
}
//因为thetastore把所有事物都以string（URI）的方式来存，因此函数的命名为WriteURIToTempfile
//其本质实际上是Write(AllString)ToTempfile，数据比较只是因为使用了保序ID
Status URITable::WriteURIToTempfile(const char* URI, int flag) {
	switch (flag) {
	case 0:
		// stringfile
		stringFile->write(strlen(URI), URI);
		stringFile->write(1, "\n");//仅用于回车换行，下边也是
		break;
	case 1:
		// intfile
		intfile->write(strlen(URI), URI);
		intfile->write(1, "\n");
		break;
	case 2:
		// doublefile,decimal
		doublefile->write(strlen(URI), URI);
		doublefile->write(1, "\n");
		break;
	case 3:
		// boolfile
		boolfile->write(strlen(URI), URI);
		boolfile->write(1, "\n");
		break;
	case 4:
		//datefile
		datefile->write(strlen(URI), URI);
		datefile->write(1, "\n");
		break;
	case 5:
		urifile->write(strlen(URI), URI);
		urifile->write(1, "\n");
		break;
	default:
		cout << "object type can not be understood" << endl;
	}
	ID fakeid;
	insert_fake_Table(URI, fakeid);
	return OK;
}
void URITable::display_Head(Strings_Sort::words* word) {
	Strings_Sort::words * curpre = word;
	Strings_Sort::words * cursuff = curpre->nextsuffix;

	int order = 1;
	int count = 1;
	while (cursuff != NULL) {

		cout << "order :" << order << "	count :" << count << "prefix :" << SINGLE << "	suffix:" << cursuff->vp << endl;
		cursuff = cursuff->nextsuffix;
		count++;
	}
	order++;
	curpre = curpre->next;
	while (curpre != NULL) {
		count = 1;
		cursuff = curpre->nextsuffix;
		while (cursuff != NULL) {
			cout << "order :" << order << "	count :" << count << "prefix :" << curpre->vp << "	suffix:" << cursuff->vp << endl;
			cursuff = cursuff->nextsuffix;
			count++;
		}
		curpre = curpre->next;
		order++;
	}

}
Strings_Sort::words * URITable::getHead() {
	Strings_Sort::words * realHead = (Strings_Sort::words*) calloc(1,
		(prefix_count) * sizeof(Strings_Sort::words));
	realHead->len = prefix_count;
	realHead->sufffixcount = head->sufffixcount;
	realHead->next = NULL;
	Strings_Sort::words * tail = realHead;
	Strings_Sort::words *curr = realHead + 1;
	Strings_Sort::words *fakep = head;
	Strings_Sort::words *lastfake = head;
	int counturi = 0;
//for_single
	cout << "single suffix count is" << head->sufffixcount << endl;

	counturi += fakep->sufffixcount;
	Strings_Sort::words *sufflist = (Strings_Sort::words*) calloc(1,
			fakep->sufffixcount * sizeof(Strings_Sort::words));
	if (sufflist != NULL) {
		cout << " alocate space sucseed" << endl;
	}
	Strings_Sort::words *sufflisttail = NULL;
	Strings_Sort::words *p = sufflist;
	Strings_Sort::words *suff = fakep->nextsuffix;
	Strings_Sort::words *lastsuff = suff;
	bool isFirst = true;
	int countsuffix = 0;
	while (suff != NULL) {
		Strings_Sort::wordscopy_for_suffix(p, suff);
		if (isFirst == true) {
			p->nextsuffix = NULL;
			sufflist = sufflisttail = p;
			isFirst = false;
		} else {
			p->nextsuffix = NULL;
			sufflisttail->nextsuffix = p;
			sufflisttail = p;
		}
		p++;
		lastsuff = suff;
		suff = suff->nextsuffix;
		delete lastsuff;
		countsuffix++;
	}
	cout << "single copy succ" << endl;
	realHead->nextsuffix = sufflist;
	assert(countsuffix == realHead->sufffixcount);
	int temp = 1;
	Strings_Sort::words *q = realHead->nextsuffix;
	while (q != NULL) {
		//cout << " before sort,the " << temp << " suffix is :" << q->vp << endl;
		q = q->nextsuffix;
		temp++;
	}
	pthread_mutex_init(&g_mutex, NULL);
	ssort->sortwords_multi_for_suffix(realHead);
	pthread_mutex_destroy(&g_mutex);
	q = realHead->nextsuffix;
	temp = 1;
	while (q != NULL) {
		//cout << " after sort,the " << temp << " suffix is :" << q->vp << endl;
		q = q->nextsuffix;
		temp++;
	}
	//cout << "single suffixlist sort finished" << endl;

	//for_prefix(except single)
	fakep = head->next;
	delete lastfake;

	while (fakep != NULL) {
		countsuffix = 0;
		counturi += fakep->sufffixcount;
		sufflist = (Strings_Sort::words*) calloc(1,
				fakep->sufffixcount * sizeof(Strings_Sort::words));
		if (sufflist != NULL) {
			cout << " alocate space sucseed" << endl;
		}
		sufflisttail = sufflist;
		p = sufflist;
		isFirst = true;
		suff = fakep->nextsuffix;
		lastsuff = suff;
		while (suff != NULL) {
			Strings_Sort::wordscopy_for_suffix(p, suff);
			if (isFirst == true) {
				p->nextsuffix = NULL;
				sufflist = sufflisttail = p;
				isFirst = false;
			} else {
				p->nextsuffix = NULL;
				sufflisttail->nextsuffix = p;
				sufflisttail = p;
			}
			p++;
			lastsuff = suff;
			suff = suff->nextsuffix;
			countsuffix++;
			delete lastsuff;

		}

		Strings_Sort::wordscopy(curr, fakep);

		curr->nextsuffix = sufflist;
		//cout << " copy suffix and pre finished" << endl;
		if (curr->sufffixcount > THREAD_NUMBER * 2) {
			pthread_mutex_init(&g_mutex, NULL);
			ssort->sortwords_multi_for_suffix(curr);
			pthread_mutex_destroy(&g_mutex);
		} else if (curr->sufffixcount > 1) {
			ssort->sortwords_suffix(curr);
		} else {
			//仅有一个后缀时，不需要排序
		}
		curr->next = NULL;
		tail->next = curr;
		tail = curr;
		lastfake = fakep;
		fakep = fakep->next;
		delete lastfake;
		assert(countsuffix == curr->sufffixcount);
		curr++;

	}
	cout << " copy succ and uricount is" << counturi << endl;
	delete fakep;

	return realHead;

}
Status URITable::getPrefix(const char* URI) {
	size_t size = strlen(URI);
	int i;
	for (i = size - 2; i >= 0; i--) {
		if (URI[i] == '/')
			break;
	}

	if (i == -1) {
		prefix.str = SINGLE.c_str();
		prefix.length = SINGLE.length();
		suffix.str = URI;
		suffix.length = size;
	} else {
		prefix.str = URI;
		prefix.length = i;
		suffix.str = URI + i + 1;
		suffix.length = size - i - 1;
	}

	return OK;
}
Status URITable::insertfake_prefixTable(LengthString prefix) //wonder
		{

	ID prefixId;
	if (fake_prefix_segment->findIdByString(prefixId, &prefix) == false)
		prefixId = fake_prefix_segment->addStringToSegment(&prefix);
	return OK;
}
Status URITable::insert_fake_Table(const char* URI, ID& id) {
	getPrefix(URI);
	char temp[20];
	ID prefixId;

	prefixId = 1;
	if (fake_prefix_segment->findIdByString(prefixId, &prefix) == false)
		prefixId = fake_prefix_segment->addStringToSegment(&prefix);
	sprintf(temp, "%d", prefixId);

	searchStr.assign(suffix.str, suffix.length);
	for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
		searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1); //suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
	}

	searchLen.str = searchStr.c_str();
	searchLen.length = searchStr.length();
	id = fake_suffix_segment->addStringToSegment(&searchLen);
	searchStr.clear();
	return OK;

}

Status URITable::insertTable(const char* URI, ID& id) {
	getPrefix(URI);
	char temp[20];
	ID prefixId;

	prefixId = 1;
	if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
		prefixId = prefix_segment->addStringToSegment(&prefix);
	} else {
		//cout<<"find prefix:"<<prefix.str<<"	prefixid:"<<prefixId<<endl;
	}
	sprintf(temp, "%d", prefixId);

	searchStr.assign(suffix.str, suffix.length);
	for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
		searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1); //suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
	}

	searchLen.str = searchStr.c_str();
	searchLen.length = searchStr.length();
	id = suffix_segment->addStringToSegment(&searchLen);
	searchStr.clear();
	return OK;
}

//利用id再返回回来URI不就完事儿了，这里的URI其实就是个string字符串，包括了所有URI，string，integer等等
Status URITable::getURIById(string& URI, ID id) {
	//cout<<"id is"<<id<<endl;
	LengthString prefix, suffix;
	URI.clear();
	if (suffix_segment->findStringById(&suffix, id) == false)
		return URI_NOT_FOUND;
	char temp[10];
	memset(temp, 0, 10);
	const char* ptr = suffix.str;

	int i;
#ifdef USE_C_STRING
	for (i = 0; i < 10; i++) {
		if (ptr[i] > 10)
			break;
		temp[i] = (ptr[i] - 1) + '0';
	}
#else
	for(i = 0; i < 10; i++) {
		if(ptr[i] > 9)
		break;
		temp[i] = ptr[i] + '0';
	}
#endif

	ID prefixId = atoi(temp);
	if (prefixId == 1)
		URI.assign(suffix.str + 1, suffix.length - 1);
	else {
		if (prefix_segment->findStringById(&prefix, prefixId) == false)
			return URI_NOT_FOUND;
		URI.assign(prefix.str, prefix.length);
		URI.append("/");
		URI.append(suffix.str + i, suffix.length - i);
	}

	return OK;
}
//作用：加载uri_prefix的三个文件和uri_suffix的三个文件
URITable* URITable::load(const string dir) { 
	URITable* uriTable = new URITable();
	uriTable->prefix_segment = StringIDSegment::load(dir, "/uri_prefix");
	uriTable->suffix_segment = StringIDSegment::load(dir, "/uri_suffix");
	return uriTable;
}

void URITable::dump() {
	prefix_segment->dump();
	fake_prefix_segment->dump();
	suffix_segment->dump();
}
