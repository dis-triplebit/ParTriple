#ifndef URITABLE_H_
#define URITABLE_H_

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

#include "TripleBit.h"
#include "StringIDSegment.h"
#include <stdlib.h>
#include "Strings_Sort.h"
#include "TempFile.h"
using namespace std;
class URITable {
	ID prefixStartID;
	StringIDSegment* prefix_segment;
	StringIDSegment* fake_prefix_segment;
	StringIDSegment* suffix_segment;
	StringIDSegment* fake_suffix_segment;
	LengthString prefix, suffix;
	LengthString searchLen;

	Strings_Sort::words * head;
	Strings_Sort::words *prev;


	string SINGLE;
	string searchStr;
public:
	//FILE *prefix_sortfile;
	//Strings_Sort::words * head;
	size_t prefix_count;
	Strings_Sort *ssort;
	pthread_mutex_t g_mutex;

	/*vector<TempFile*> stringFile;
	int fileindex;
	unsigned fileoff;
	bool firsttime;*/
	TempFile *stringFile;
	TempFile *intfile;
	TempFile *doublefile;
	TempFile *datefile;
	TempFile *boolfile;
	TempFile *urifile;
	string dir;
private:

	Status getPrefix(const char*  URI);
public:
	void delteFakefile();
	void k_way_sortURI();
	void sortStrings();//wonder
	void sortPrefix();//wonder
	Status get_fake_Prefix(const char* URI);
	//Status WriteURIToTempfile(const char* URI);
	Status WriteURIToTempfile(const char* URI,int flag);
	Status closeTempfile();
	Status removeTempfile();
	Strings_Sort::words * getHead();
	void display_Head(Strings_Sort::words *word);
	void modify_uri_fake(Strings_Sort::words * word_list);
	void updateFakeid(string sortfile,int filetype);
	URITable();
	URITable(const string dir);
	virtual ~URITable();
	Status insertTable(const char* URI,ID& id);
	Status insert_fake_Table(const char* URI,ID& id);//wonder
	Status insertfake_prefixTable(LengthString prefix);//wonder
	Status get_fake_IdByURI(const char* URI,ID& id);//wonder
	Status getIdByURI(const char* URI,ID& id);
	Status getURIById(string& URI,ID id);
	Status insertNumID(const char* URI,ID& id);
	size_t getSize() {
		cout<<"max id: "<<suffix_segment->getMaxID()<<endl;
		return prefix_segment->getSize() + suffix_segment->getSize();
	}

	ID getUriCount(){
		return suffix_segment->idStroffPool->size();
	}
	ID getMaxID();
	void dump();
public:
	static ID startID;
	static URITable* load(const string dir);

};

#endif /* URITABLE_H_ */
