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

#include "PredicateTable.h"
#include "StringIDSegment.h"

PredicateTable::PredicateTable(const string dir) :
		SINGLE("single") {
	// TODO Auto-generated constructor stub
	prefix_segment = StringIDSegment::create(dir, "/predicate_prefix");
	suffix_segment = StringIDSegment::create(dir, "/predicate_suffix");

	prefix_segment->addStringToSegment(SINGLE);
}

PredicateTable::~PredicateTable() {
	// TODO Auto-generated destructor stub
	if (prefix_segment != NULL)
		delete prefix_segment;
	prefix_segment = NULL;

	if (suffix_segment != NULL)
		delete suffix_segment;
	suffix_segment = NULL;
}

Status PredicateTable::getPrefix(const char* URI) {
	size_t size = strlen(URI);
	int i;
	for (i = size - 2; i >= 0; i--) {
		if (URI[i] == '/')
			break;
	}

	if (i == -1) { //直接是一个字符串，不带'/‘,这时让前缀为single，后缀为URI
		prefix.str = SINGLE.c_str();
		prefix.length = SINGLE.length();
		suffix.str = URI;
		suffix.length = size;
	} else {
		//prefix.assign(URI.begin(), URI.begin() + size);
		//suffix.assign(URI.begin() + size + 1, URI.end());
		prefix.str = URI;
		prefix.length = i;
		suffix.str = URI + i + 1;
		suffix.length = size - i - 1;
	}

	return OK;
}
//作用：向谓词所对应的各个（6个）文件存入对应的值，并获取了整个URI的ID
Status PredicateTable::insertTable(const char* str, ID& id) {
	getPrefix(str);
	char temp[20];
	ID prefixId = 1;

	if (prefix_segment->findIdByString(prefixId, &prefix) == false)
		prefixId = prefix_segment->addStringToSegment(&prefix);
	sprintf(temp, "%d", prefixId);

	searchStr.assign(suffix.str, suffix.length);//用suffix开始的length长度的字符串赋值给searchStr
	for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
		searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);	//前插法形成前缀ID+后缀字符串
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
	}

	searchLen.str = searchStr.c_str();
	searchLen.length = searchStr.length();
	id = suffix_segment->addStringToSegment(&searchLen);		//获取了整个URI的ID
	if (id == -1)
		cout << "invalid prefix id is" << prefixId << "and string is" << str << endl;
	searchStr.clear();
	return OK;
}

Status PredicateTable::getPredicateByID(string& URI, ID id) {
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

string PredicateTable::getPredicateByID(ID id) {
	searchStr.clear();
	if (suffix_segment->findStringById(&suffix, id) == false)
		return searchStr;
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
		searchStr.assign(suffix.str + 1, suffix.length - 1);
	else {
		if (prefix_segment->findStringById(&prefix, prefixId) == false)
			return string("");
		searchStr.assign(prefix.str, prefix.length);
		searchStr.append("/");
		searchStr.append(suffix.str + i, suffix.length - i);
	}

	return searchStr;
}

Status PredicateTable::getIDByPredicate(const char* str, ID& id) {
	getPrefix(str);
	if (prefix.equals(SINGLE.c_str())) {
		searchStr.clear();
		searchStr.insert(searchStr.begin(), 2);		//开头插入一个非可显字符
		searchStr.append(suffix.str, suffix.length);
		//searchStr.assign(suffix.str, suffix.length);
		searchLen.str = searchStr.c_str();
		searchLen.length = searchStr.length();
		if (suffix_segment->findIdByString(id, &searchLen) == false)
			return PREDICATE_NOT_BE_FINDED;
	} else {
		char temp[10];
		ID prefixId;
		if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
			return PREDICATE_NOT_BE_FINDED;
		} else {
			sprintf(temp, "%d", prefixId);
			searchStr.assign(suffix.str, suffix.length);
			for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
				searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');	//采用前插法
#endif
			}

			//cout<<searchStr<<endl;
			searchLen.str = searchStr.c_str();
			searchLen.length = searchStr.length();
			if (suffix_segment->findIdByString(id, &searchLen) == false)//这里形成了整个字符串的ID
				return PREDICATE_NOT_BE_FINDED;
		}
	}

	searchStr.clear();
	return OK;
}

size_t PredicateTable::getPredicateNo() {
	return suffix_segment->idStroffPool->size();
}

PredicateTable* PredicateTable::load(const string dir) {
	PredicateTable* table = new PredicateTable();

	table->prefix_segment = StringIDSegment::load(dir, "/predicate_prefix");
	table->suffix_segment = StringIDSegment::load(dir, "/predicate_suffix");

	return table;
}

void PredicateTable::dump() {
	prefix_segment->dump();
	suffix_segment->dump();
}
