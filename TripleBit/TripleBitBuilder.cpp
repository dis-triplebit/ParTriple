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

#include "MemoryBuffer.h"//没有改动
#include "MMapBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitBuilder.h"
#include "PredicateTable.h"
#include "TripleBit.h"
#include "URITable.h"
#include "Sorter.h"
#include "StatisticsBuffer.h"

#include <string.h>
#include <pthread.h>
#include "Apcluster.h"
using namespace AP;

static int getCharPos(const char* data, char ch) {
	const char * p = data;
	int i = 0;
	while (*p != '\0') {
		if (*p == ch)
			return i + 1;
		p++;
		i++;
	}

	return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir):dir(_dir){
	// TODO Auto-generated constructor stub
	numtable = new vector<string>;	//wo
	//numId = new map<string, ID>;	//wo
	wayfile = new MMapBuffer((dir + "/wayfile").c_str(), 20);
	preType = new MMapBuffer((dir + "/preTypefile").c_str(), 1024);	//一个类型用char表示
	//dir()返回一个string，表示文件名或目录名
	preTable = new PredicateTable(dir);
	//建立了predicate_suffix_stringPool 以及 predicate_prefix_stringPool ,predicate_prefix_idStroffPool predicate_suffix_idStroffPool predicate_suffix_stringHashTable predicate_prefix_stringHashTable

	uriTable = new URITable(dir);
	//建立了uri_prefix_stringPool 以及 uri_suffix_stringPool ,uri_prefix_idStroffPool uri_suffix_idStroffPool uri_prefix_stringHashTable uri_suffix_stringHashTable
	maxID = 0;

	bitmap = new BitmapBuffer(dir);
	//建立了temp1、temp2、temp3、temp4

	statementFile.open(string(dir + "/statement.triple").c_str(), ios::out);
	//建立了statement.triple

	statBuffer[0] = new OneConstantStatisticsBuffer(string(dir + "/subject_statis"), StatisticsBuffer::SUBJECT_STATIS);	//subject statistics buffer，并将建立的索引文件映射进地址空间
	statBuffer[1] = new OneConstantStatisticsBuffer(string(dir + "/object_statis"), StatisticsBuffer::OBJECT_STATIS);//object statistics buffer;
	statBuffer[2] = new TwoConstantStatisticsBuffer(string(dir + "/subjectpredicate_statis"),StatisticsBuffer::SUBJECTPREDICATE_STATIS);	//subject-predicate statistics buffer;
	statBuffer[3] = new TwoConstantStatisticsBuffer(string(dir + "/objectpredicate_statis"),StatisticsBuffer::OBJECTPREDICATE_STATIS);//object-predicate statistics buffer;

	staReifTable = new StatementReificationTable(); //建立有一定空间的MemoryBuffer
	first = true;
	//outfacts = new ofstream((dir + "/outfacts").c_str(), ios::out);
}

TripleBitBuilder::~TripleBitBuilder() {
	// TODO Auto-generated destructor stub
#ifdef TRIPLEBITBUILDER_DEBUG
	cout << "Bit map builder destroyed begin " << endl;
#endif
	if (preTable != NULL)
		delete preTable;
	preTable = NULL;

	if (uriTable != NULL) {
		uriTable->delteFakefile();
		delete uriTable;
	}
	uriTable = NULL;
	//delete uriStaBuffer;
	if (staReifTable != NULL)
		delete staReifTable;
	staReifTable = NULL;
	if (bitmap != NULL) {
		delete bitmap;
		bitmap = NULL;
	}
	for (int i = 0; i < 4; i++) {
		if (statBuffer[i] != NULL)
			delete statBuffer[i];
		statBuffer[i] = NULL;
	}
}

bool TripleBitBuilder::isStatementReification(const char* object) {
	int pos;
	const char* p;
	if ((pos = getCharPos(object, '#')) != -1) {
		p = object + pos;
		if (strcmp(p,"Statement")==0||strcmp(p,"subject")==0||strcmp(p,"predicate")==0||strcmp(p,"object")==0){
			return true;
		}
	}
	return false;
}

//仅被N3parse调用过，用于解析一条三元组，将各个分量转化为id，存储在文件中
void TripleBitBuilder::NTriplesParse(const char* subject, const char* predicate,const char* object,Type::ID objectType, TempFile& facts) {
	ID subjectID, objectID, predicateID;

	if (isStatementReification(object) == false && isStatementReification(predicate) == false) {
		if (preTable->getIDByPredicate(predicate, predicateID)== PREDICATE_NOT_BE_FINDED) { 
			//通过谓词字符串，判断是否已给其分配ID
			preTable->insertTable(predicate, predicateID);
			//作用：向谓词所对应的各个（6个）文件存入对应的值，并获取了整个URI的ID
		}

		//可以做个假设，假设所有有特殊类型（例如integer，string等）表示得object都不会出现在三元组得subject中。这样的话就可以放心大胆的对subject进行原文插入和读取，因为subject全都是URI，没有其他类型。所以这时候对object的任何原文修改打标签行文都不会影响到subject的写入和读取（例如多次重复写入和读取失败）

		//这里的insertTable操作，包括下边的object的代码块，实际上在前边beforeBuildforNum中的updateFakeid时已经进行insertTable了，而且前边也将所有数据都解析过了，为什么这里再进行insertTable一次，完全没有必要啊
		if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND) {
			uriTable->insertTable(subject, subjectID);
			//作用：向URI(SO)所对应的各个（6个）文件存入对应的值，并获取了整个URI的ID
			if (first) {
				//cout << "now the big subjectID is" << subjectID << endl;
				//first = false;
			}
		}
		//但是这里还是要获取到objectID，因为之前插入URItable的object都加了标签，所以这里获取objectID的时候用的字符串需要加上标签，否则获取不到，可以在N3Parse向NTriplesParse多传一个objectType参数，用来给object字符串加标签用
		string objectandtype(object);
		switch (objectType)
		{
		case Type::URI:
		case Type::Literal:
		case Type::CustomLanguage:
		case Type::CustomType:
			break;
		case Type::String:
		case Type::Integer:
		case Type::Decimal:
		case Type::Double:
		case Type::Boolean:
		case Type::Date:
			objectandtype = objectandtype + "^" + Type::tostring(objectType);
			break;
		default:
			break;
		}

		if (uriTable->getIdByURI(objectandtype.c_str(), objectID) == URI_NOT_FOUND) {
			uriTable->insertTable(objectandtype.c_str(), objectID);
			//作用：向URI(SO)所对应的各个（6个）文件存入对应的值，并获取了整个URI的ID
			if (first) {
				//cout << "now the big objectID is" << objectID << endl;
				//first = false;
			}
		}

		//因为id必须要有，因为排序是必须的，所以必须在table中纪录每个object得类型，但是纪录object类型之后在后边resolvetriple得时候接收参数不包括类型
		facts.writeId(subjectID);
		facts.writeId(predicateID);
		facts.writeId(objectID); //将获取到的ID写入./test文件，在startbuildN3里传参了
		//但是这个test文件用到过吗？有用吗？有用，这里边存的都是ID，用来进行insertTriple用

		ID tempID = subjectID >= objectID ? subjectID : objectID;
		maxID = tempID >= maxID ? tempID : maxID;
	} else {
//		statementFile << subject << " : " << predicate << " : " << object << endl;
	}

}

int TripleBitBuilder::numCount = 0;

bool TripleBitBuilder::searchNumString(istream& in, const char* name) //wo
{
	cerr << "start searching Num in " << name << "..." << endl;
	TempFile numFacts("./num"); //建立文件test.0(ID三元组文件)
	TurtleParser parser(in);
	try {
		string subject, predicate, object, source;
		Type::ID objectType;
		char * preTypeFile = preType->getBuffer();
		while (true) {
			try {
				//读到一个三元组
				if (!parser.parse(subject, predicate, object, objectType))
					break;
			} catch (const TurtleParser::Exception& e) {
				while (in.get() != '\n');
				continue;
			}
			if (subject.length() && predicate.length() && object.length()) {
				unsigned int subjectID;
				//得到subject的id，存在subjectID中
				if (uriTable->get_fake_IdByURI(subject.c_str(), subjectID)== URI_NOT_FOUND) {
					//将subject的字面值当作string写入临时文件，根据objectType写入不同的临时文件（第二个参数）
					//thetastore系统默认subject均是URI表示
					uriTable->WriteURIToTempfile(subject.c_str(), 5);
				}
				unsigned int predicateID;
				if (preTable->getIDByPredicate(predicate.c_str(), predicateID)== PREDICATE_NOT_BE_FINDED) { //通过谓词字符串，判断是否已给其分配ID
					//作用：向谓词所对应的各个（6个）文件存入对应的值，并获取了整个URI的ID，存在predicateID中
					//thetastore默认谓词全是URI表示
					preTable->insertTable(predicate.c_str(), predicateID); 
					//只有谓词进行了insertTable操作，而且是在pretable中，不是URItable
					//其他事物进行insertTable操作是在updateFakeid中进行的，位于beforeBuildforNum函数最后的地方，也就是本函数（searchNumString）结束之后。
				}
				if (predicateID >= preType->getSize()) {
					preTypeFile = preType->resize(1024);
				}

				ID objectFakeID;
				//下方switch既是将object写入文件，因此为了在insertTriple时可以获得type，可以将type一并写入文件
				switch (objectType) {
				case Type::Integer:
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						//将object的字面值以string形式写入临时文件，根据objectType写入不同的临时文件（第二个参数）
						uriTable->WriteURIToTempfile(object.c_str(), 1);
					if (preTypeFile[predicateID - 1] < 1)
						preTypeFile[predicateID - 1] = 1;
					break;
				case Type::Decimal:
				case Type::Double:
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						uriTable->WriteURIToTempfile(object.c_str(), 2);
					if (preTypeFile[predicateID - 1] < 2)
						preTypeFile[predicateID - 1] = 2;
					break;
				case Type::Boolean:
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						uriTable->WriteURIToTempfile(object.c_str(), 3);
					if (preTypeFile[predicateID - 1] < 3)
						preTypeFile[predicateID - 1] = 3;
					break;
				case Type::Date:
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						uriTable->WriteURIToTempfile(object.c_str(), 4);
					if (preTypeFile[predicateID - 1] < 4)
						preTypeFile[predicateID - 1] = 4;
					break;
				case Type::URI: //URI
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						uriTable->WriteURIToTempfile(object.c_str(), 5);
					preTypeFile[predicateID - 1] = 5;
					break;
				default: //默认为字符串
					if (uriTable->get_fake_IdByURI(object.c_str(), objectFakeID)== URI_NOT_FOUND)
						uriTable->WriteURIToTempfile(object.c_str(), 0);
					if (preTypeFile[predicateID - 1] < 6)
						preTypeFile[predicateID - 1] = 6;
					break;
				}
			}
		}
	} catch (const TurtleParser::Exception&) {
		return false;
	}
	uriTable->closeTempfile();
	//uriTable->sortPrefix();
	
	//对string，int，double，date，boolean（File）进行排序并输出在sort*File中。不排序URIFile
	uriTable->k_way_sortURI();

	return true;

}
//仅在startbuildN3里用到过，用于将解析生成的ID三元组（位于in中，文件名name）写入rawFacts文件中
bool TripleBitBuilder::N3Parse(istream& in, const char* name,TempFile& rawFacts) {
	cerr << "Parsing " << name << "..." << endl;

	TurtleParser parser(in);
	try {
		string subject, predicate, object, source;
		Type::ID objectType;
		while (true) {
			try {
				if (!parser.parse(subject, predicate, object, objectType))
					break;
			} catch (const TurtleParser::Exception& e) {
				while (in.get() != '\n');
				continue;
			}
			//Construct IDs
			//and write the triples
			//cout<<subject <<"    "<<predicate<<"	"<<object<<endl;
			if (subject.length() && predicate.length() && object.length())
				NTriplesParse((char*)subject.c_str(),(char*)predicate.c_str(),(char*)object.c_str(),objectType,rawFacts);//解析一条三元组，将各个分量转化为id，存储在文件和table中

		}
	} catch (const TurtleParser::Exception&) {
		return false;
	}
	return true;
}

const char* TripleBitBuilder::skipIdIdId(const char* reader) {
	return TempFile::skipId(TempFile::skipId(TempFile::skipId(reader)));
}

int TripleBitBuilder::compare213(const char* left, const char* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l2, l1, l3, r2, r1, r3);
}

int TripleBitBuilder::compare231(const char* left, const char* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l2, l3, l1, r2, r3, r1);
}

int TripleBitBuilder::compare123(const char* left, const char* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l1, l2, l3, r1, r2, r3);
}

int TripleBitBuilder::compare321(const char* left, const char* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);

	return cmpTriples(l3, l2, l1, r3, r2, r1);
}

//private function，跟据ID得到objectandtype,object,type
void TripleBitBuilder::getObject(ID objectID,string& objectandtype,string& object,string& type) {
	uriTable->getURIById(objectandtype, objectID);
	int pos = objectandtype.find_last_of("^");
	if (pos + 8 < objectandtype.length()) {
		//说明"^"没有出现在合适的位置，objectandtype不可拆分
		object = objectandtype;
		type = "";
	}
	else {
		//"^"出现在合适的位置，objectandtype需要拆分
		object = objectandtype.substr(0, pos);
		type = objectandtype.substr(pos + 1);
	}
}

Status TripleBitBuilder::storeWayofXY_MetaDta(TempFile &sortedFile,unsigned char sortedWay) {
	ID subjectID, objectID, predicateID;
	ID lastSubject = 0, lastObject = 0, lastPredicate = 0;
	unsigned count0 = 0, count1 = 0;
	ID lastMin = UINT_MAX, lastMax = 0;//标记一个SP(OP)所对应的最小最大O(S)
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(sortedFile.getFile().c_str()));
	const char* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
	string objectandtype, object, objecttype;
	Element objectelement;

	loadTriple(reader, subjectID, predicateID, objectID);//也就是从sortedFile中得到的ID，这个ID是来源于最开始的rawFacts，我需要做的就是根据这个ID，这样的话就得需要知道存储表是哪个
	//我只要在这里得到一个URItable的实例就可以。TripleBitBuilder已经实例化了一个ID了。因此可以直接调用
	getObject(objectID, objectandtype, object, objecttype);
	

	lastSubject = subjectID;
	lastPredicate = predicateID;
	lastObject = objectID;
	reader = skipIdIdId(reader);
	if (sortedWay == 0) {//SPO按S排序
		//将获取到的ID写outfacts中供monetdb使用
		lastMin = lastMax = objectID;
		//bool v = generateXY(subjectID, objectID);//S>O,则S=O，O=差值，返回1；S<=O,则S不变，O=差值，返回0；采用引用，S对应X，O对应Y
		//v=1时，x对应O，Y对应delta;v=0时，x对应S，Y对应delta
		//将首个SO形成的<XY>插入到内存中
		if (objecttype=="Double"||objecttype=="Integer"||objecttype=="Decimal") {
			//object类型可比较，inserttriple写入字面值
			if (objecttype == "Integer") {
				objectelement.f = stoi(object);
				bitmap->insertTriple(predicateID, subjectID, objectelement, 1, 0);
			}else {
				objectelement.d = stod(object);
				bitmap->insertTriple(predicateID, subjectID, objectelement, 2, 0);
			}
		}else {
			//object类型不可比较，inserttriple写入ID
			objectelement.id = objectID;
			bitmap->insertTriple(predicateID, subjectID, objectelement, 0, 0);
		}
		count0 = count1 = 1;//count0 表示同一个S（O）出现的次数，count表示同一个SP（OP）出现的次数
		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, objectID);
			if (lastSubject == subjectID && lastPredicate == predicateID && lastObject == objectID) {
				reader = skipIdIdId(reader);
				continue;
			}
			if (subjectID != lastSubject) {
				((OneConstantStatisticsBuffer*) statBuffer[0])->addStatis(lastSubject, count0);//添加S统计，带一个默认值参数的3参数
				statBuffer[2]->addStatis_new(lastSubject, lastPredicate, count1,lastMin, lastMax);//添加SP统计
				lastPredicate = predicateID;
				lastSubject = subjectID;
				lastObject = objectID;
				count0 = count1 = 1;
				//不同S不同
				lastMin = lastMax = objectID;
			} else if (predicateID != lastPredicate) {//S相等
				statBuffer[2]->addStatis_new(lastSubject, lastPredicate, count1,lastMin, lastMax);//添加SP统计
				lastPredicate = predicateID;
				lastObject = objectID;
				count0++;
				count1 = 1;
				//同S不同P
				lastMin = lastMax = objectID;
			} else {
				count0++;
				count1++;
				lastObject = objectID;
				//同一个sp,仅关注同一个SP下的O的最大最小值
				if (lastMin > objectID) {
					lastMin = objectID;
				}
				if (lastMax < objectID) {
					lastMax = objectID;
				}
			}
			reader = reader + 12;
			//v = generateXY(subjectID, objectID);
			//0 indicate the triple is sorted by subjects' id;
			//v=1标识S>O;v=0,标识S<=O

			getObject(objectID, objectandtype, object, objecttype);
			if (objecttype == "Double" || objecttype == "Integer" || objecttype == "Decimal") {
				//object类型可比较，inserttriple写入字面值
				if (objecttype == "Integer") {
					objectelement.f = stoi(object);
					bitmap->insertTriple(predicateID, subjectID, objectelement, 1, 0);
				}
				else {
					objectelement.d = stod(object);
					bitmap->insertTriple(predicateID, subjectID, objectelement, 2, 0);
				}
			}
			else {
				//object类型不可比较，inserttriple写入ID
				objectelement.id = objectID;
				bitmap->insertTriple(predicateID, subjectID, objectelement, 0, 0);
			}
		}
		((OneConstantStatisticsBuffer*) statBuffer[0])->addStatis(lastSubject,
				count0);//添加S统计，带一个默认值参数的3参数
		(TwoConstantStatisticsBuffer*) statBuffer[2]->addStatis_new(lastSubject,
				lastPredicate, count1, lastMin, lastMax);//添加SP统计
	} else {//SPO按O排序
		lastMin = lastMax = subjectID;
		//bool v = generateXY(objectID, subjectID);
		getObject(objectID, objectandtype, object, objecttype);
		if (objecttype == "Double" || objecttype == "Integer" || objecttype == "Decimal") {
			//object类型可比较，inserttriple写入字面值
			if (objecttype == "Integer") {
				objectelement.f = stoi(object);
				bitmap->insertTriple(predicateID, subjectID, objectelement, 4, 1);
			}
			else {
				objectelement.d = stod(object);
				bitmap->insertTriple(predicateID, subjectID, objectelement, 5, 1);
			}
		}
		else {
			//object类型不可比较，inserttriple写入ID
			objectelement.id = objectID;
			bitmap->insertTriple(predicateID, subjectID, objectelement, 3, 1);
		}
		count0 = count1 = 1;
		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, objectID);
			if (lastSubject == subjectID && lastPredicate == predicateID && lastObject == objectID) {
				reader = skipIdIdId(reader);
				continue;
			}
			if (objectID != lastObject) {
				((OneConstantStatisticsBuffer*) statBuffer[1])->addStatis(lastObject, count0);
				statBuffer[3]->addStatis_new(lastObject, lastPredicate, count1,lastMin, lastMax);
				lastPredicate = predicateID;
				lastObject = objectID;
				lastSubject = subjectID;
				count0 = count1 = 1;
				lastMin = lastMax = subjectID;
			} else if (predicateID != lastPredicate) {
				statBuffer[3]->addStatis_new(lastObject, lastPredicate, count1,lastMin, lastMax);
				lastPredicate = predicateID;
				lastSubject = subjectID;
				count0++;
				count1 = 1;
				lastMin = lastMax = subjectID;
			} else {
				lastSubject = subjectID;
				count0++;
				count1++;
				if (lastMin > subjectID) {
					lastMin = subjectID;
				}
				if (lastMax < subjectID) {
					lastMax = subjectID;
				}
			}
			reader = skipIdIdId(reader);
			//v = generateXY(objectID, subjectID);
			// 1 indicate the triple is sorted by objects' id;
			getObject(objectID, objectandtype, object, objecttype);
			if (objecttype == "Double" || objecttype == "Integer" || objecttype == "Decimal") {
				//object类型可比较，inserttriple写入字面值
				if (objecttype == "Integer") {
					objectelement.f = stoi(object);
					bitmap->insertTriple(predicateID, subjectID, objectelement, 4, 1);
				}
				else {
					objectelement.d = stod(object);
					bitmap->insertTriple(predicateID, subjectID, objectelement, 5, 1);
				}
			}
			else {
				//object类型不可比较，inserttriple写入ID
				objectelement.id = objectID;
				bitmap->insertTriple(predicateID, subjectID, objectelement, 3, 1);
			}
		}
		((OneConstantStatisticsBuffer*) statBuffer[1])->addStatis(lastObject,count0);
		(TwoConstantStatisticsBuffer*) statBuffer[3]->addStatis_new(lastObject,lastPredicate, count1, lastMin, lastMax);
		//*outdegree << lastObject << " " << lastPredicate << " " << count1 << " "<< lastMin << " " << lastMax << "\n";
		//outdegree->close();
	}

	mappedIn.close();
	bitmap->flush();
	((OneConstantStatisticsBuffer*) statBuffer[0])->flush();
	((OneConstantStatisticsBuffer*) statBuffer[1])->flush();
	((TwoConstantStatisticsBuffer*) statBuffer[2])->flush();
	((TwoConstantStatisticsBuffer*) statBuffer[3])->flush();
	return OK;
}

Status TripleBitBuilder::resolveSortFile(TempFile &sortedFile,unsigned char sortedWay)//wonder
{
	//sortedWay=0,表示按S排序，否则按O排序
	Snum = 5;
	Onum = 3;
	//storeWayofAdjList(sortedFile, sortedWay);

	unsigned *array = (unsigned *) wayfile->getBuffer();
	array[sortedWay] = 0;
	storeWayofXY_MetaDta(sortedFile, sortedWay);
	return OK;

}

//解析ID三元组文件，并建立按S/O排序的SPO文件，将<X,Y>压缩存储到bitmap中
Status TripleBitBuilder::resolveTriples(TempFile& rawFacts, TempFile& facts){//facts没有用
	cout << "Sort by Subject" << endl;
	//ID subjectID, objectID, predicateID;
	//ID lastSubject = 0, lastObject = 0, lastPredicate = 0;
	//unsigned count0 = 0, count1 = 0;
	TempFile sortedBySubject("./SortByS"), sortedByObject("./SortByO");


	cout << "Sort by Subject over" << endl;
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare123); //利用了归并排序，形成了./SortByS文件，内容是SPO，按S排序
	sortedBySubject.close();
	resolveSortFile(sortedBySubject, 0);


	cout << "Sort by Object" << endl;
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare321);
	sortedByObject.close();
	resolveSortFile(sortedByObject, 1);


	wayfile->flush();
	rawFacts.discard();
	sortedBySubject.discard();
	sortedByObject.discard();
	cout << "resolve sortFiles over" << endl;
	return OK;
}
Status TripleBitBuilder::beforeBuildforNum(string fileName)	//wo
{
	ifstream in((char*) fileName.c_str());
	if (!in.is_open()) {
		cerr << "Unable to open " << fileName << endl;
		return ERROR;
	}
	if (!searchNumString(in, fileName.c_str())) {
		in.close();
		return ERROR;
	}
	in.close();
	/*
	 std::sort((*numtable).begin(), (*numtable).end(), compareNum);
	 vector<string>::iterator end_unique = unique((*numtable).begin(),
	 (*numtable).end());
	 (*numtable).erase(end_unique, (*numtable).end());
	 cout << "numtable's compacity is " << (*numtable).size() << endl;

	 //
	 //可以进行聚类操作
	 //可以进行聚类操作
	 if (numCount > 0) {
	 //APcluster();
	 numIDinuriTable();
	 }*/

	//在此之前只有谓词进行了写table操作，而且是predicateTable
	//sort文件为searchNumString中的最后一行k_way_sortURI的输出结果
	//uriTable->updateFakeid("sortboolFile",1);
	//uriTable->updateFakeid("sortintFile",2);
	//uriTable->updateFakeid("sortdoubleFile",3);
	//uriTable->updateFakeid("sortdateFile",4);
	//uriTable->updateFakeid("sortstringFile",5);
	//uriTable->updateFakeid("uriFile.0",6);
	//那我可不可以不在这里用updateFakeid写入table，而在下边写table
	cout << "over" << endl;
	//uriTable->removeTempfile();


	//----------------------------------------------------------------------------------------------------
	//下方代码从startBuildN3中移植过来，将beforeBuildforNum与startBuildN3合并成一个函数

	
	//string newfile=fileName;
	//newfile.append("./test");
	TempFile rawFacts("./test");	//建立文件test.0(ID三元组文件)

	ifstream in((char*)fileName.c_str());
	if (!in.is_open()) {
		cerr << "Unable to open " << fileName << endl;
		return ERROR;
	}
	if (!N3Parse(in, fileName.c_str(), rawFacts)) {	//将解析生成的ID三元组写入./test中
		in.close();
		return ERROR;
	}
	//outfacts->close();
	in.close();
	delete uriTable; // 存放so的一个vector
	uriTable = NULL;
	delete preTable;
	preTable = NULL;
	delete staReifTable; //???没弄懂这个做了什么用
	staReifTable = NULL;

	rawFacts.flush();
	cout << "over" << endl; //三元组写入到./test中写入完毕

	//sort by s,o
	TempFile facts(fileName); //原先有的代码，以数据集名eg:LUBM1U.n3建立临时文件LUBM1U.n3.1(id)
	//TempFile facts("./datasets ");//经检验，文件为空，无内容，为了函数参数用，也就是说resolveTriples设计不合理
	resolveTriples(rawFacts, facts);//facts没有用
	facts.discard();	//可以注释掉,看到文件
	//注意结束时，并没有删除掉bitmap，也就是说bitmap可以继续使用，而uritable和pretable已被释放空间
	return OK;
}

Status TripleBitBuilder::endBuild() {
	//作用：建立了BitmapBuffer文件和BitmapBuffer_predicate
	bitmap->completeInsert();	//????实质什么也没做
	statementFile.close();
	cout << "maxID is :" << maxID << endl;
	//unsigned *array = (unsigned *) wayfile->getBuffer();
	//array[2]=maxID;
	//wayfile->flush();
	bitmap->save1();
	//ofstream是从内存到硬盘，ifstream是从硬盘到内存，其实所谓的流缓冲就是内存空间;

	//ofstream ofile(string(dir + "/statIndex").c_str());
	MMapBuffer* indexBuffer = NULL;
	((OneConstantStatisticsBuffer*) statBuffer[0])->save(indexBuffer);////仅在第一次执行时建立文件statIndex
	((OneConstantStatisticsBuffer*) statBuffer[1])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*) statBuffer[2])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*) statBuffer[3])->save(indexBuffer);

	delete indexBuffer;
	return OK;
}
