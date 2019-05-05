/*
 * BuildTripleBit.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBitBuilder.h"
#include "../TripleBit/OSFile.h"

char* DATABASE_PATH;
char* columns;
int main(int argc, char* argv[])
{
	//默认数据集格式为3列，btc4列
	if (argc < 3 || argc >4) {
		fprintf(stderr, "Usage: %s <N3 file name> <Database Directory> [--columnNum]\n", argv[0]);
		return -1;
	}

	if (OSFile::directoryExists(argv[2]) == false) {
		OSFile::mkdir(argv[2]);
	}
	if (argc > 3)
		columns = argv[3];
	else
		columns = NULL;
	DATABASE_PATH = argv[2];
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);//建立4个统计索引文件,uri的6个文件，pre的6个文件以及temp的4个文件,并将SINGLE写入字典中

	cout << "start to store" << endl;
	builder->beforeBuildforNum(argv[1]);// 开始解析


	//builder->startBuildN3(argv[1]);
	builder->endBuild();
	delete builder;

	return 0;
}
