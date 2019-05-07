/*
 * BuildTripleBit.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBitBuilder.h"
#include "../TripleBit/OSFile.h"
#include "MMapBuffer.h"

char* DATABASE_PATH;
char* columns;
int main(int argc, char* argv[])
{
	if (argc < 3 || argc >4) {
		fprintf(stderr, "Usage: %s <N3 file name> <Database Directory>\n", argv[0]);
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
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);

	cout << "start to store" << endl;
	builder->beforeBuildforNum(argv[1]);
	cout<<"-----"<<endl;

	//builder->startBuildN3(argv[1]);
	builder->endBuild();

	delete builder;

	return 0;
}
