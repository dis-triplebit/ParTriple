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
	//Ĭ�����ݼ���ʽΪ3�У�btc4��
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
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);//����4��ͳ�������ļ�,uri��6���ļ���pre��6���ļ��Լ�temp��4���ļ�,����SINGLEд���ֵ���

	cout << "start to store" << endl;
	builder->beforeBuildforNum(argv[1]);// ��ʼ����
	cout<<"-----"<<endl;

	//builder->startBuildN3(argv[1]);
	builder->endBuild();
	delete builder;

	return 0;
}
