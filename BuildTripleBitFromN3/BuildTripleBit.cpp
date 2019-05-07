/*
 * BuildTripleBit.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include <MemoryBuffer.h>
#include "../TripleBit/TripleBitBuilder.h"
#include "../TripleBit/OSFile.h"
#include "BitmapBuffer.h"
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

    string filename = "database/BitmapBuffer";
    string predicateFile(filename);
    predicateFile.append("_predicate");
    string indexFile(filename);
    indexFile.append("_index");
    MMapBuffer* bitmapImage = new MMapBuffer(filename.c_str(), 0);
    MMapBuffer* bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
    MMapBuffer* bitmapIndexImage = new MMapBuffer(indexFile.c_str(), 0);
    BitmapBuffer *bitmap = BitmapBuffer::load(bitmapImage, bitmapIndexImage, bitmapPredicateImage);
    for (int i = 0; i < 2; ++i) {
        map<ID, ChunkManager*> &m = bitmap->predicate_managers[i];
        map<ID, ChunkManager*>::iterator iter;
        for (iter = m.begin(); iter != m.end(); ++iter) {
            double x, y;
            bool flag = true;
            for (int j = 0; j < 3; ++j) {
                MetaData *metaData = (MetaData*)iter->second->getStartPtr(j);
                uchar *start = iter->second->getStartPtr(j) + sizeof(MetaData);
                uchar *end = iter->second->getEndPtr(j);
                cout << "---j = " << j << endl;
                int used = 0;
                while (start < end) {
                    Chunk::readXYId(start + used, x, y, i*3+j);
                    cout <<   x << '\t' << iter->first << "\t" << y << '\t' << i << '\t' << j <<endl;
                    used += 8;
                    if (j == 2) used += 4;
                    if (used == metaData->usedSpace - sizeof(MetaData)){
                        used = 0;
                        uchar *temp = (uchar*)metaData;
                        if(j==0&&flag){
                            temp = temp - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
                            flag = false;
                        } else {
                            temp = temp + MemoryBuffer::pagesize;
                        }
                        metaData = (MetaData*)temp;
                        start = (uchar*)metaData + sizeof(MetaData);
                    }
                }
            }
        }
    }
	return 0;
}
