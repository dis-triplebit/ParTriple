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
	builder->startBuildN3(argv[1]);
	builder->endBuild();

	delete builder;

	/*
    StatisticsBuffer* statBuffer[8];
    string path="/home/peng/ParallelTripleBit/database";
    string filename=path+"/statIndex";
    statBuffer[0] = new OneConstantStatisticsBuffer(string(path + "/Subject"), StatisticsBuffer::SUBJECT_STATIS,0);	//subject statistics buffer，并将建立的索引文件映射进地址空间
    statBuffer[1] = new OneConstantStatisticsBuffer(string(path + "/ObjectInt"), StatisticsBuffer::OBJECT_STATIS,0);
    statBuffer[2] = new OneConstantStatisticsBuffer(string(path + "/ObjectFloat"), StatisticsBuffer::OBJECT_STATIS,1);
    statBuffer[3] = new OneConstantStatisticsBuffer(string(path + "/ObjectDouble"), StatisticsBuffer::OBJECT_STATIS,2);
    statBuffer[4] = new TwoConstantStatisticsBuffer(string(path + "/SubjectPredicate"),StatisticsBuffer::SUBJECTPREDICATE_STATIS,0);
    statBuffer[5] = new TwoConstantStatisticsBuffer(string(path + "/ObjectIntPredicate"),StatisticsBuffer::OBJECTPREDICATE_STATIS,0);//object-predicate statistics buffer;
    statBuffer[6] = new TwoConstantStatisticsBuffer(string(path + "/ObjectFloatPredicate"),StatisticsBuffer::OBJECTPREDICATE_STATIS,1);//object-predicate statistics buffer;
    statBuffer[7] = new TwoConstantStatisticsBuffer(string(path + "/ObjectDoublePredicate"),StatisticsBuffer::OBJECTPREDICATE_STATIS,2);
    for(unsigned i=1;i<10;i++)
    {

        unsigned count=10;
        ((OneConstantStatisticsBuffer*) statBuffer[0])->addStatis(i, count);
        ((OneConstantStatisticsBuffer*) statBuffer[1])->addStatis(i, count);
        ((OneConstantStatisticsBuffer*) statBuffer[2])->addStatis((float)i, count);
        ((OneConstantStatisticsBuffer*) statBuffer[3])->addStatis((double)i, count);
        ((TwoConstantStatisticsBuffer*) statBuffer[4])->addStatis(i, i+1,count*2);
        ((TwoConstantStatisticsBuffer*) statBuffer[5])->addStatis(i, i+1,count*2);
        ((TwoConstantStatisticsBuffer*) statBuffer[6])->addStatis((float)i, i+1,count*2);
        ((TwoConstantStatisticsBuffer*) statBuffer[7])->addStatis((double)i, i+1,count*3);
    }
    ////object statistics buffer;
     //	//subject-predicate statistics buffer;

    MMapBuffer * indexBuffer1 = NULL;
     ((OneConstantStatisticsBuffer*)statBuffer[0])->save(indexBuffer1,StatisticsBuffer::SUBJECT_STATIS,0);
    ((OneConstantStatisticsBuffer*)statBuffer[1])->save(indexBuffer1,StatisticsBuffer::OBJECT_STATIS,0);
    ((OneConstantStatisticsBuffer*)statBuffer[2])->save(indexBuffer1,StatisticsBuffer::OBJECT_STATIS,1);
    ((OneConstantStatisticsBuffer*)statBuffer[3])->save(indexBuffer1,StatisticsBuffer::OBJECT_STATIS,2);
    ((TwoConstantStatisticsBuffer*)statBuffer[4])->save(indexBuffer1,StatisticsBuffer::SUBJECTPREDICATE_STATIS,0);
    ((TwoConstantStatisticsBuffer*)statBuffer[5])->save(indexBuffer1,StatisticsBuffer::OBJECTPREDICATE_STATIS,0);
    ((TwoConstantStatisticsBuffer*)statBuffer[6])->save(indexBuffer1,StatisticsBuffer::OBJECTPREDICATE_STATIS,1);
    ((TwoConstantStatisticsBuffer*)statBuffer[7])->save(indexBuffer1,StatisticsBuffer::OBJECTPREDICATE_STATIS,2);


    MMapBuffer* indexBufferFile = MMapBuffer::create(filename.c_str(), 0);
    char* indexBuffer = indexBufferFile->get_address();



    //StatisticsBuffer* statBuffer[8];
    string statFilename = path + "/Subject";
    statBuffer[0]=((OneConstantStatisticsBuffer*)statBuffer[0])->load(StatisticsBuffer::SUBJECT_STATIS,statFilename,indexBuffer,0);
    statFilename = path + "/ObjectInt";
    statBuffer[1]=((OneConstantStatisticsBuffer*)statBuffer[1])->load(StatisticsBuffer::OBJECT_STATIS,statFilename,indexBuffer,0);
    statFilename = path + "/ObjectFloat";
    statBuffer[2]=((OneConstantStatisticsBuffer*)statBuffer[2])->load(StatisticsBuffer::OBJECT_STATIS,statFilename,indexBuffer,1);
    statFilename = path + "/ObjectDouble";
    statBuffer[3]=((OneConstantStatisticsBuffer*)statBuffer[3])->load(StatisticsBuffer::OBJECT_STATIS,statFilename,indexBuffer,2);
    statFilename = path + "/SubjectPredicate";
    statBuffer[4]=((TwoConstantStatisticsBuffer*)statBuffer[4])->load(StatisticsBuffer::SUBJECTPREDICATE_STATIS,statFilename,indexBuffer,0);
      statFilename = path + "/ObjectIntPredicate";
    statBuffer[5]=((TwoConstantStatisticsBuffer*)statBuffer[5])->load(StatisticsBuffer::OBJECTPREDICATE_STATIS,statFilename,indexBuffer,0);
    statFilename = path + "/ObjectFloatPredicate";
    statBuffer[6]=((TwoConstantStatisticsBuffer*)statBuffer[6])->load(StatisticsBuffer::OBJECTPREDICATE_STATIS,statFilename,indexBuffer,1);
    statFilename = path + "/ObjectDoublePredicate";
    statBuffer[7]=((TwoConstantStatisticsBuffer*)statBuffer[7])->load(StatisticsBuffer::OBJECTPREDICATE_STATIS,statFilename,indexBuffer,2);

    //,y=1;
    //unsigned sum=1;
    unsigned y=0;
    unsigned x=2;
    for(unsigned i=1;i<10;i++)
    {
         double x=i;
         y=i;
        ((OneConstantStatisticsBuffer*) statBuffer[0])->getStatis(x);
        cout<<i<<",count:"<<x<<endl;
        ((OneConstantStatisticsBuffer*) statBuffer[1])->getStatis(x);
        cout<<i<<",count:"<<x<<endl;
        ((OneConstantStatisticsBuffer*) statBuffer[2])->getStatis(x);
        ((OneConstantStatisticsBuffer*) statBuffer[3])->getStatis(x);
        cout<<i<<",count:"<<x<<endl;
        ((TwoConstantStatisticsBuffer*) statBuffer[4])->getStatis(y,y+1);
        ((TwoConstantStatisticsBuffer*) statBuffer[5])->getStatis(y,y+1);
        ((TwoConstantStatisticsBuffer*) statBuffer[6])->getStatis(x,i+1);
         cout<<i<<","<<i+1<<",count:"<<x<<endl;
         ((TwoConstantStatisticsBuffer*) statBuffer[7])->getStatis(x,i+1);
         cout<<i<<","<<i+1<<",count:"<<x<<endl;

    }*/



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
