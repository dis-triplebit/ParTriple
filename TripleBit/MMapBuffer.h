#ifndef MMAPBUFFER_H_
#define MMAPBUFFER_H_

#include "TripleBit.h"

class MMapBuffer {
	int fd;
	char volatile* mmap_addr;
    string fileName;
	size_t size;
public:
	char* resize(size_t incrementSize);
	char* getBuffer();
	char* getBuffer(int pos);
	void discard();
	Status flush();
	size_t getSize() { return size;}
	size_t get_length() { return size;}
	char * get_address() const { return (char*)mmap_addr; }

	virtual Status resize(size_t new_size,bool clear);
	virtual void   memset(char value);

	MMapBuffer(const char* _fileName, size_t initSize);
	virtual ~MMapBuffer();

public:
	static MMapBuffer* create(const char* filename, size_t initSize);
};

#endif /* MMAPBUFFER_H_ */
