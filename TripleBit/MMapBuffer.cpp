#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <stdio.h>
#include <errno.h>
#include "MMapBuffer.h"

/**
* construct a new MMapBuffer
* open file and map the file to memory
* set to memory address mmap_addr
* @param _fileName mapped file
* @param initSize memory init size
*/
MMapBuffer::MMapBuffer(const char* _filename, size_t initSize) : filename(_filename) {
	fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
	if(fd < 0) {
		perror(_filename);
		MessageEngine::showMessage("Create map file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if(size < initSize) {
		size = initSize;
		if(ftruncate(fd, initSize) != 0) {
			perror(_filename);
			MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
		}
	}
	if(lseek(fd, 0, SEEK_SET) != 0) {
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}

	mmap_addr = (char volatile*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if(mmap_addr == MAP_FAILED) {
		perror(_filename);
		cout<<"size: "<<size<<endl;
		MessageEngine::showMessage("MMapBuffer::MMapBuffer:map file to memory error", MessageEngine::ERROR);
	}
}

MMapBuffer::~MMapBuffer() {
	flush();
	munmap((char*)mmap_addr, size);
	close(fd);
}

Status MMapBuffer::flush()
{
	if(msync((char*)mmap_addr, size, MS_SYNC) == 0) {
		return OK;
	}

	return ERROR;
}

/**
 * expand Buffer to (originSize + incrementSize)
 * @param incrementSize
 * @return new address of buffer
 */
char* MMapBuffer::resize(size_t incrementSize)
{
	size_t newsize = size + incrementSize;

	//cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;

	char* new_addr = NULL;
	if (munmap((char*)mmap_addr, size) != 0 ){
		MessageEngine::showMessage("resize-munmap error!", MessageEngine::ERROR);
		return NULL;
	}

	if(ftruncate(fd, newsize) != 0) {
		MessageEngine::showMessage("resize-ftruncate file error!", MessageEngine::ERROR);
		return NULL;
	}

	if((new_addr = (char*)mmap(NULL, newsize,PROT_READ|PROT_WRITE,MAP_FILE|MAP_SHARED, fd, 0)) == (char*)MAP_FAILED)
	{
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return NULL;
	}

	//cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;
	mmap_addr = (char volatile*)new_addr;

	::memset((char*)mmap_addr + size, 0, incrementSize);

	//cout<<filename.c_str()<<": "<<__FUNCTION__<<" end: "<<size<<" : "<<newsize<<endl;
	size = newsize;
	return (char*)mmap_addr;
}

void MMapBuffer::discard()
{
	munmap((char*)mmap_addr, size);
	close(fd);
	unlink(filename.c_str());
}

char* MMapBuffer::getBuffer()
{
	return (char*)mmap_addr;
}

char* MMapBuffer::getBuffer(int pos)
{
	return (char*)mmap_addr + pos;
}

/**
 * truly resize buffer to new_size, parameter clear
 * is used as placeholder for function overloading
 * @param new_size
 * @param clear
 * @return
 */
Status MMapBuffer::resize(size_t new_size, bool clear)
{
	//size_t newsize = size + incrementSize;
	char* new_addr = NULL;
	if (munmap((char*)mmap_addr, size) != 0 || ftruncate(fd, new_size) != 0 ||
				(new_addr = (char*)mmap(NULL, new_size,PROT_READ|PROT_WRITE,MAP_FILE|MAP_SHARED, fd, 0)) == (char*)MAP_FAILED)
	{
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return ERROR;
	}

	mmap_addr = (char volatile*)new_addr;

	::memset((char*)mmap_addr + size, 0, new_size - size);
	size = new_size;
	return OK;
}

void MMapBuffer::memset(char value)
{
	::memset((char*)mmap_addr, value, size);
}

/**
 * static function to get a MMapBuffer
 * @param filename
 * @param initSize
 * @return
 */
MMapBuffer* MMapBuffer::create(const char* filename, size_t initSize)
{
	MMapBuffer* buffer = new MMapBuffer(filename, initSize);
//	char ch;
//	for(size_t i = 0; i < buffer->size; ++i) ch = buffer->mmap_addr[i];
	return buffer;
}










