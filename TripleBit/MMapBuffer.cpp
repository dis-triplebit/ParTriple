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

MMapBuffer::MMapBuffer(const char* _filename, size_t initSize) : filename(_filename) {
	// TODO Auto-generated constructor stub
	//功能：将文件大小设为指定大小，并将其映射进地址空间
	fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);//第一打开该路径下的文件时，若不存在则创建。若存在以可读可写方式打开
	if(fd < 0) {
		perror(_filename);
		MessageEngine::showMessage("Create map file error", MessageEngine::ERROR);
	}

	bool createNew = false;
	//cout<<"size: "<<size<<endl;
	size = lseek(fd, 0, SEEK_END);//文件指针移动到文件尾，并得出文件大小
	/**
	 * 函数原形： long lseek(int handle, long offset, int fromwhere)
		函数功能:  移动文件位置指针到指定位置
		参数描述   handle 文件句柄，offset 文件位置
           fromwhere 从文件何处开始移动，该参数可使用以下宏定义：
                       SEEK_CUR  1  从当前位置计算offset
                  	   SEEK_END  2  从文件结束位置计算offset，此时offset为负数。
                       SEEK_SET  0  从文件开始位置计算offset */
	//cout<<"size: "<<size<<endl;
	if(size < initSize) {
		size = initSize;//将文件大小设为页数空间的倍数
		/**
		 *
		ftruncate()
			函数功能：改变文件大小
			相关函数：open、truncate
			表头文件：#include <unistd.h>
			函数原型：int ftruncate(int fd, off_t length)
			函数说明：ftruncate()会将参数fd指定的文件大小改为参数length指定的大小。
			参数fd为已打开的文件描述词，而且必须是以写入模式打开的文件。如果原来的文件件大小比参数length大，则超过的部分会被删去
				返 回 值：0、-1
					错误原因：errno
    					EBADF     参数fd文件描述词为无效的或该文件已关闭
 	 	 	 	 	 	EINVAL    参数fd为一socket并非文件，或是该文件并非以写入模式打开
						使用方法：fd一般可以fileno(FILE *fp)获取，标示文件当前的大小，length则可由用户定义。此函数一般用在文件初始化或者重新为文件分配空间时。
						注意事项：此函数并未实质性的向磁盘写入数据，只是分配了一定的空间供当前文件使用。当 fd<length时，此时如果使用十六进制编辑工具打开该文件，
						你会发现文件末尾多了很多00，这就是执行这个函数后的效果。如果发生系统复位或 者装置掉电以后，该函数所产生的作用将被文件系统忽略，也就是说它
						所分配的空间将不能被识别，文件的大小将会是最后一次写入操作的区域大小，而非 ftruncate分配的空间大小，也就是说，文件大小有可能会被改变。
						解决方法：可以在执行完ftruncate之后，在新空间的末尾写入一个或以上字节的数据（不为Ox00），这样新空间则不为空，文件系统会把这部分空间当
						成这个文件的私有空间处理，而不会出现文件大小改变的错误。
		 * */
		if(ftruncate(fd, initSize) != 0) {
			perror(_filename);
			MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
		}
		createNew = true;
	}
	if(lseek(fd, 0, SEEK_SET) != 0) {//文件指针移动到文件开始位置
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}
	/*
	 *   mmap系统调用并不是完全为了用于共享内存而设计的。它本身提供了不同于一般对普通文件的访问方式，进程可以像读写内存一样对普通文件的操作。而Posix或系统V的共享内存IPC则纯粹用于共享目的，当然mmap()实现共享内存也是其主要应用之一。
          mmap系统调用使得进程之间通过映射同一个普通文件实现共享内存。普通文件被映射到进程地址空间后，进程可以像访问普通内存一样对文件进行访问，不必再调用read()，write（）等操作。

          我们的程序中大量运用了mmap，用到的正是mmap的这种“像访问普通内存一样对文件进行访问”的功能。实践证明，当要对一个文件频繁的进行访问，并且指针来回移动时，调用mmap比用常规的方法快很多。
          来看看mmap的定义：
			void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset);
	参数fd为即将映射到进程空间的文件描述字，一般由open()返回，同时，fd可以指定为-1，此时须指定flags参数中的MAP_ANON，表明进行的是匿名映射（不涉及具体的文件名，避免了文件的创建及打开，很显然只能用于具有亲缘关系的进程间通信）。
    len是映射到调用进程地址空间的字节数，它从被映射文件开头offset个字节开始算起。
	prot参数指定共享内存的访问权限。可取如下几个值的或：PROT_READ（可读）,PROT_WRITE（可写）,PROT_EXEC（可执行）,PROT_NONE（不可访问）。
	flags由以下几个常值指定：MAP_SHARED, MAP_PRIVATE, MAP_FIXED。其中，MAP_SHARED,MAP_PRIVATE必选其一，而MAP_FIXED则不推荐使用。
    如果指定为MAP_SHARED，则对映射的内存所做的修改同样影响到文件。如果是MAP_PRIVATE，则对映射的内存所做的修改仅对该进程可见，对文件没有影响。
    offset参数一般设为0，表示从文件头开始映射。
    参数addr指定文件应被映射到进程空间的起始地址，一般被指定一个空指针，此时选择起始地址的任务留给内核来完成。函数的返回值为最后文件映射到进程空间的地址，
  进程可直接操作起始地址为该值的有效地址。
	 * */
	mmap_addr = (char volatile*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); //将文件映射进地址空间，进程可以像访问普通内存一样对文件进行访问
	/*
	 * NULL参数指定映射存储区的地址，用NULL表示由系统选择映射区的起始地址
	 * fd指定要被映射的文件的描述符
	 * PROT_READ | PROT_WRITE 表示映射区可读可写。
	 *  volatile提醒编译器它后面所定义的变量随时都有可能改变，因此编译后的程序每次需要存储或读取这个变量的时候，
	 *  都会直接从变量地址中读取数据。如果没有volatile关键字，则编译器可能优化读取和存储，可能暂时使用寄存器中的值，
	 *  如果这个变量由别的程序更新了的话，将出现不一致的现象。*/

	if(mmap_addr == MAP_FAILED) {
		perror(_filename);
		cout<<"size: "<<size<<endl;
		MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}

/*
	cout<<_filename<<endl;
	fd = open(_filename, O_RDONLY);
	if(fd < 0) {
		perror(_filename);
                MessageEngine::showMessage("Create map file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if(!(~size)) {
		MessageEngine::showMessage("leek file error!", MessageEngine::ERROR);
	}
	
	void* mmaping = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
	printf("%p", mmaping);
	mmap_addr = (char*)mmaping;
	if(mmap_addr == MAP_FAILED) {
		perror(_filename);
                cout<<"size: "<<size<<endl;
                MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}
*/
}

MMapBuffer::~MMapBuffer() {
	// TODO Auto-generated destructor stub
	flush();
	munmap((char*)mmap_addr, size);
	::close(fd);
}

Status MMapBuffer::flush()
{
	if(msync((char*)mmap_addr, size, MS_SYNC) == 0) {
		return OK;
	}

	return ERROR;
}
//文件扩容，ftruncate可表示扩容或截断
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
	::close(fd);
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

Status MMapBuffer::resize(size_t new_size, bool clear)
{
	//截断文件
	//size_t newsize = size + incrementSize;
	char* new_addr = NULL;
	if (munmap((char*)mmap_addr, size) != 0 || ftruncate(fd, new_size) != 0 ||
				(new_addr = (char*)mmap(NULL, new_size,PROT_READ|PROT_WRITE,MAP_FILE|MAP_SHARED, fd, 0)) == (char*)MAP_FAILED)
	{
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return ERROR;
	}

	mmap_addr = (char volatile*)new_addr;
	if( new_size >size)
		::memset((char*)mmap_addr + size, 0, new_size - size);
	size = new_size;
	return OK;
}

void MMapBuffer::memset(char value)
{
	::memset((char*)mmap_addr, value, size);
}

MMapBuffer* MMapBuffer::create(const char* filename, size_t initSize)
{
	MMapBuffer* buffer = new MMapBuffer(filename, initSize);
	char ch;
	for(size_t i = 0 ; i < buffer->size; i++) {
		ch = buffer->mmap_addr[i];
	}
	return buffer;
}
