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
MMapBuffer::MMapBuffer(const char *_fileName, size_t initSize) : fileName(_fileName) {
    fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        perror(_fileName);
        MessageEngine::showMessage("Create map file error", MessageEngine::ERROR);
    }
    // get file size and use it to set buffer size
    size = lseek(fd, 0, SEEK_END);
    if (size < initSize) {
        size = initSize;
        //  truncate the file to initSize
        if (ftruncate(fd, initSize) != 0) {
            perror(_fileName);
            MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
        }
    }
    // set the file offset to zero
    if (lseek(fd, 0, SEEK_SET) != 0) {
        perror(_fileName);
        MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
    }

    mmap_addr = (char volatile *) mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mmap_addr == MAP_FAILED) {
        perror(_fileName);
        cout << "MAP_FAILED file size: " << size << endl;
        MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
    }
}

MMapBuffer::~MMapBuffer() {
    flush();
    munmap((char *) mmap_addr, size);
    close(fd);
}

Status MMapBuffer::flush() {
    if (msync((char *) mmap_addr, size, MS_SYNC) == 0) {
        return OK;
    }
    return ERROR;
}

/**
 * expand Buffer to (originSize + incrementSize)
 * I think it's a bad function name.
 * @param incrementSize
 * @return new address of buffer
 */
char *MMapBuffer::resize(size_t incrementSize) {
    size_t newSize = size + incrementSize;
    char *newAddr = NULL;
    if (munmap((char *) mmap_addr, size) != 0) {
        MessageEngine::showMessage("resize-munmap error!", MessageEngine::ERROR);
        return NULL;
    }

    if (ftruncate(fd, newSize) != 0) {
        MessageEngine::showMessage("resize-ftruncate file error!", MessageEngine::ERROR);
        return NULL;
    }

    if ((newAddr = (char *) mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) ==
        (char *) MAP_FAILED) {
        MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
        return NULL;
    }

    mmap_addr = (char volatile *) newAddr;
    // set new space to zero
    ::memset((char *) mmap_addr + size, 0, incrementSize);
    size = newSize;
    return (char *) mmap_addr;
}

void MMapBuffer::discard() {
    munmap((char *) mmap_addr, size);
    close(fd);
    /*
     * unlink() deletes a name from the filesystem.  If that name was the
     * last link to a file and no processes have the file open, the file is
     * deleted and the space it was using is made available for reuse.
     */
    unlink(fileName.c_str());
}

char *MMapBuffer::getBuffer() {
    return (char *) mmap_addr;
}

char *MMapBuffer::getBuffer(int pos) {
    return (char *) mmap_addr + pos;
}

/**
 * truly resize buffer to new_size, parameter clear
 * is used as placeholder for function overloading
 * @param new_size
 * @param clear
 * @return
 */
Status MMapBuffer::resize(size_t new_size, bool clear) {
    char *new_addr = NULL;
    if (munmap((char *) mmap_addr, size) != 0 || ftruncate(fd, new_size) != 0 ||
        (new_addr = (char *) mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) ==
        (char *) MAP_FAILED) {
        MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
        return ERROR;
    }

    mmap_addr = (char volatile *) new_addr;
    // only set zero when new size grater than size
    if (new_size > size) {
        ::memset((char *) mmap_addr + size, 0, new_size - size);
    }
    size = new_size;
    return OK;
}

/**
 * set all buffer bytes to value
 * @param value target value
 */
void MMapBuffer::memset(char value) {
    ::memset((char *) mmap_addr, value, size);
}

/**
 * static function to get a MMapBuffer
 * @param filename
 * @param initSize
 * @return
 */
MMapBuffer *MMapBuffer::create(const char *filename, size_t initSize) {
    MMapBuffer *buffer = new MMapBuffer(filename, initSize);
    return buffer;
}










