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

#include "Sorter.h"
#include "TempFile.h"
#include <vector>
#include <algorithm>
#include <cassert>
#include <cstring>
using namespace std;
//---------------------------------------------------------------------------
/// Maximum amount of usable memory. XXX detect at runtime!
static const unsigned memoryLimit = sizeof(void*) * (1 << 27);//4B*2^27
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A memory range
struct Range {
	const char* from, *to;

	/// Constructor
	Range(const char* from, const char* to) :
		from(from), to(to) {
	}

	/// Some content?
	bool equals(const Range& o) {
		return ((to - from) == (o.to - o.from)) && (memcmp(from, o.from, to - from) == 0);
	}
};
//---------------------------------------------------------------------------
/// Sort wrapper that colls the comparison function
struct CompareSorter {
	/// Comparison function
	typedef int (*func)(const char*, const char*);

	/// Comparison function
	const func compare;

	/// Constructor
	CompareSorter(func compare) :
		compare(compare) {
	}

	/// Compare two entries
	bool operator()(const Range& a, const Range& b) const {
		return compare(a.from, b.from) < 0;
	}
};
//---------------------------------------------------------------------------
static char* spool(char* ofs, TempFile& out, const vector<Range>& items, bool eliminateDuplicates)
// Spool items to disk
{
	Range last(0, 0);
	for (vector<Range>::const_iterator iter = items.begin(), limit = items.end(); iter != limit; ++iter) {
		if ((!eliminateDuplicates) || (!last.equals(*iter))) {
			last = *iter;
			out.write(last.to - last.from, last.from);
			ofs += last.to - last.from;
		}
	}
	return ofs;
}
//---------------------------------------------------------------------------
}
// Sort a temporary file,将./test文件按S（O）排序后写入到参数out中即：./sortedBySubject(Object)中
void Sorter::sort(TempFile& in,TempFile& out,const char* (*skip)(const char*), int(*compare)(const char*, const char*), bool eliminateDuplicates)
{
	// Open the input
	in.close();
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(in.getFile().c_str()));//打开的是./test
	const char* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce runs
	vector<Range> runs;//Range中含const char* from , to;
	TempFile intermediate(out.getBaseFile());//建立了./sortedBySubject.1(临时文件id编号)
	char* ofs = 0;
	while (reader < limit) {
		// Collect items
		vector<Range> items;//之前的item被清空
		const char* maxReader = reader + memoryLimit;//可用的最大内存数
		while (reader < limit) {
			const char* start = reader;//start指向一个SPO的开始地址
			reader = skip(reader);//这里的skip指向skipIdIdId，reader=reader+12,reader指向该SPO的结束地址
			items.push_back(Range(start, reader));

			// Memory Overflow?
			if ((reader + (sizeof(Range) * items.size())) > maxReader)
				break;
		}

		// Sort the run
		std::sort(items.begin(), items.end(), CompareSorter(compare));//利用vector自带的排序算法，std:stable_sort

		// Did everything fit?
		if ((reader == limit) && (runs.empty())) {
			spool(0, out, items, eliminateDuplicates);//最终./test读取结束后将保存到items中的最后一小部分写入out（./sortedByS）中，
			break;
		}

		// No, spool to intermediate file
		char* newOfs = spool(ofs, intermediate, items, eliminateDuplicates);//将items写入out（./sortedBySubject.1）中，
		runs.push_back(Range(ofs, newOfs));
		ofs = newOfs;
	}
	intermediate.close();
	mappedIn.close();

	// Do we have to merge runs?
	if (!runs.empty()) {
		// Map the ranges
		MemoryMappedFile tempIn;
		assert(tempIn.open(intermediate.getFile().c_str()));
		for (vector<Range>::iterator iter = runs.begin(), limit = runs.end(); iter != limit; ++iter) {
			(*iter).from = tempIn.getBegin() + ((*iter).from - static_cast<char*> (0));
			(*iter).to = tempIn.getBegin() + ((*iter).to - static_cast<char*> (0));
		}

		// Sort the run heads
		std::sort(runs.begin(), runs.end(), CompareSorter(compare));

		// And merge them
		Range last(0, 0);
		while (!runs.empty()) {
			// Write the first entry if no duplicate
			Range head(runs.front().from, skip(runs.front().from));
			if ((!eliminateDuplicates) || (!last.equals(head)))
				out.write(head.to - head.from, head.from);
			last = head;

			// Update the first entry. First entry done?
			if ((runs.front().from = head.to) == runs.front().to) {
				runs[0] = runs[runs.size() - 1];
				runs.pop_back();
			}

			// Check the heap condition
			unsigned pos = 0, size = runs.size();
			while (pos < size) {
				unsigned left = 2 * pos + 1, right = left + 1;
				if (left >= size)
					break;
				if (right < size) {
					if (compare(runs[pos].from, runs[left].from) > 0) {
						if (compare(runs[pos].from, runs[right].from) > 0) {
							if (compare(runs[left].from, runs[right].from) < 0) {
								std::swap(runs[pos], runs[left]);
								pos = left;
							} else {
								std::swap(runs[pos], runs[right]);
								pos = right;
							}
						} else {
							std::swap(runs[pos], runs[left]);
							pos = left;
						}
					} else if (compare(runs[pos].from, runs[right].from) > 0) {
						std::swap(runs[pos], runs[right]);
						pos = right;
					} else
						break;
				} else {
					if (compare(runs[pos].from, runs[left].from) > 0) {
						std::swap(runs[pos], runs[left]);
						pos = left;
					} else
						break;
				}
			}
		}
		tempIn.close();
	}

	intermediate.discard();
	out.close();
}
// Sort a temporary file,将./test文件按S（O）排序后写入到参数out中即：./sortedBySubject(Object)中
void Sorter::sort(char* in, TempFile& out, const char* (*skip)(const char*), int(*compare)(const char*, const char*), bool eliminateDuplicates)
{
	// Open the input
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(in));//打开的是./test
	const char* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce runs
	vector<Range> runs;//Range中含const char* from , to;
	TempFile intermediate(out.getBaseFile());//建立了./sortedBySubject.1(临时文件id编号)
	char* ofs = 0;
	while (reader < limit) {
		// Collect items
		vector<Range> items;//之前的item被清空
		const char* maxReader = reader + memoryLimit;//可用的最大内存数
		while (reader < limit) {
			const char* start = reader;//start指向一个SPO的开始地址
			reader = skip(reader);//这里的skip指向skipIdIdId，reader=reader+12,reader指向该SPO的结束地址
			items.push_back(Range(start, reader));

			// Memory Overflow?
			if ((reader + (sizeof(Range) * items.size())) > maxReader)
				break;
		}

		// Sort the run
		std::sort(items.begin(), items.end(), CompareSorter(compare));//利用vector自带的排序算法，std:stable_sort

		// Did everything fit?
		if ((reader == limit) && (runs.empty())) {
			spool(0, out, items, eliminateDuplicates);//最终./test读取结束后将保存到items中的最后一小部分写入out（./sortedByS）中，
			break;
		}

		// No, spool to intermediate file
		char* newOfs = spool(ofs, intermediate, items, eliminateDuplicates);//将items写入out（./sortedBySubject.1）中，
		runs.push_back(Range(ofs, newOfs));
		ofs = newOfs;
	}
	intermediate.close();
	mappedIn.close();

	// Do we have to merge runs?
	if (!runs.empty()) {
		// Map the ranges
		MemoryMappedFile tempIn;
		assert(tempIn.open(intermediate.getFile().c_str()));
		for (vector<Range>::iterator iter = runs.begin(), limit = runs.end(); iter != limit; ++iter) {
			(*iter).from = tempIn.getBegin() + ((*iter).from - static_cast<char*> (0));
			(*iter).to = tempIn.getBegin() + ((*iter).to - static_cast<char*> (0));
		}

		// Sort the run heads
		std::sort(runs.begin(), runs.end(), CompareSorter(compare));

		// And merge them
		Range last(0, 0);
		while (!runs.empty()) {
			// Write the first entry if no duplicate
			Range head(runs.front().from, skip(runs.front().from));
			if ((!eliminateDuplicates) || (!last.equals(head)))
				out.write(head.to - head.from, head.from);
			last = head;

			// Update the first entry. First entry done?
			if ((runs.front().from = head.to) == runs.front().to) {
				runs[0] = runs[runs.size() - 1];
				runs.pop_back();
			}

			// Check the heap condition
			unsigned pos = 0, size = runs.size();
			while (pos < size) {
				unsigned left = 2 * pos + 1, right = left + 1;
				if (left >= size)
					break;
				if (right < size) {
					if (compare(runs[pos].from, runs[left].from) > 0) {
						if (compare(runs[pos].from, runs[right].from) > 0) {
							if (compare(runs[left].from, runs[right].from) < 0) {
								std::swap(runs[pos], runs[left]);
								pos = left;
							} else {
								std::swap(runs[pos], runs[right]);
								pos = right;
							}
						} else {
							std::swap(runs[pos], runs[left]);
							pos = left;
						}
					} else if (compare(runs[pos].from, runs[right].from) > 0) {
						std::swap(runs[pos], runs[right]);
						pos = right;
					} else
						break;
				} else {
					if (compare(runs[pos].from, runs[left].from) > 0) {
						std::swap(runs[pos], runs[left]);
						pos = left;
					} else
						break;
				}
			}
		}
		tempIn.close();
	}

	intermediate.discard();
	out.close();
}
