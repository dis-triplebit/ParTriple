#include "SemiJoin.h"
#include "../EntityIDBuffer.h"

bool searchValueInEntMap(ID keyValue, map<ID, EntityIDBuffer*> * entBufferMap,
		int IDCount2, int joinKey2) {
	map<ID, EntityIDBuffer*>::iterator iter = entBufferMap->begin();
	for (; iter != entBufferMap->end(); iter++) {
		EntityIDBuffer* ent = iter->second;
		int length2 = ent->getSize();
		ID *p = ent->getBuffer();
		for (int i = 0; i < length2; i++) {
			if (p[i * IDCount2 + joinKey2] == keyValue)
				return true;
		}
	}
	return false;
}
int searchValueInEnt(ID keyValue, EntityIDBuffer* ent, int IDCount2,
		int joinKey2) {
	return ent->getEntityIDPos(keyValue);
}
void SemiJoin::Join(EntityIDBuffer* entBuffer1,
		map<ID, EntityIDBuffer*>* entBufferMap, int joinKey1, int joinKey2) {
	joinKey1--;
	joinKey2--;
	if (entBufferMap->size() <= 0 || entBuffer1->getSize() <= 0)
		return;
	if (entBuffer1->getSortKey() == joinKey1
			&& entBufferMap->begin()->second->getSortKey() == joinKey2) {
		//两者均按照链接变量有序
	} else if (entBuffer1->getSortKey() == joinKey1
			&& entBufferMap->begin()->second->getSortKey() != joinKey2) {
		//前者按照链接变量有序，而后者相当于无序
		//cout << "semi join case3" << endl;
		register ID keyValue = 0;
		ID* buffer1 = entBuffer1->getBuffer();
		size_t length1 = entBuffer1->getSize();
		int IDCount1 = entBuffer1->getIDCount();
		ID* temp1 = (ID*) malloc(4096 * sizeof(ID));
		size_t i = 0;
		size_t pos1 = 0;
		size_t size1 = 0;
		int IDCount2 = entBufferMap->begin()->second->getIDCount();
		while (i < length1) {

			keyValue = buffer1[i * IDCount1 + joinKey1];
			//如果能找到,将这个keyValue的所有行全部拷贝到temp1中
			if (searchValueInEntMap(keyValue, entBufferMap, IDCount2,
					joinKey2)) {

				do {
					if (pos1 == 4096) {
						memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
						size1 = size1 + pos1;
						pos1 = 0;
					}

					for (int k = 0; k < IDCount1; k++) {
						temp1[pos1] = buffer1[i * IDCount1 + k];
						pos1++;
					}
					i++;
					//cout << "pos1 is" << pos1 << endl;
				} while (buffer1[i * IDCount1 + joinKey1] == keyValue
						&& i < length1);
				continue;

			} else {
				do {
					i++;
				} while (buffer1[i * IDCount1 + joinKey1] == keyValue
						&& i < length1);
			}

		}
		//cout << "enter" << IDCount1 << endl;
		memcpy(buffer1 + size1, temp1, pos1 * sizeof(ID));
		size1 = size1 + pos1;
		entBuffer1->usedSize = size1;
		free(temp1);

	} else if (entBuffer1->getSortKey() != joinKey1
			&& entBufferMap->begin()->second->getSortKey() == joinKey2) {
		//前者按照链接变量无序，而后者相当于有序

	} else {
		//均无序

	}
}
void SemiJoin::Join(EntityIDBuffer* entBuffer1, EntityIDBuffer*entBuffer2,
		int joinKey1, int joinKey2, bool sort1, bool sort2, bool modify1,
		bool modify2) {
	joinKey1--;
	joinKey2--;
	if (sort1 && !sort2)
		Join10(entBuffer1, entBuffer2, joinKey1, joinKey2, modify1, modify2);
	else if (!sort1 && sort2) {
		Join01(entBuffer1, entBuffer2, joinKey1, joinKey2, modify1, modify2);
	}
}
void SemiJoin::Join10(EntityIDBuffer* entBuffer1, EntityIDBuffer*entBuffer2,
		int joinKey1, int joinKey2, bool modify1, bool modify2) {
//ent1有序，ent2无序
	Join01(entBuffer2, entBuffer1, joinKey2, joinKey1, true, true);
}
void SemiJoin::Join01(EntityIDBuffer* entBuffer1, EntityIDBuffer*entBuffer2,
		int joinKey1, int joinKey2, bool modify1, bool modify2) {

//ent2有序，ent1无序
	int sortkey1 = entBuffer1->getSortKey();
	ID sortkey2 = entBuffer2->getSortKey();
	cout << "sortKey2:" << sortkey2 << "     joinkey2:" << joinKey2 << endl;
	cout << "sortKey1:" << entBuffer1->getSortKey() << "     joinkey1:"
			<< joinKey1 << endl;
	map<ID, bool> hasfind;
	assert(modify1 == true);
	if (modify2 == false) {
		register ID keyValue = 0;
		ID* buffer1 = entBuffer1->getBuffer();
		size_t length1 = entBuffer1->getSize();
		int IDCount1 = entBuffer1->getIDCount();
		ID* temp1 = (ID*) malloc(4096 * sizeof(ID));
		size_t i = 0, size1 = 0;
		size_t pos1 = 0;
		size_t length2 = entBuffer2->getSize();
		while (i < length1) {
			keyValue = buffer1[i * IDCount1 + joinKey1];
			//如果能找到,将这个keyValue的所有行全部拷贝到temp1中
			if (hasfind.count(keyValue) == 0) {
				if (entBuffer2->isInBuffer(keyValue)) {
					//ent1无序，且对应的keyValue在有序的ent2中找到
					hasfind[keyValue] = true;
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						if (pos1 == 4096) {
							memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
							size1 = size1 + pos1;
							pos1 = 0;
						}
						for (int k = 0; k < IDCount1; k++) {
							temp1[pos1] = buffer1[i * IDCount1 + k];
							pos1++;
						}
						i++;
					}

				} else {
					hasfind[keyValue] = false;
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						i++;
					}
				}
			} else {
				if (hasfind[keyValue]) {
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						if (pos1 == 4096) {
							memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
							size1 = size1 + pos1;
							pos1 = 0;
						}
						for (int k = 0; k < IDCount1; k++) {
							temp1[pos1] = buffer1[i * IDCount1 + k];
							pos1++;
						}
						i++;
					}
				} else {
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						i++;
					}
				}
			}
		}
		memcpy(buffer1 + size1, temp1, pos1 * sizeof(ID));
		size1 += pos1;
		entBuffer1->usedSize = size1;
		free(temp1);

	} else {
		register ID keyValue = 0;
		ID* buffer1 = entBuffer1->getBuffer();
		ID* buffer2=  entBuffer2->getBuffer();
		size_t length1 = entBuffer1->getSize();
		int IDCount1 = entBuffer1->getIDCount();
		int IDCount2 = entBuffer2->getIDCount();
		ID* temp1 = (ID*) malloc(4096 * sizeof(ID));
		size_t i = 0, size1 = 0;
		size_t pos1 = 0;

		size_t length2 = entBuffer2->getSize();
		char *flag2 = (char*) malloc(length2);
		memset(flag2, 0, length2);
		while (i < length1) {
			keyValue = buffer1[i * IDCount1 + joinKey1];
			//如果能找到,将这个keyValue的所有行全部拷贝到temp1中
			if (hasfind.count(keyValue) == 0) {
				size_t pos = entBuffer2->getEntityIDPos(keyValue);
				if (pos != size_t(-1)) {
					hasfind[keyValue] = true;
					if (flag2[pos] == 0) {
						while (pos < length2&&buffer2[pos*IDCount2+joinKey2]==keyValue) {
							flag2[pos]++;
							pos++;
						}
					}
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						if (pos1 == 4096) {
							memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
							size1 = size1 + pos1;
							pos1 = 0;
						}

						for (int k = 0; k < IDCount1; k++) {
							temp1[pos1] = buffer1[i * IDCount1 + k];
							pos1++;
						}
						i++;
					}
					/*register ID keyValue11 = buffer1[i * IDCount1 + sortkey1];
					 while (buffer1[i * IDCount1 + sortkey1] == keyValue11
					 && i < length1) {
					 i++;
					 }*/
				} else {
					hasfind[keyValue] = false;
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						i++;
					}
				}
			} else {
				if (hasfind[keyValue]) {
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						if (pos1 == 4096) {
							memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
							size1 = size1 + pos1;
							pos1 = 0;
						}

						for (int k = 0; k < IDCount1; k++) {
							temp1[pos1] = buffer1[i * IDCount1 + k];
							pos1++;
						}
						i++;
					}
				} else {
					while (buffer1[i * IDCount1 + joinKey1] == keyValue
							&& i < length1) {
						i++;
					}
				}
			}

		}
		memcpy(buffer1 + size1, temp1, pos1 * sizeof(ID));
		size1 += pos1;
		entBuffer1->usedSize = size1;
		entBuffer2->modifyByFlag2(flag2);
		free(temp1);
		free(flag2);
	}
}
void SemiJoin::Join00(EntityIDBuffer* entBuffer1, EntityIDBuffer*entBuffer2,
		int joinKey1, int joinKey2, bool modify1, bool modify2) {
//两者都无序

}
