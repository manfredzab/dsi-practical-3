#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"



HeapFile* BlockNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, int B)
{
	Status status = OK;

	// Create a HeapFile for join results
	HeapFile* joinedFile = new HeapFile(NULL, status);
	if (OK != status)
	{
		cerr << "ERROR: cannot create a file for the joined relation.\n";
		return NULL;
	}

	int recLenR = specOfR.recLen;
	int recLenS = specOfS.recLen;
	int recLenJoined = recLenR + recLenS;

	char* recBlockR = new char[B]; // Allocate memory for the block
	char* recS = new char[recLenS];
	char* recJoined = new char[recLenJoined];

	RecordID ridR, ridS, ridJoined;

	Scan* scanR = specOfR.file->OpenScan(status);
	if (OK != status)
	{
		cerr << "ERROR: cannot open scan on the relation R heap file.\n";
		return NULL;
	}

	const int recordsPerBlock = B / recLenR;

	bool lastBlock = false;
	while (!lastBlock)
	{
		// Fill the block
		int i;
		for (i = 0; i < recordsPerBlock; i++)
		{
			if (OK != scanR->GetNext(ridR, recBlockR + i*recLenR, recLenR))
			{
				lastBlock = true;
				break;
			}
		}
		int lastRecordIndex = i;

		Scan* scanS = specOfS.file->OpenScan(status);
		if (OK != status)
		{
			cerr << "ERROR: cannot open scan on the relation S heap file.\n";
			return NULL;
		}

		while (OK == scanS->GetNext(ridS, recS, recLenS))
		{
			int* joinArgS = (int*)&recS[specOfS.offset];

			for (int currentRecordIndex = 0; currentRecordIndex < lastRecordIndex; currentRecordIndex++)
			{
				char* currentRecordPtr = recBlockR + (currentRecordIndex * recLenR);
				int* joinArgR = (int*)(currentRecordPtr + specOfR.offset);

				if (*joinArgR == *joinArgS)
				{
					MakeNewRecord(recJoined, currentRecordPtr, recS, recLenR, recLenS);
					joinedFile->InsertRecord(recJoined, recLenJoined, ridJoined);
				}
			}
		}

		delete scanS;
	}

	// Release the allocated resources
	delete scanR;

	delete[] recBlockR;
	delete[] recS;
	delete[] recJoined;

	return joinedFile;
}
