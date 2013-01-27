#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/btfile.h"
#include "../include/btfilescan.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"



//---------------------------------------------------------------
// Each join method takes in at least two parameters :
// - specOfS
// - specOfR
//
// They specify which relations we are going to join, which 
// attributes we are going to join on, the offsets of the 
// attributes etc.  specOfS specifies the inner relation while
// specOfR specifies the outer one.
//
//You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed.  This includes any Scan/BTreeFileScan that 
// you have opened.
//---------------------------------------------------------------


HeapFile* IndexNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS)
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

	char* recR = new char[recLenR];
	char* recS = new char[recLenS];
	char* recJoined = new char[recLenJoined];

	RecordID ridR, ridS, ridJoined;

	// Build the B+-tree index on the inner relation (S)
	Scan* scanS = specOfS.file->OpenScan(status);
	if (OK != status)
	{
		cerr << "ERROR: cannot open scan on the relation S heap file.\n";
		return NULL;
	}

	BTreeFile* bTree = new BTreeFile(status, "IJBT", ATTR_INT, sizeof(int));
	while (OK == scanS->GetNext(ridS, recS, recLenS))
	{
		bTree->Insert(recS + specOfS.offset, ridS);
	}
	delete scanS;

	// Iterate through the outer relation (R) and join
	Scan* scanR = specOfR.file->OpenScan(status);
	if (OK != status)
	{
		cerr << "ERROR: cannot open scan on the relation R heap file.\n";
		return NULL;
	}

	while (OK == scanR->GetNext(ridR, recR, recLenR))
	{
		int* joinArgR = (int*)&recR[specOfR.offset];

		BTreeFileScan* bTreeScan = (BTreeFileScan*)bTree->OpenSearchScan(joinArgR, joinArgR);
		int key;
		while (OK == bTreeScan->GetNext(ridS, &key))
		{
		    specOfS.file->GetRecord(ridS, recS, recLenS);

			MakeNewRecord(recJoined, recR, recS, recLenR, recLenS);
			joinedFile->InsertRecord(recJoined, recLenJoined, ridJoined);
		}
		delete bTreeScan;
	}

	// Release the allocated resources
	delete scanR;

	delete[] recR;
	delete[] recS;
	delete[] recJoined;

	delete bTree;

	return joinedFile;
}
