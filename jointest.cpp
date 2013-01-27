#include <stdlib.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"
#include "include/scan.h"

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis

void PrintVerboseInfo(JoinSpec specOfS, JoinSpec specOfR, HeapFile* joinedFile);
void SaveJoinedRelToFile(JoinSpec specOfR, JoinSpec specOfS, HeapFile* joinedFile, const char* resultFileNameString);
int AreFilesEqual(const char* fileNameA, const char* fileNameB);
int CountJoinedRecords(HeapFile* file);

int RunTests()
{
	// Remove MINIBASE.DB if it exists
	std::remove("MINIBASE.DB");

	Status s;

	// Initialize Minibase's Global Variables
	minibase_globals = new SystemDefs(s,
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		NUM_OF_BUF_PAGES,  // Number of frames in buffer pool
		NULL);

	// Initialize random seed
	srand(1);

	// Create Random Relations R(outer relation) and S for joining. The definition is in relation.h,
	// # of tuples: NUM_OF_REC_IN_R, NUM_OF_REC_IN_S in join.h
	CreateR();
	CreateS();

	// Initialize the specification for joins
	JoinSpec specOfS, specOfR;

	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	// File names for comparison
	const char* nestedTupleFileName = "nestedTuple";
	const char* nestedBlockFileName = "nestedBlock";
	const char* nestedIndexFileName = "nestedIndex";

	// Join
	HeapFile* tupleJoinedFile = TupleNestedLoopJoin(specOfR, specOfS);
	//PrintVerboseInfo(specOfR, specOfS, tupleJoinedFile);
	SaveJoinedRelToFile(specOfR, specOfS, tupleJoinedFile, nestedTupleFileName);
	tupleJoinedFile->DeleteFile();

	HeapFile* blockJoinedFile = BlockNestedLoopJoin(specOfR, specOfS, (MINIBASE_BM->GetNumOfUnpinnedBuffers() - 3 * 3) * MINIBASE_PAGESIZE);
	//PrintVerboseInfo(specOfR, specOfS, blockJoinedFile);
	SaveJoinedRelToFile(specOfR, specOfS, blockJoinedFile, nestedBlockFileName);
	blockJoinedFile->DeleteFile();

	HeapFile* indexJoinedFile = IndexNestedLoopJoin(specOfR, specOfS);
	//PrintVerboseInfo(specOfR, specOfS, indexJoinedFile);
	SaveJoinedRelToFile(specOfR, specOfS, indexJoinedFile, nestedIndexFileName);
	indexJoinedFile->DeleteFile();

	if (!AreFilesEqual(nestedTupleFileName, nestedBlockFileName))
	{
		cout << "PASS: nested tuple join and nested block joins yield equivalent results.\n";
	}
	else
	{
		cerr << "FAIL: nested tuple join and nested block joins DO NOT yield equivalent results (see files " << nestedTupleFileName << ", " << nestedBlockFileName << ").\n";
	}

	if (!AreFilesEqual(nestedBlockFileName, nestedIndexFileName))
	{
		cout << "PASS: nested block join and nested index joins yield equivalent results.\n";
	}
	else
	{
		cerr << "FAIL: nested block join and nested index joins DO NOT yield equivalent results (see files " << nestedBlockFileName << ", " << nestedIndexFileName << ").\n";
	}

	if (!AreFilesEqual(nestedIndexFileName, nestedTupleFileName))
	{
		cout << "PASS: nested index join and nested tuple joins yield equivalent results.\n";
	}
	else
	{
		cerr << "FAIL: nested index join and nested tuple joins DO NOT yield equivalent results (see files " << nestedIndexFileName << ", " << nestedTupleFileName << ").\n";
	}

	return 0;
}


void SaveJoinedRelToFile(JoinSpec specOfR, JoinSpec specOfS, HeapFile* joinedFile, const char* resultFileNameString)
{
	char* resultFileName = new char[255];
	strcpy(resultFileName, resultFileNameString);

	HeapFile* sortedJoinedFile = SortFile(joinedFile, specOfR.recLen + specOfS.recLen, 0);
	PrintResult(sortedJoinedFile, resultFileName, false);
	sortedJoinedFile->DeleteFile();
}


void PrintVerboseInfo(JoinSpec specOfR, JoinSpec specOfS, HeapFile* joinedFile)
{
	// Joined relation file name
	char* resultFileName = new char[3];
	strcpy(resultFileName, "RS");

	// Print out relations
	cout << "\nRelation R [Employee (id, age, proj, salary, rating, dept)]:\n";
	PrintR(specOfR.file, specOfR.relName);

	cout << "\nRelation S [Project (id, fund, manager, status)]:\n";
	PrintS(specOfS.file, specOfS.relName);

	cout << "\nJoined relation [Employee.proj = Project.id] (E.proj, R.id, E.id):\n";
	PrintResult(joinedFile, resultFileName);
}


int CountJoinedRecords(HeapFile* file)
{
	Status status = OK;
	Scan *scan = file->OpenScan(status);
	if (status != OK)
	{
		cerr << "Cannot open scan on result HeapFile." << endl;
	}

	EmployeeProject e;
	int len;
	RecordID rid;

	int recordCount = 0;
	while (scan->GetNext(rid, (char *)&e, len) != DONE)
	{
		recordCount++;
	}

	delete scan;
	return recordCount;
}


// Returns true if files are different and false if files are equal
bool AreFilesEqual(const char* fileNameA, const char* fileNameB)
{
	FILE* fileA = fopen(fileNameA, "r");
	FILE* fileB  = fopen(fileNameB, "r");

    bool filesAreDifferent = false;
    int bufferSize = 65536;

    char* buffer1 = (char*)calloc(1, bufferSize + 1);
    char* buffer2 = (char*)calloc(1, bufferSize + 1);

    size_t s1, s2;

    do
    {
		s1 = fread(buffer1, 1, bufferSize, fileA);
		s2 = fread(buffer2, 1, bufferSize, fileB);

		if (s1 != s2 || memcmp(buffer1, buffer2, s1))
		{
			filesAreDifferent = true;
			break;
		}
    }
    while (!feof(fileA) || !feof(fileB));

    free(buffer1);
    free(buffer2);

    fclose(fileA);
    fclose(fileB);

    return filesAreDifferent;
}
