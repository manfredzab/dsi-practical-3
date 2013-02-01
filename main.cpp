// --------------------------------- INCLUDES ---------------------------------
#include <stdlib.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"

#include "timer.h"
// ----------------------------------------------------------------------------


// -------------------------------- DEFINITIONS -------------------------------
// Minibase definitions
int MINIBASE_RESTART_FLAG = 0; // Used in Minibase part
#define NUM_OF_DB_PAGES  2000  // # of DB pages

// Performance analyser definitions
#define RUN_TESTS           1  // Test mode ON/OFF
#define REPETITION_COUNT    5  // Number of repetitions for each algorithm

#if RUN_TESTS
#include "include/jointest.h"
#endif
// ----------------------------------------------------------------------------


// ----------------------------------- ENUMS ----------------------------------
enum JoinAlgorithm
{
	TUPLE_NESTED_LOOP,
	BLOCK_NESTED_LOOP,
	INDEX_NESTED_LOOP
};
// ----------------------------------------------------------------------------


// ------------------------------- DECLARATIONS -------------------------------
void AnalysePerformance(JoinAlgorithm algorithmType,
						int numOfBufPages,
						int numOfRecInR,
						int numOfRecInS,
						double& elapsedTime,
						long& pinCount,
						long& missCount);
// ----------------------------------------------------------------------------


// ---------------------------------- METHODS ---------------------------------
int main()
{
#if RUN_TESTS
	return RunTests();
#endif

	// Initialise random seed
	srand(1);

	JoinAlgorithm joinAlgorithms[] = { TUPLE_NESTED_LOOP, BLOCK_NESTED_LOOP, INDEX_NESTED_LOOP };

	for (int algorithmIndex = 0; algorithmIndex < 3; algorithmIndex++)
	//int algorithmIndex = 1;
	{
		JoinAlgorithm joinAlgorithm = joinAlgorithms[algorithmIndex];
		for (int bufferPoolSize = 10; bufferPoolSize <= NUM_OF_DB_PAGES; bufferPoolSize *= 2)
		//int bufferPoolSize = 50;
		{
			for (int numberOfRecordsInR = 4; numberOfRecordsInR <= 16384; numberOfRecordsInR *= 2)
			//int numberOfRecordsInR = 10000;
			{
				for (int numberOfRecordsInS = 4; numberOfRecordsInS <= 16384; numberOfRecordsInS *= 2)
				//int numberOfRecordsInS = numberOfRecordsInR / 4;
				{
					cout << "Algorithm:\t" << algorithmIndex << "\n";
					cout << "Buffer pool size:\t" << bufferPoolSize << "\n";
					cout << "Number of records in R:\t" << numberOfRecordsInR << "\n";
					cout << "Number of records in S:\t" << numberOfRecordsInS << "\n";

					double avgElapsedTime = 0.0, avgPinCount = 0.0, avgMissCount = 0.0;
					for (int repetition = 0; repetition < REPETITION_COUNT; repetition++)
					{
						double elapsedTime;
						long pinCount, missCount;

						AnalysePerformance(joinAlgorithm, bufferPoolSize, numberOfRecordsInR, numberOfRecordsInS, elapsedTime, pinCount, missCount);

						avgElapsedTime += elapsedTime / (double)REPETITION_COUNT;
						avgPinCount += (double)pinCount / (double)REPETITION_COUNT;
						avgMissCount += (double)missCount / (double)REPETITION_COUNT;

					cout << "Average elapsed time:\t" << avgElapsedTime << "\n";
					cout << "Average miss/pin count:\t" << avgMissCount << "\t" << avgPinCount << "\n\n";
					
					}
				}
			}
		}
	}

	return 0;
}

//
// Input:  algorithmType - join algorithm type,
//         numOfBufPages - number of buffer pages,
//         numOfRecInR   - number of records for relation R,
//         numOfRecInS   - number of records for relation S.
//
// Output: elapsedTime   - elapsed time to perform the join,
//         pinCount      - number of pages pinned,
//         missCount     - number of misses when pinning the pages
//
void AnalysePerformance(JoinAlgorithm algorithmType,
						int numOfBufPages,
						int numOfRecInR,
						int numOfRecInS,
						double& elapsedTime,
						long& pinCount,
						long& missCount)
{
	HeapFile* (*joinAlgorithm) (JoinSpec, JoinSpec);
	switch (algorithmType)
	{
		case TUPLE_NESTED_LOOP: joinAlgorithm = &TupleNestedLoopJoin; break;
		case BLOCK_NESTED_LOOP: joinAlgorithm = &BlockNestedLoopJoin; break;
		case INDEX_NESTED_LOOP: joinAlgorithm = &IndexNestedLoopJoin; break;
	}

	// Remove MINIBASE.DB if it exists
	std::remove("MINIBASE.DB");

	Status s;

	// Initialise Minibase's Global Variables
	minibase_globals = new SystemDefs(s, 
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		numOfBufPages,  // Number of frames in buffer pool
		NULL);

	// Create Random Relations R(outer relation) and S for joining. The definition is in relation.h/
	CreateR(numOfRecInR, numOfRecInS);
	CreateS(numOfRecInR, numOfRecInS);

	// Initialize the specification for joins
	JoinSpec specOfS, specOfR;

	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	// Create a timer for time measurement
	Timer timer;

	// Join and collect statistics
	MINIBASE_BM->ResetStat();

	timer.start();
	// =================== TIMED SECTION ===================

	joinAlgorithm(specOfR, specOfS);

	// ================ END OF TIMED SECTION ===============
	elapsedTime = timer.stop();

	// Obtain statistics
	MINIBASE_BM->GetStat(pinCount, missCount);
//	cout << elapsedTime << "\n";
//	cout << missCount << "/" << pinCount << "\n";
	delete minibase_globals;
}
// ----------------------------------------------------------------------------
