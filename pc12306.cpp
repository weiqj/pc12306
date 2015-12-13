/*
 * pc12306.cpp
 *
 *  Created on: Dec 12, 2015
 *      Author: weiqj
 */

#include <sys/time.h>
#include <sys/resource.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <vector>
#include <stdexcept>
#include "pc12306.h"

using namespace std;

typedef void (*QWThreadStartRoutine)(void*);
struct QWThreadDataStd {
public:
	QWThreadDataStd(QWThreadStartRoutine startRoutine, void *arg) :
		_startRoutine(startRoutine),
		_arg(arg) {
	}

	~QWThreadDataStd() {
	}

	QWThreadStartRoutine		_startRoutine;
	void *						_arg;
};
static void *__QWThreadStartStd(void *arg) {
	QWThreadDataStd *pData = (QWThreadDataStd *)arg;
	pData->_startRoutine(pData->_arg);
	delete pData;
	return NULL;
}
static pthread_t createThreadStd(
		QWThreadStartRoutine startRoutine,
		void *arg) {
	QWThreadDataStd *data = new QWThreadDataStd(startRoutine, arg);
	pthread_t thread;
	int result = pthread_create(&thread, NULL, &__QWThreadStartStd, data);
	if (result != 0) {
		delete data;
		throw std::runtime_error("QWThread.createThread");
	}
	return thread;
}

struct SearchOffsets {
	int32_t		start;
	int32_t		length;
};

static size_t nSearches = 0;
typedef vector<SearchOffsets> Offsets;
static Offsets offsets;
static TicketPool *ticketPool = NULL;

static void generateSearchPatterns() {
	offsets.clear();
	offsets.push_back({0, 0});		// Just matches
	for (int i = 1; i<SEGMENTS; i++) {
		for (int j = 0; j<SEGMENTS; j++) {
			int k = i - j;
			if (k >= 0) {
				offsets.push_back({-j, k});
			} else {
				break;
			}
		}
	}
	nSearches = offsets.size();
	printf("Total %d\n", (int)nSearches);
	for (Offsets::iterator it = offsets.begin(); it != offsets.end(); ++it) {
		printf("%d    %d    %d\n", it->start ,it->length, it->length - it->start);
	}
}

void TrainTicketMap::initTickets(TicketPool *tp) {
	Ticket *prev = NULL;
	for (int i=0; i<SEATS; i++) {
		Ticket *t = tp->allocate();
		t->_start = 0;
		t->_length = SEGMENTS;
		t->_seat = i + 1;
		t->_next = NULL;
		if (NULL == prev) {
			_map[0 + (SEGMENTS - 1) * SEGMENTS] = t;
		} else {
			prev->_next = t;
		}
		prev = t;
	}
}

Ticket *TrainTicketMap::allocate(int start, int length) {
	Ticket *ret = NULL;
	length--;		// normalize to [0, 9]
	for (Offsets::iterator it = offsets.begin(); it != offsets.end(); ++it) {
		register int curStart = start + it->start;
		register int curLength = length - it->start + it->length;
		if (curStart >= 0 && curLength < SEGMENTS) {
			//printf("Searching %d %d\n", curStart, curLength);
			size_t index = curStart + curLength * SEGMENTS;
			if (NULL != _map[index]) {
				ret = _map[index];
				break;
			}
		}
	}
	return ret;
}

static TrainTicketMap *trains[TRAINS];

static double getTime() {
    struct timeval t;
    struct timezone tzp;
    gettimeofday(&t, &tzp);
    return t.tv_sec + t.tv_usec*1e-6;
}

static void testReserve(int train, int start, int length) {
	printf("Reserving Start=%d Length=%d\n", start, length);
	Ticket *t = trains[train]->allocate(start, length);
	if (NULL != t) {
		t = trains[train]->reserve(start, length, ticketPool, t);
		assert(t);
		printf("Succeed number=%d start=%d length=%d\n", t->_seat, t->_start, t->_length);
		ticketPool->free(t);
	} else {
		printf("Failed\n");
	}
}

static void test1() {
	for (int i=0; i<SEATS + 2; i++) {
		testReserve(0, 3, 1);
		testReserve(0, 0, 3);
		testReserve(0, 4, 6);
	}
}

static void benchmark() {
	double t1 = getTime();
	printf("start benchmark\n");
	for (size_t i=0; i<100000000LL; i++) {
		int train = rand() % TRAINS;
		int start = rand() % SEGMENTS;
		int length = rand() % (SEGMENTS - start);
		if (length == 0) {
			length = 1;
		}
		assert((start + length) <= SEGMENTS);
		Ticket *t = trains[train]->allocate(start, length);
		if (NULL != t) {
			t = trains[train]->reserve(start, length, ticketPool, t);
			assert(t);
			ticketPool->free(t);
		}
	}
	double t2 = getTime();
	printf("Total time = %f\n", t2 - t1);
}

static bool volatile terminatePocess = false;
static bool volatile terminated1 = false;
static bool volatile terminated2 = false;

#define CLIENT_REQ_SIZE	1024*1024

static ClientReq			clientReqs[CLIENT_REQ_SIZE];
static uint64_t volatile	clientReqPos;

static void iohandler(void *) {
	typedef vector<ClientSession *> Sessions;
	int _fdListen = socket(AF_INET,SOCK_STREAM,IPPROTO_TCP);
	if (_fdListen < 0)
		throw std::runtime_error("SRServerScheduler: Socket");
	int optval = 1;
	setsockopt(_fdListen, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(PORT);
	if (::bind(_fdListen, (const sockaddr *)(&addr), sizeof(sockaddr_in)) < 0)
		throw std::runtime_error("SRServerScheduler: Bind");
	int flags = fcntl(_fdListen, F_GETFL, 0);
	fcntl(_fdListen, F_SETFL, flags | O_NONBLOCK);
	if (listen(_fdListen, 10) < 0)
		throw std::runtime_error("SRServerScheduler: Listen");
	while (!terminatePocess) {
		sockaddr_in addr;
		socklen_t addrlen = (socklen_t)sizeof(sockaddr_in);
		int fd = accept(_fdListen, (sockaddr *)&addr, &addrlen);
		if (fd < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				// ignore
			} else {
				printf("Error accepting socket.\n");
			}
		} else {

		}
	}
	terminated1 = true;
}

static void tickethandler(void *) {
	uint64_t curPos = 0;
	while (!terminatePocess) {
		while (curPos < clientReqPos) {
			ClientReq &curReq = clientReqs[curPos % CLIENT_REQ_SIZE];
			curPos++;
			int seat = -1;
			register int start = curReq._req._start;
			register int length = curReq._req._length;
			register int train = curReq._req._train;
			if (start >= 0 &&
					length > 0 &&
					(start + length) <= SEGMENTS &&
					train >= 0 &&
					train < TRAINS) {
				Ticket *t = trains[train]->allocate(start, length);
				if (NULL != t) {
					t = trains[train]->reserve(start, length, ticketPool, t);
					seat = t->_seat;
					assert(t);
					ticketPool->free(t);
				}
			}
			curReq._session->sendResponse(curReq._req._reqID, seat);
		}
	}
	terminated2 = true;
}

int main(int argc, char* argv[]) {
	ticketPool = new TicketPool();
	generateSearchPatterns();
	for (int i=0; i<TRAINS; i++) {
		trains[i] = new TrainTicketMap();
		trains[i]->initTickets(ticketPool);
	}
	//test1();
	//benchmark();
	createThreadStd(&iohandler, NULL);
	for (;;) {
		sleep(1000);
	}
	return 0;
}

