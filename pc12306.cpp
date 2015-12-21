/*
 * pc12306.cpp
 *
 *  Created on: Dec 12, 2015
 *      Author: weiqj
 */

#include <sys/time.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/ioctl.h>
#include <sys/epoll.h>
#include <signal.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <poll.h>
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

typedef vector<SearchOffsets> Offsets;
static Offsets offsets;
static TicketPool *ticketPool = NULL;
static bool volatile terminatePocess = false;
static bool volatile terminated1 = false;
static bool volatile terminated2 = false;

static ClientReq *			clientReqs = NULL;
static uint64_t volatile	clientReqPos = 0;
static uint64_t volatile	clientReqProcessed = 0;

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
	/*
	size_t nSearches = offsets.size();
	printf("Total %d\n", (int)nSearches);
	for (Offsets::iterator it = offsets.begin(); it != offsets.end(); ++it) {
		printf("%d    %d    %d\n", it->start ,it->length, it->length - it->start);
	}
	*/
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

size_t ClientSession::maxRead() const {
	size_t ret;
	size_t sendPos = _respSent / sizeof(NetResp);	// Sent resp messages
	size_t pending = _reqPos - sendPos;				// Reqs not processed
	assert(pending <= SESSION_QUEUE_SIZE);
	size_t available = SESSION_QUEUE_SIZE - pending;	// How many more we can read
	if (available <= 4) {
		printf("read stall\n");
		ret = 0;
	} else {
		ret = available - 4;
	}
	ssize_t p = (ssize_t)(clientReqPos - clientReqProcessed);
	if (p > 0) {
		available = 0;
		if ((size_t)p < GLOBAL_QUEUE_SIZE) {
			available = GLOBAL_QUEUE_SIZE - (size_t)p;
		}
		if (available <= 4) {
			available = 0;
		} else {
			available -= 4;
		}
		if (ret > available) {
			ret = available;
		}
	}
	return ret;
}

static TrainTicketMap *trains[TRAINS];

static void iohandler(void *) {
	typedef vector<ClientSession *> Sessions;
	Sessions sessions1;
	Sessions sessions2;

	Sessions &sessions = sessions1;
	Sessions &sessionsValid = sessions2;
	int _fdListen = socket(AF_INET,SOCK_STREAM,IPPROTO_TCP);
	if (_fdListen < 0)
		throw std::runtime_error("Socket");
	int optval = 1;
	setsockopt(_fdListen, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(PORT);
	if (::bind(_fdListen, (const sockaddr *)(&addr), sizeof(sockaddr_in)) < 0)
		throw std::runtime_error("Bind");
	int flags = fcntl(_fdListen, F_GETFL, 0);
	fcntl(_fdListen, F_SETFL, flags | O_NONBLOCK);
	if (listen(_fdListen, 10) < 0)
		throw std::runtime_error("Listen");

	int efdr = epoll_create(MAX_CONN);
	int efdw = epoll_create(MAX_CONN);

	{
		epoll_event ev;
		ev.events = EPOLLIN;
		ev.data.fd = _fdListen;
		ev.data.ptr = NULL;
		epoll_ctl(efdr, EPOLL_CTL_ADD, _fdListen, &ev);
	}
	clientReqPos = 0;
	struct epoll_event events[MAX_CONN];
	while (!terminatePocess) {
		double curTime = getTime();
		// Write first
		for (Sessions::iterator it = sessions.begin(); it != sessions.end(); ++it) {
			if (!(*it)->release()) {
				// Write
				if ((*it)->_canWrite) {
					if ((*it)->writeResponses() < 0) {
						(*it)->setRelease();
						epoll_ctl(efdr, EPOLL_CTL_DEL, (*it)->_fd, NULL);
						epoll_ctl(efdw, EPOLL_CTL_DEL, (*it)->_fd, NULL);
					}
				}
			}
		}
		// Read
		bool canRead = false;
		size_t waitingWrite = 0;
		size_t writePending = 0;
		sessionsValid.clear();
		for (Sessions::iterator it = sessions.begin(); it != sessions.end(); ++it) {
			if (!(*it)->release()) {
				if ((*it)->_canRead) {
					ssize_t res = (*it)->readReq();
					if (res > 0) {
						(*it)->_lastRecvTime = curTime;
					} else if (res < 0) {
						(*it)->setRelease();
						epoll_ctl(efdr, EPOLL_CTL_DEL, (*it)->_fd, NULL);
						epoll_ctl(efdw, EPOLL_CTL_DEL, (*it)->_fd, NULL);
					}
				}
				if (!(*it)->release()) {
					while ((*it)->_reqProcessed < (*it)->_reqPos) {
						NetReq &cur = (*it)->_reqs[(*it)->_reqProcessed % SESSION_QUEUE_SIZE];
						(*it)->_reqProcessed++;
						ClientReq &cr = clientReqs[clientReqPos % GLOBAL_QUEUE_SIZE];
						cr._session = *it;
						cr._req = cur;
						asm volatile("" ::: "memory");
						clientReqPos++;
					}
					if ((curTime - (*it)->_lastRecvTime) > SESSION_TIMEOUT_SECONDS) {
						printf("Session timeout\n");
						(*it)->setRelease();
						epoll_ctl(efdr, EPOLL_CTL_DEL, (*it)->_fd, NULL);
						epoll_ctl(efdw, EPOLL_CTL_DEL, (*it)->_fd, NULL);
					}
				}
			}
			if ((*it)->release() && (*it)->_reqPos == (*it)->_respPos) {
				(*it)->close();
				delete (*it);
				printf("Session closed\n");
			} else {
				sessionsValid.push_back(*it);
				if (!(*it)->release()) {
					canRead |= (*it)->_canRead;
					size_t respPos = (*it)->_respPos;
					size_t respSize = respPos * sizeof(NetResp);
					if (respSize != (*it)->_respSent) {
						writePending++;
						if (!(*it)->_canWrite)
							waitingWrite++;
					}
				}
			}
		}
		std::swap(sessions, sessionsValid);
		if (waitingWrite > 0 && writePending == waitingWrite) {
			int nfds = epoll_wait(efdw, events, MAX_CONN, 1);
			for (int i=0; i<nfds; i++) {
				ClientSession *cur = reinterpret_cast<ClientSession *>(events[i].data.ptr);
				if (0 != (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))) {
					cur->setRelease();
					epoll_ctl(efdr, EPOLL_CTL_DEL, cur->_fd, NULL);
					epoll_ctl(efdw, EPOLL_CTL_DEL, cur->_fd, NULL);
				} else {
					cur->_canWrite = true;
				}
			}
		}
		if (!canRead) {
			int nfds = epoll_wait(efdr, events, MAX_CONN, 1);
			//printf("Read wait\n");
			for (int i=0; i<nfds; i++) {
				ClientSession *cur = reinterpret_cast<ClientSession *>(events[i].data.ptr);
				if (NULL == cur) {
					sockaddr_in addr;
					socklen_t addrlen = (socklen_t)sizeof(sockaddr_in);
					int fd = accept(_fdListen, (sockaddr *)&addr, &addrlen);
					if (fd < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK) {
							// ignore
						} else {
							printf("Error accepting socket.\n");
						}
						break;
					} else {
						if (sessions.size() > MAX_CONN) {
							printf("Max TCP reached.\n");
							::close(fd);
						} else {
							ClientSession *session = new ClientSession(fd);
							epoll_event ev;
							ev.events = EPOLLIN | EPOLLET;
							ev.data.fd = fd;
							ev.data.ptr = session;
							epoll_ctl(efdr, EPOLL_CTL_ADD, fd, &ev);
							ev.events = EPOLLOUT | EPOLLET;
							epoll_ctl(efdw, EPOLL_CTL_ADD, fd, &ev);
							sessions.push_back(session);
						}
					}
				} else {
					if (0 != (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))) {
						cur->setRelease();
						epoll_ctl(efdr, EPOLL_CTL_DEL, cur->_fd, NULL);
						epoll_ctl(efdw, EPOLL_CTL_DEL, cur->_fd, NULL);
					} else {
						cur->_canRead = true;
					}
				}
			}
		}
	}
	terminated1 = true;
}

static void tickethandler(void *) {
	while (!terminatePocess) {
		while (clientReqProcessed < clientReqPos) {
			ClientReq &curReq = clientReqs[clientReqProcessed % GLOBAL_QUEUE_SIZE];
			int seat = -1;
			register int start = curReq._req._start;
			register int stop = curReq._req._stop;
			register int length = stop - start;
			register int train = curReq._req._train;
			if (start >= 0 &&
					stop > start &&
					stop <= SEGMENTS &&
					train >= 0 &&
					train < TRAINS) {
				Ticket *t = trains[train]->allocate(start, length);
				if (NULL != t) {
					t = trains[train]->reserve(start, length, ticketPool, t);
					seat = t->_seat;
					//assert(t);
					ticketPool->free(t);
				}
			}
			curReq._session->addResponse(
					curReq._req._reqID,
					(int64_t)clientReqProcessed,
					seat);
			asm volatile("" ::: "memory");
			clientReqProcessed++;
		}
	}
	terminated2 = true;
}

static void signal_ignore_handler(int signum){
}
int main(int argc, char* argv[]) {
	signal(SIGPIPE, signal_ignore_handler);
	ticketPool = new TicketPool();
	generateSearchPatterns();
	for (int i=0; i<TRAINS; i++) {
		trains[i] = new TrainTicketMap();
		trains[i]->initTickets(ticketPool);
	}
	//test1();
	//benchmark();
	//if (argc > 1) {
	//	createThreadStd(&clientKB3, argv[1]);
	//} else {
	if(mlockall(MCL_CURRENT|MCL_FUTURE) == -1) {
		perror("mlockall");
		exit(-2);
	}
	clientReqs = new ClientReq[GLOBAL_QUEUE_SIZE];
	createThreadStd(&iohandler, NULL);
	createThreadStd(&tickethandler, NULL);
	//}
	for (;;) {
		sleep(1000);
	}
	return 0;
}
RoCE
