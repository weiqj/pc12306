/*
 * pc12306.h
 *
 *  Created on: Dec 12, 2015
 *      Author: weiqj
 */

#ifndef PC12306_H_
#define PC12306_H_

#include <stdint.h>
#include <memory.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>

#ifndef __x86_64__
	#error("X64 arch is required!!!")
#endif

#ifndef SEGMENTS
#define SEGMENTS			10
#endif
#ifndef TRAINS
#define TRAINS				5000
#endif
#ifndef SEATS
#define SEATS				3000
#endif
#ifndef PORT
#define PORT				12306
#endif
#ifndef MAX_CONN
#define MAX_CONN			500
#endif
#ifndef HEARTBEAT_SECONDS
#define HEARTBEAT_SECONDS			5
#endif
#ifndef SESSION_TIMEOUT_SECONDS
#define SESSION_TIMEOUT_SECONDS		10
#endif
#ifndef TARGET_RATE_PER_SECOND
#define TARGET_RATE_PER_SECOND		2000000
#endif
#ifndef TARGET_MAX_LATENCY_SECONDS
#define TARGET_MAX_LATENCY_SECONDS	3
#endif
#ifndef SESSION_QUEUE_SIZE
#define SESSION_QUEUE_SIZE				(TARGET_RATE_PER_SECOND * TARGET_MAX_LATENCY_SECONDS)
#endif
#ifndef GLOBAL_QUEUE_SIZE
#define GLOBAL_QUEUE_SIZE			(SESSION_QUEUE_SIZE * 5)
#endif

#ifdef __GNUC__
#pragma ms_struct on
#endif
#pragma pack(push, 2)
struct NetReq {
	int64_t		_reqID;
	int32_t		_train;		// [0, 5000)
	int16_t		_start;		// [0, 10)
	int16_t		_stop;		// [1, 10]
};

struct NetResp {
	int64_t		_reqID;
	int32_t		_respID;
	int32_t		_seat;
};
#pragma pack(pop)
#ifdef __GNUC__
#pragma ms_struct reset
#endif

struct Ticket {
	int32_t 	_seat;
	int16_t		_start;
	int16_t		_length;
	Ticket *	_next;
};

inline void clflush(volatile void *p) {
    asm volatile ("clflush (%0)" :: "r"(p));
}

inline double getTime() {
    struct timeval t;
    struct timezone tzp;
    gettimeofday(&t, &tzp);
    return t.tv_sec + t.tv_usec*1e-6;
}

class TicketPool {
public:
	TicketPool() {
		const size_t n = (size_t)SEGMENTS * (size_t)TRAINS * (size_t)SEATS;
		for (size_t i=0; i<n; i++) {
			Ticket *cur = &_tickets[i];
			cur->_next = (cur + 1);
		}
		_tickets[n - 1]._next = NULL;
		_head = &(_tickets[0]);
	}

	inline Ticket *allocate() {
		Ticket *ret = _head;
		_head = _head->_next;
		return ret;
	}

	inline void free(Ticket *t) {
		t->_next = _head;
		_head = t;
	}
private:
	Ticket *		_head;
	Ticket			_tickets[SEGMENTS * TRAINS * SEATS];
};

class TrainTicketMap {
public:
	TrainTicketMap() {
		memset(_map, 0, sizeof(Ticket *) * SEGMENTS * SEGMENTS);
	}

	void initTickets(TicketPool *tp);
	Ticket *allocate(int start, int length);		// Start [0, 9], length >= 1 start + length <= 10
	inline Ticket *reserve(int start, int length, TicketPool *tp, Ticket *t) {	// May return NULL in case of serial race, only happens during multi-core scaling
		int index = (t->_length - 1) * SEGMENTS + t->_start; // May add more tickets, if return non-null ticket it shall be returned to pool by caller
		Ticket *ret = _map[index];
		if (NULL != ret) {
			_map[index] = ret->_next;
			{
				int startDelta = start - t->_start;
				if (startDelta > 0) {
					Ticket *p = tp->allocate();
					p->_seat = ret->_seat;
					p->_start = t->_start;
					p->_length = startDelta;
					int index2 = p->_start + (startDelta - 1) * SEGMENTS;
					p->_next = _map[index2];
					_map[index2] = p;
				}
			}
			{
				int tailDelta = t->_start + t->_length - start - length;
				if (tailDelta > 0) {
					Ticket *p = tp->allocate();
					p->_seat = ret->_seat;
					p->_start = start + length;
					p->_length = tailDelta;
					int index2 = p->_start + (tailDelta - 1) * SEGMENTS;
					p->_next = _map[index2];
					_map[index2] = p;
				}
			}
		}
		return ret;
	}
private:
	Ticket *		_map[SEGMENTS * SEGMENTS];
};

class ClientSession {
public:
	ClientSession(int fd) : _fd(fd), _release(false), _reqRead(0), _respSent(0), _respPos(0), _reqPos(0), _reqProcessed(0),
			_canRead(false), _canWrite(false), _lastRecvTime(getTime()) {
		int flags = fcntl(_fd, F_GETFL, 0);
		fcntl(_fd, F_SETFL, flags | O_NONBLOCK);
		int size = 1024 * 1024 * 128;
		int err = setsockopt(_fd, SOL_SOCKET, SO_SNDBUF,  &size, sizeof(int));
		if (err != 0) {
			printf("Unable to set send buffer size, continuing with default size\n");
		}
		size = 1024 * 1024 * 10;
		err = setsockopt(_fd, SOL_SOCKET, SO_RCVBUF,  &size, sizeof(int));
		if (err != 0) {
			printf("Unable to set receive buffer size, continuing with default size\n");
		}
		flags = 1;
        setsockopt(_fd,            /* socket affected */
			IPPROTO_TCP,     /* set option at TCP level */
			TCP_NODELAY,     /* name of option */
			(char *)&flags,  /* the cast is historical cruft */
			sizeof(int));    /* length of option value */
	}

	inline ssize_t read(uint8_t *buffer, size_t length) {
		ssize_t ret = recv(_fd, buffer, length, MSG_DONTWAIT);
		if (ret <= 0) {
			if (0 == ret)
				ret = -1;
			else {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					_canRead = false;
					ret = 0;
				}
			}
		}
		return ret;
	}

	inline ssize_t read(uint8_t *buf1, size_t len1,
					uint8_t *buf2, size_t len2) {
		iovec vecs[2];
		vecs[0].iov_base = (void *)buf1;
		vecs[0].iov_len = len1;
		vecs[1].iov_base = (void *)buf2;
		vecs[1].iov_len = len2;
		ssize_t ret = readv(_fd, vecs, 2);
		if (ret <= 0) {
			if (0 == ret)
				ret = -1;
			else {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					_canRead = false;
					ret = 0;
				}
			}
		}
		return ret;
	}

	inline ssize_t write(const uint8_t *buffer, size_t length) {
		ssize_t ret = ::send(_fd, buffer, length, MSG_NOSIGNAL | MSG_DONTWAIT);
		if (ret < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				ret = 0;
			}
		}
		if (ret == 0) {
			_canWrite = false;
		}
		return ret;
	}

	inline ssize_t write(
			const uint8_t *buf1, size_t len1,
			const uint8_t *buf2, size_t len2) {
		iovec vecs[2];
		vecs[0].iov_base = (void *)buf1;
		vecs[0].iov_len = len1;
		vecs[1].iov_base = (void *)buf2;
		vecs[1].iov_len = len2;
		ssize_t ret = ::writev(_fd, vecs, 2);
		if (ret < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				ret = 0;
			}
		}
		if (ret == 0) {
			_canWrite = false;
		}
		return ret;
	}

	inline void close() {
		if (_fd > 0) {
			::close(_fd);
			_fd = 0;
		}
	}

	inline void addResponse(int64_t reqID, int64_t respID, int32_t seat) {
		size_t p = _respPos;
		size_t i = p % SESSION_QUEUE_SIZE;
		_resps[i]._reqID = reqID;
		_resps[i]._respID = (int32_t)respID;
		_resps[i]._seat = seat;
		asm volatile("" ::: "memory");
		_respPos = p + 1;
	}

	inline ssize_t writeResponses() {
		const size_t CAPACITY = sizeof(NetResp) * SESSION_QUEUE_SIZE;
		size_t total = _respPos * sizeof(NetResp);
		size_t start = _respSent % CAPACITY;
		ssize_t ret = 0;
		if (_respPos == _reqPos || ((total - _respSent) > 1400)) {		// MTU = 1500?
			size_t end = total % CAPACITY;
			if (start > end) {	// rewind
				if (end > 0) {
					ret = write(((const uint8_t *)_resps) + start, CAPACITY - start,
							(const uint8_t *)_resps, end);
				} else {
					ret = write(((const uint8_t *)_resps) + start, CAPACITY - start);
				}
			} else if (start < end) {
				ret = write(((const uint8_t *)_resps) + start, end - start);
			}
			if (ret > 0) {
				_respSent += ret;
			}
		}
		return ret;
	}

	size_t maxRead() const;

	inline ssize_t readReq() {
		const size_t CAPACITY = sizeof(NetReq) * SESSION_QUEUE_SIZE;
		ssize_t ret = 0;
		size_t n = maxRead();
		if (n > 0) {
			size_t targetEndBytes = (_reqPos + n) * sizeof(NetReq);
			assert(targetEndBytes > _reqRead);
			size_t start = _reqRead % CAPACITY;
			size_t end = targetEndBytes % CAPACITY;
			if (start > end) {	// rewind
				if (end > 0) {
					ret = read(((uint8_t *)_reqs) + start, CAPACITY - start,
							(uint8_t *)_reqs, end);
				} else {
					ret = read(((uint8_t *)_reqs) + start, CAPACITY - start);
				}
			} else if (start < end) {
				ret = read(((uint8_t *)_reqs) + start, end - start);
			}
			if (ret > 0) {
				_reqRead += ret;
				_reqPos = _reqRead / sizeof(NetReq);
			}
		}
		return ret;
	}

	inline bool release() const {
		return _release;
	}

	inline void setRelease() {
		_release = true;
	}
public:
	int 			_fd;
	bool			_release;
	NetResp			_resps[SESSION_QUEUE_SIZE];
	size_t			_reqRead;		// Bytes
	size_t			_respSent;		// Bytes

	size_t volatile	_respPos;
	size_t			_reqPos;
	size_t			_reqProcessed;
	NetReq			_reqs[SESSION_QUEUE_SIZE];

	bool			_canRead;
	bool			_canWrite;

	double			_lastRecvTime;
};

struct ClientReq {
	ClientSession *		_session;
	NetReq				_req;
};
#endif /* PC12306_H_ */
