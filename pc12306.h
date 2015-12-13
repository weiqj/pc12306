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
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>

#define SEGMENTS	10
#define TRAINS		5000
#define SEATS		3000
#define PORT		12306

#define QUEUE_SIZE_IN	1024
#define QUEUE_SIZE_OUT	1024

struct Ticket {
	int32_t 	_seat;
	int16_t		_start;
	int16_t		_length;
	Ticket *	_next;
};

struct NetReq {
	int64_t		_reqID;
	int32_t		_train;		// [0, 5000)
	int16_t		_start;		// [0, 10)
	int16_t		_length;	// [1, 10]
};

struct NetResp {
	int64_t		_reqID;
	int32_t		_respID;
	int32_t		_seat;
};

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
	ClientSession(int fd) : _fd(fd), _reqPos(0), _respPos(0), _respSent(0) {
		int flags = fcntl(_fd, F_GETFL, 0);
		fcntl(_fd, F_SETFL, flags | O_NONBLOCK);
	}

	inline ssize_t read(uint8_t *buffer, size_t length) {
		ssize_t ret = recv(_fd, buffer, length, MSG_DONTWAIT);
		if (ret <= 0) {
			if (0 == ret)
				ret = -1;
			else {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
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
		return ret;
	}

	inline void close() {
		if (_fd > 0) {
			::close(_fd);
			_fd = 0;
		}
	}

	inline void sendResponse(int64_t reqID, int32_t seat) {
		size_t p = _respPos;
		size_t i = p % QUEUE_SIZE_OUT;
		_resps[i]._reqID = reqID;
		_resps[i]._respID = (int32_t)_respPos;
		_resps[i]._seat = seat;
		asm volatile("" ::: "memory");
		_respPos = p + 1;
	}

	inline ssize_t sendResponse() {
		const size_t CAPACITY = sizeof(NetResp) * QUEUE_SIZE_OUT;
		size_t total = _respPos * sizeof(NetResp);
		size_t start = _respSent % CAPACITY;
		size_t end = total % CAPACITY;
		ssize_t ret = 0;
		if (start > end) {	// rewind
			if (end > 0) {
				ret = write((const uint8_t *)_resps + start, CAPACITY - start,
						(const uint8_t *)_resps, end);
			} else {
				ret = write((const uint8_t *)_resps + start, CAPACITY - start);
			}
		} else if (start < end) {
			ret = write((const uint8_t *)_resps + start, end - start);
		}
		if (ret > 0) {
			_respSent += ret;
		}
		return ret;
	}

	inline size_t canRead() const {
		size_t sendPos = _respSent / sizeof(NetResp);
		size_t pending = _reqPos - sendPos;
		size_t available = QUEUE_SIZE_OUT - pending;
		if (available <= 4) {
			return 0;
		} else {
			return available - 4;
		}
	}
private:
	int 			_fd;
	NetResp			_resps[QUEUE_SIZE_OUT];
	size_t			_reqPos;
	size_t volatile	_respPos;
	size_t			_respSent;		// Bytes
};

struct ClientReq {
	ClientSession *		_session;
	NetReq				_req;
};
#endif /* PC12306_H_ */
