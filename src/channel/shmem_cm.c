#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include "global.h"
#include "shmem.h"
#include "shmem_cm.h"

// Overwrite global print config.
// #define ENABLE_PRINT 1
#include "log.h"

#define MAX_EVENTS 32

static void print_shmem_cm_request(struct shmem_cm_request *req);
static void print_shmem_cm_response(struct shmem_cm_response *res);

int connect_to_shmem_server(void *arg, key_t *shm_key, uint64_t *shm_size,
			    key_t *cq_shm_key)
{
	int client_fd, ret;
	struct sockaddr_un server_addr;
	struct shmem_cm_response *buf_in;
	struct shmem_cm_request *buf_out;
	struct shmem_ch_cb *cb;

	cb = (struct shmem_ch_cb *)arg;

	client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (client_fd == -1) {
		log_error("shmem cm client socket failed.");
		ret = -1;
		goto err1;
	}

	memset(&server_addr, 0, sizeof(struct sockaddr_un));
	server_addr.sun_family = AF_UNIX;

	strncpy(server_addr.sun_path, cb->cm_socket_name,
		sizeof(server_addr.sun_path) - 1);

	ret = connect(client_fd, (struct sockaddr *)&server_addr,
		      sizeof(struct sockaddr_un));
	if (ret == -1) {
		log_error("shmem cm client connect failed.");
		goto err2;
	}

	log_debug("Connecting to server on %s", cb->cm_socket_name);

	buf_in = calloc(1, sizeof(struct shmem_cm_response));
	if (!buf_in) {
		log_error("calloc failed");
		ret = -1;
		goto err2;
	}

	buf_out = calloc(1, sizeof(struct shmem_cm_request));
	if (!buf_out) {
		log_error("calloc failed");
		ret = -1;
		goto err3;
	}

	buf_out->op = REGISTER;

	// Send cm request.
	log_info("Sending CM message to server:");
	print_shmem_cm_request(buf_out);

	ret = write(client_fd, buf_out, sizeof(struct shmem_cm_request));
	if (ret == -1) {
		log_error("shmem cm client write failed.");
		goto err4;
	}

	// Recv cm response.

	ret = read(client_fd, buf_in, sizeof(struct shmem_cm_response));
	if (ret == -1) {
		log_error("shmem cm client read failed.");
		goto err4;
	}

	log_info("Received CM message from server:");
	print_shmem_cm_response(buf_in);

	*shm_key = buf_in->client_key;
	*shm_size = buf_in->shm_size;
	*cq_shm_key = buf_in->cq_key;

	log_info("CM response: shm_key=%lu shm_size=%lu cq_shm_key=%lu",
		 (uint64_t)*shm_key, *shm_size, (uint64_t)*cq_shm_key);

	// We maintain the connection for server to detect client's disconnection.
	// close(client_fd);

	free(buf_out);
	free(buf_in);

	return 0;

err4:
	free(buf_out);
err3:
	free(buf_in);
err2:
	close(client_fd);
err1:
	return ret;
}

void disconnect_from_server(int client_fd)
{
	close(client_fd);
}

/**
 * @brief Handle client's cm request.
 * 
 * @param server_cb  Server's cb.
 * @param client_fd Client's CM socket fd.
 * @param req Client's request.
 * @param res Server's response. It is filled in this function.
 */
void handle_cm_msg(struct shmem_ch_cb *server_cb, int client_fd,
		   struct shmem_cm_request *req, struct shmem_cm_response *res)
{
	switch (req->op) {
	case REGISTER:
		// Alloc a new control block.
		// Alloc shmem seg and register a client.
		register_client(server_cb, client_fd, &res->client_key,
				&res->cq_key);
		res->op = REGISTERED;
		break;

	case DEREGISTER:
		// TODO:: Deregister client.
		deregister_client_with_key(server_cb, req->client_key);
		res->op = DEREGISTERED;
		break;

	default:
		break;
	}
}

static int set_nonblocking(int sockfd)
{
	if (fcntl(sockfd, F_SETFD, fcntl(sockfd, F_GETFD, 0) | O_NONBLOCK) ==
	    -1) {
		log_error("Failed to set server socket non-blocking.");
		return -1;
	}
	return 0;
}

static void epoll_ctl_add(int epfd, int fd, uint32_t events)
{
	struct epoll_event ev;
	ev.events = events;
	ev.data.fd = fd;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
		perror("epoll_ctl()\n");
		exit(1);
	}
}

int accept_client_connection(int server_fd, struct sockaddr_un *client_addr,
			     int epfd)
{
	unsigned int len;
	int client_fd;

	len = sizeof(struct sockaddr_un);
	client_fd = accept(server_fd, (struct sockaddr *)client_addr, &len);
	if (client_fd == -1) {
		log_error("shmem cm accept failed.");
		return -1;
	}

	log_debug("Accept client socket fd=%d", client_fd);

	set_nonblocking(client_fd);
	epoll_ctl_add(epfd, client_fd,
		      EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLHUP);
	return 0;
}

int handle_epollin(struct shmem_ch_cb *cb, int client_fd,
		   struct shmem_cm_request *buf_in,
		   struct shmem_cm_response *buf_out)
{
	int ret;
	while (1) {
		// Get request.
		ret = read(client_fd, buf_in, sizeof(struct shmem_cm_request));
		if (ret < 0) { // errno == EAGAIN
			break;
		} else {
			log_info("Received CM message from client:");
			print_shmem_cm_request(buf_in);

			handle_cm_msg(cb, client_fd,
				      (struct shmem_cm_request *)buf_in,
				      (struct shmem_cm_response *)buf_out);

			// Send response.
			log_info("Sending CM message to client:");
			print_shmem_cm_response(buf_out);

			ret = write(client_fd, buf_out,
				    sizeof(struct shmem_cm_response));
			if (ret == -1) {
				log_error("shmem cm write failed.");
				return -1;
			}

			break;
		}
	}
	return 0;
}

void disconnect_client(int epfd, int client_fd)
{
	epoll_ctl(epfd, EPOLL_CTL_DEL, client_fd, NULL);
	close(client_fd);
}

// Server cm thread.
void *shmem_cm_thread(void *arg)
{
	int server_fd, ret;
	// int client_fd;
	// unsigned int len;
	struct sockaddr_un server_addr, client_addr;
	struct shmem_cm_request *buf_in;
	struct shmem_cm_response *buf_out;
	struct shmem_ch_cb *cb; // server's cb.

	int epfd, n_fds, i;
	struct epoll_event events[MAX_EVENTS];

	cb = (struct shmem_ch_cb *)arg;

	// sigaction(SIGPIPE, &(struct sigaction){ SIG_IGN }, NULL);

	server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (server_fd == -1) {
		log_error("shmem cm error on socket()");
		ret = -1;
		goto err1;
	}

	memset(&server_addr, 0, sizeof(struct sockaddr_un));
	server_addr.sun_family = AF_UNIX;

	strncpy(server_addr.sun_path, cb->cm_socket_name,
		sizeof(server_addr.sun_path) - 1);

	log_debug("CM socket_name=%s", cb->cm_socket_name);

	// FIXME: It unlink the previous connection.
	// Be careful to handle the duplicate socket_name issue.
	ret = unlink(cb->cm_socket_name);
	if (!ret) { // On success.
		log_info("Existing socket(%s) is deleted.", cb->cm_socket_name);
	}

	ret = bind(server_fd, (struct sockaddr *)&server_addr,
		   sizeof(struct sockaddr_un));
	if (ret == -1) {
		log_error("shmem cm error on bind()");
		ret = -1;
		goto err2;
	}

	set_nonblocking(server_fd);

	ret = listen(server_fd, 5);
	if (ret == -1) {
		log_error("shmem cm error on listen()");
		ret = -1;
		goto err2;
	}

	log_info("Server listening on %s", cb->cm_socket_name);

	buf_in = calloc(1, sizeof(struct shmem_cm_request));
	if (!buf_in) {
		log_error("calloc failed");
		ret = -1;
		goto err2;
	}

	buf_out = calloc(1, sizeof(struct shmem_cm_response));
	if (!buf_out) {
		log_error("calloc failed");
		ret = -1;
		goto err3;
	}

	/* Epoll way */
	epfd = epoll_create(1);
	epoll_ctl_add(epfd, server_fd, EPOLLIN | EPOLLOUT | EPOLLET);

	while (1) {
		n_fds = epoll_wait(epfd, events, MAX_EVENTS, -1);
		for (i = 0; i < n_fds; i++) {
			if (events[i].data.fd == server_fd) {
				ret = accept_client_connection(
					server_fd, &client_addr, epfd);
				if (ret < 0)
					goto err4;

			} else if (events[i].events & (EPOLLRDHUP | EPOLLHUP)) {
				// client disconnected.
				log_info("Connection closed.\n");
				disconnect_client(epfd, events[i].data.fd);
				deregister_client_with_sockfd(
					cb, events[i].data.fd);
				continue;

			} else if (events[i].events & EPOLLIN) {
				ret = handle_epollin(cb, events[i].data.fd,
						     buf_in, buf_out);
				if (ret < 0)
					goto err4;

			} else {
				log_error("Unexpected epoll event.");
			}
		}
	}

	/* Traditional way. */
	// len = sizeof(struct sockaddr_un);
	// while (1) {
	// 	client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
	// 			   &len);
	// 	if (client_fd == -1) {
	// 		log_error("shmem cm accept failed.");
	// 		ret = -1;
	// 		goto err4;
	// 	}

	// 	// Get request.
	// 	ret = read(client_fd, buf_in, sizeof(struct shmem_cm_request));
	// 	if (ret == -1) {
	// 		log_error("shmem cm read failed.");
	// 		ret = -1;
	// 		goto err4;
	// 	}

	// 	log_info("Received CM message from client:");
	// 	print_shmem_cm_request(buf_in);

	// 	handle_cm_msg(cb, (struct shmem_cm_request *)buf_in,
	// 		      (struct shmem_cm_response *)buf_out);

	// 	// Send response.
	// 	log_info("Sending CM message to client:");
	// 	print_shmem_cm_response(buf_out);

	// 	ret = write(client_fd, buf_out,
	// 		    sizeof(struct shmem_cm_response));
	// 	if (ret == -1) {
	// 		log_error("shmem cm write failed.");
	// 		ret = -1;
	// 		goto err4;
	// 	}

	// 	// TODO: Need to implement sending a heartbeat to clients and
	// 	// deregistering dead clients.
	// 	// close(client_fd);
	// }

	close(server_fd);
	return 0;

err4:
	free(buf_out);
err3:
	free(buf_in);
err2:
	close(server_fd);
err1:
	return NULL;
}

// TODO: read not work. we have to use epoll. EPOLLHUB, EPOLLRDHUP
// void *check_client_disconnection(void *arg)
// {
// 	struct shmem_ch_cb *cb;
// 	struct shmem_server_state *server;
// 	struct shmem_cm_request buf_in;
// 	bit_index_t cur, next;
// 	int ret;

// 	cb = (struct shmem_ch_cb *)arg;
// 	server = cb->server_state;

// 	while (1) {
// 		pthread_testcancel();

// 		// Lookup client bitmap.
// 		cur = 0;
// 		next = 0;

// 		// Read only. No locking required.
// 		while (bit_array_find_next_set_bit(server->client_bitmap, cur,
// 						   &next)) {
// 			log_debug("Check client start next=%d", next);
// 			// Check disconnection.
// 			ret = read(server->clients[next]->client_cm_fd, &buf_in,
// 				   sizeof(struct shmem_cm_request) - 1);
// 			cur = next + 1;
// 			log_debug("Check client end. errno=%s ret=%d",
// 				  strerror(errno), ret);
// 		}

// 		sleep(5); // Check every 10 secs.
// 	}

// 	return NULL;
// }

static void print_shmem_cm_request(struct shmem_cm_request *req)
{
	log_info("shmem_cm_request: op=%d client_key=%d", req->op,
		 req->client_key);
}
static void print_shmem_cm_response(struct shmem_cm_response *res)
{
	log_info(
		"shmem_cm_response: op=%d client_key=%d cq_key=%d shm_size=%lu",
		res->op, res->client_key, res->cq_key, res->shm_size);
}
