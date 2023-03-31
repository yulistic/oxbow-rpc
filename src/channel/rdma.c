// #define _GNU_SOURCE
// #define _BSD_SOURCE
#include <endian.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <inttypes.h>
// #include <netinet/in.h>
// #include <infiniband/arch.h>
#include <rdma/rdma_cma.h>
#include "rdma.h"
#include "log.h"

#define RDMA_SQ_DEPTH 16

// TODO: to be deleted.
#define RPING_MSG_FMT "rdma-ping-%d: "

struct rdma_event_channel *create_first_event_channel(void)
{
	struct rdma_event_channel *channel;

	channel = rdma_create_event_channel();
	if (!channel) {
		if (errno == ENODEV)
			fprintf(stderr, "No RDMA devices were detected\n");
		else
			printf("Failed to create RDMA CM event channel\n");
	}
	return channel;
}

/*
 * rping "ping/pong" loop:
 * 	client sends source rkey/addr/len
 *	server receives source rkey/add/len
 *	server rdma reads "ping" data from source
 * 	server sends "go ahead" on rdma read completion
 *	client sends sink rkey/addr/len
 * 	server receives sink rkey/addr/len
 * 	server rdma writes "pong" data to sink
 * 	server sends "go ahead" on rdma write completion
 * 	<repeat loop>
 */

static int cma_event_handler(struct rdma_cm_id *cma_id,
			     struct rdma_cm_event *event)
{
	int ret = 0;
	struct rdma_ch_cb *cb = cma_id->context;

	log_debug("cma_event type %s cma_id %p (%s)\n",
		  rdma_event_str(event->event), cma_id,
		  (cma_id == cb->cm_id) ? "parent" : "child");

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		cb->state = ADDR_RESOLVED;
		ret = rdma_resolve_route(cma_id, 2000);
		if (ret) {
			cb->state = ERROR;
			fprintf(stderr, "rdma_resolve_route");
			sem_post(&cb->sem);
		}
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		cb->state = ROUTE_RESOLVED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
		cb->state = CONNECT_REQUEST;
		cb->child_cm_id = cma_id;
		log_debug("child cma %p\n", cb->child_cm_id);
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		log_debug("ESTABLISHED\n");

		/*
		 * Server will wake up when first RECV completes.
		 */
		if (!cb->server) {
			cb->state = CONNECTED;
		}
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
	case RDMA_CM_EVENT_REJECTED:
		fprintf(stderr, "cma event %s, error %d\n",
			rdma_event_str(event->event), event->status);
		sem_post(&cb->sem);
		ret = -1;
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		fprintf(stderr, "%s DISCONNECT EVENT...\n",
			cb->server ? "server" : "client");
		cb->state = DISCONNECTED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		fprintf(stderr, "cma detected device removal!!!!\n");
		ret = -1;
		break;

	default:
		fprintf(stderr, "unhandled event: %s, ignoring\n",
			rdma_event_str(event->event));
		break;
	}

	return ret;
}

void *cm_thread(void *arg)
{
	struct rdma_ch_cb *cb = (struct rdma_ch_cb *)arg;
	struct rdma_cm_event *event;
	int ret;

	while (1) {
		ret = rdma_get_cm_event(cb->cm_channel, &event);
		if (ret) {
			fprintf(stderr, "rdma_get_cm_event");
			exit(ret);
		}
		ret = cma_event_handler(event->id, event);
		rdma_ack_cm_event(event);
		if (ret)
			exit(ret);
	}
}

static int bind_server(struct rdma_ch_cb *cb)
{
	int ret;

	if (cb->sin.ss_family == AF_INET)
		((struct sockaddr_in *)&cb->sin)->sin_port = cb->port;
	else
		((struct sockaddr_in6 *)&cb->sin)->sin6_port = cb->port;

	ret = rdma_bind_addr(cb->cm_id, (struct sockaddr *)&cb->sin);
	if (ret) {
		fprintf(stderr, "rdma_bind_addr failed.");
		return ret;
	}
	log_info("rdma_bind_addr successful");

	log_info("rdma_listen");
	ret = rdma_listen(cb->cm_id, 3); // FIXME: Proper backlog value?
	if (ret) {
		fprintf(stderr, "rdma_listen failed.");
		return ret;
	}

	return 0;
}

static struct rdma_ch_cb *clone_cb(struct rdma_ch_cb *listening_cb)
{
	struct rdma_ch_cb *cb = malloc(sizeof *cb);
	if (!cb)
		return NULL;
	memset(cb, 0, sizeof *cb);
	*cb = *listening_cb;
	cb->child_cm_id->context = cb;
	return cb;
}

static int create_qp(struct rdma_ch_cb *cb)
{
	struct ibv_qp_init_attr init_attr;
	int ret;

	memset(&init_attr, 0, sizeof(init_attr));

	// TODO: Check configuration.
	init_attr.cap.max_send_wr = RDMA_SQ_DEPTH;
	init_attr.cap.max_recv_wr = 2;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.send_cq = cb->cq;
	init_attr.recv_cq = cb->cq;

	if (cb->server) {
		ret = rdma_create_qp(cb->child_cm_id, cb->pd, &init_attr);
		if (!ret)
			cb->qp = cb->child_cm_id->qp;
	} else {
		ret = rdma_create_qp(cb->cm_id, cb->pd, &init_attr);
		if (!ret)
			cb->qp = cb->cm_id->qp;
	}

	return ret;
}

static int setup_qp(struct rdma_ch_cb *cb, struct rdma_cm_id *cm_id)
{
	int ret;

	cb->pd = ibv_alloc_pd(cm_id->verbs);
	if (!cb->pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return errno;
	}
	log_debug("created pd %p", cb->pd);

	cb->channel = ibv_create_comp_channel(cm_id->verbs);
	if (!cb->channel) {
		fprintf(stderr, "ibv_create_comp_channel failed\n");
		ret = errno;
		goto err1;
	}
	log_debug("created channel %p", cb->channel);

	cb->cq = ibv_create_cq(cm_id->verbs, RDMA_SQ_DEPTH * 2, cb, cb->channel,
			       0);
	if (!cb->cq) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err2;
	}
	log_debug("created cq %p\n", cb->cq);

	ret = ibv_req_notify_cq(cb->cq, 0);
	if (ret) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err3;
	}

	ret = create_qp(cb);
	if (ret) {
		fprintf(stderr, "rdma_create_qp");
		goto err3;
	}
	log_debug("created qp %p", cb->qp);
	return 0;

err3:
	ibv_destroy_cq(cb->cq);
err2:
	ibv_destroy_comp_channel(cb->channel);
err1:
	ibv_dealloc_pd(cb->pd);
	return ret;
}

// TODO: Check WR.
static void setup_wr(struct rdma_ch_cb *cb)
{
	cb->recv_sgl.addr = (uint64_t)(unsigned long)&cb->recv_buf;
	cb->recv_sgl.length = sizeof cb->recv_buf;
	cb->recv_sgl.lkey = cb->recv_mr->lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;

	cb->send_sgl.addr = (uint64_t)(unsigned long)&cb->send_buf;
	cb->send_sgl.length = sizeof cb->send_buf;
	cb->send_sgl.lkey = cb->send_mr->lkey;

	cb->sq_wr.opcode = IBV_WR_SEND;
	cb->sq_wr.send_flags = IBV_SEND_SIGNALED;
	cb->sq_wr.sg_list = &cb->send_sgl;
	cb->sq_wr.num_sge = 1;

	cb->rdma_sgl.addr = (uint64_t)(unsigned long)cb->rdma_buf;
	cb->rdma_sgl.lkey = cb->rdma_mr->lkey;
	cb->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
	cb->rdma_sq_wr.sg_list = &cb->rdma_sgl;
	cb->rdma_sq_wr.num_sge = 1;
}

// TODO: Check buffers. Pass MR region info.
static int setup_buffers(struct rdma_ch_cb *cb)
{
	int ret;

	log_debug("setup_buffers called on cb %p", cb);

	cb->recv_mr = ibv_reg_mr(cb->pd, &cb->recv_buf, sizeof cb->recv_buf,
				 IBV_ACCESS_LOCAL_WRITE);
	if (!cb->recv_mr) {
		fprintf(stderr, "recv_buf reg_mr failed\n");
		return errno;
	}

	cb->send_mr = ibv_reg_mr(cb->pd, &cb->send_buf, sizeof cb->send_buf, 0);
	if (!cb->send_mr) {
		fprintf(stderr, "send_buf reg_mr failed\n");
		ret = errno;
		goto err1;
	}

	cb->rdma_buf = malloc(cb->size);
	if (!cb->rdma_buf) {
		fprintf(stderr, "rdma_buf malloc failed\n");
		ret = -ENOMEM;
		goto err2;
	}

	// TODO: RDMA is used by DATA FETCHER.
	cb->rdma_mr =
		ibv_reg_mr(cb->pd, cb->rdma_buf, cb->size,
			   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
				   IBV_ACCESS_REMOTE_WRITE);
	if (!cb->rdma_mr) {
		fprintf(stderr, "rdma_buf reg_mr failed\n");
		ret = errno;
		goto err3;
	}

	if (!cb->server) {
		cb->start_buf = malloc(cb->size);
		if (!cb->start_buf) {
			fprintf(stderr, "start_buf malloc failed\n");
			ret = -ENOMEM;
			goto err4;
		}

		cb->start_mr = ibv_reg_mr(cb->pd, cb->start_buf, cb->size,
					  IBV_ACCESS_LOCAL_WRITE |
						  IBV_ACCESS_REMOTE_READ |
						  IBV_ACCESS_REMOTE_WRITE);
		if (!cb->start_mr) {
			fprintf(stderr, "start_buf reg_mr failed\n");
			ret = errno;
			goto err5;
		}
	}

	setup_wr(cb);
	log_debug("allocated & registered buffers...");
	return 0;

err5:
	free(cb->start_buf);
err4:
	ibv_dereg_mr(cb->rdma_mr);
err3:
	free(cb->rdma_buf);
err2:
	ibv_dereg_mr(cb->send_mr);
err1:
	ibv_dereg_mr(cb->recv_mr);
	return ret;
}

static int server_recv(struct rdma_ch_cb *cb, struct ibv_wc *wc)
{
	if (wc->byte_len != sizeof(cb->recv_buf)) {
		fprintf(stderr, "Received bogus data, size %d\n", wc->byte_len);
		return -1;
	}

	cb->remote_rkey = be32toh(cb->recv_buf.rkey);
	cb->remote_addr = be64toh(cb->recv_buf.buf);
	cb->remote_len = be32toh(cb->recv_buf.size);
	log_debug("Received rkey %x addr %" PRIx64 " len %d from peer",
		  cb->remote_rkey, cb->remote_addr, cb->remote_len);

	if (cb->state <= CONNECTED || cb->state == RDMA_WRITE_COMPLETE)
		cb->state = RDMA_READ_ADV;
	else
		cb->state = RDMA_WRITE_ADV;

	return 0;
}

static int client_recv(struct rdma_ch_cb *cb, struct ibv_wc *wc)
{
	if (wc->byte_len != sizeof(cb->recv_buf)) {
		fprintf(stderr, "Received bogus data, size %d\n", wc->byte_len);
		return -1;
	}

	if (cb->state == RDMA_READ_ADV)
		cb->state = RDMA_WRITE_ADV;
	else
		cb->state = RDMA_WRITE_COMPLETE;

	return 0;
}

static int cq_event_handler(struct rdma_ch_cb *cb)
{
	struct ibv_wc wc;
	struct ibv_recv_wr *bad_wr;
	int ret;
	int flushed = 0;

	while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
		ret = 0;

		if (wc.status) {
			if (wc.status == IBV_WC_WR_FLUSH_ERR) {
				flushed = 1;
				continue;
			}
			fprintf(stderr, "cq completion failed status %d\n",
				wc.status);
			ret = -1;
			goto error;
		}

		switch (wc.opcode) {
		case IBV_WC_SEND:
			log_debug("send completion");
			break;

		case IBV_WC_RDMA_WRITE:
			log_debug("rdma write completion");
			cb->state = RDMA_WRITE_COMPLETE;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RDMA_READ:
			log_debug("rdma read completion");
			cb->state = RDMA_READ_COMPLETE;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RECV:
			log_debug("recv completion\n");
			ret = cb->server ? server_recv(cb, &wc) :
					   client_recv(cb, &wc);
			if (ret) {
				fprintf(stderr, "recv wc error: %d\n", ret);
				goto error;
			}

			ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
			if (ret) {
				fprintf(stderr, "post recv error: %d\n", ret);
				goto error;
			}
			sem_post(&cb->sem);
			break;

		default:
			log_debug("unknown!!!!! completion\n");
			ret = -1;
			goto error;
		}
	}
	if (ret) {
		fprintf(stderr, "poll error %d\n", ret);
		goto error;
	}
	return flushed;

error:
	cb->state = ERROR;
	sem_post(&cb->sem);
	return ret;
}

static void *cq_thread(void *arg)
{
	struct rdma_ch_cb *cb = arg;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
	int ret;

	log_debug("cq_thread started.");

	while (1) {
		pthread_testcancel();

		ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
		if (ret) {
			fprintf(stderr, "Failed to get cq event!\n");
			pthread_exit(NULL);
		}
		if (ev_cq != cb->cq) {
			fprintf(stderr, "Unknown CQ!\n");
			pthread_exit(NULL);
		}
		ret = ibv_req_notify_cq(cb->cq, 0);
		if (ret) {
			fprintf(stderr, "Failed to set notify!\n");
			pthread_exit(NULL);
		}
		ret = cq_event_handler(cb);
		ibv_ack_cq_events(cb->cq, 1);
		if (ret)
			pthread_exit(NULL);
	}
}

static int do_accept(struct rdma_ch_cb *cb)
{
	int ret;

	log_debug("accepting client connection request");

	ret = rdma_accept(cb->child_cm_id, NULL);
	if (ret) {
		fprintf(stderr, "rdma_accept\n");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state == ERROR) {
		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
		return -1;
	}
	return 0;
}

static void free_buffers(struct rdma_ch_cb *cb)
{
	log_debug("free_buffers called on cb %p", cb);
	ibv_dereg_mr(cb->recv_mr);
	ibv_dereg_mr(cb->send_mr);
	ibv_dereg_mr(cb->rdma_mr);
	free(cb->rdma_buf);
	if (!cb->server) {
		ibv_dereg_mr(cb->start_mr);
		free(cb->start_buf);
	}
}

static void free_qp(struct rdma_ch_cb *cb)
{
	ibv_destroy_qp(cb->qp);
	ibv_destroy_cq(cb->cq);
	ibv_destroy_comp_channel(cb->channel);
	ibv_dealloc_pd(cb->pd);
}

static void free_cb(struct rdma_ch_cb *cb)
{
	free(cb);
}

// TODO: Implement server logic.
static int handle_request(struct rdma_ch_cb *cb)
{
	struct ibv_send_wr *bad_wr;
	int ret;

	while (1) {
		/* Wait for client's Start STAG/TO/Len */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_READ_ADV) {
			fprintf(stderr, "wait for RDMA_READ_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		log_debug("server received sink adv");

		/* Issue RDMA Read. */
		cb->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
		cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
		cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
		cb->rdma_sq_wr.sg_list->length = cb->remote_len;

		ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		log_debug("server posted rdma read req");

		/* Wait for read completion */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_READ_COMPLETE) {
			fprintf(stderr,
				"wait for RDMA_READ_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		log_debug("server received read complete");

		/* Display data in recv buf */
		if (cb->verbose)
			printf("server ping data: %s\n", cb->rdma_buf);

		/* Tell client to continue */
		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		log_debug("server posted go ahead");

		/* Wait for client's RDMA STAG/TO/Len */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_ADV) {
			fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		log_debug("server received sink adv");

		/* RDMA Write echo data */
		cb->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
		cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
		cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
		cb->rdma_sq_wr.sg_list->length = strlen(cb->rdma_buf) + 1;
		log_debug("rdma write from lkey %x laddr %" PRIx64 " len %d\n",
			  cb->rdma_sq_wr.sg_list->lkey,
			  cb->rdma_sq_wr.sg_list->addr,
			  cb->rdma_sq_wr.sg_list->length);

		ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* Wait for completion */
		ret = sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_COMPLETE) {
			fprintf(stderr,
				"wait for RDMA_WRITE_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		log_debug("server rdma write complete");

		/* Tell client to begin again */
		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		log_debug("server posted go ahead");
	}

	return (cb->state == DISCONNECTED) ? 0 : ret;
}

static void *server_thread(void *arg)
{
	struct rdma_ch_cb *cb = arg;
	struct ibv_recv_wr *bad_wr;
	int ret;

	ret = setup_qp(cb, cb->child_cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		goto err0;
	}

	ret = setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		fprintf(stderr, "pthread_create\n");
		goto err2;
	}

	ret = do_accept(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	// TODO: Main server logic.
	handle_request(cb);

	rdma_disconnect(cb->child_cm_id);
	pthread_join(cb->cqthread, NULL);
	free_buffers(cb);
	free_qp(cb);
	rdma_destroy_id(cb->child_cm_id);
	free_cb(cb);
	return NULL;
err3:
	pthread_cancel(cb->cqthread);
	pthread_join(cb->cqthread, NULL);
err2:
	free_buffers(cb);
err1:
	free_qp(cb);
err0:
	free_cb(cb);
	return NULL;
}

int run_server(struct rdma_ch_cb *listening_cb)
{
	int ret;
	struct rdma_ch_cb *cb;
	pthread_attr_t attr;

	ret = bind_server(listening_cb);
	if (ret) {
		goto err;
	}

	/*
	 * Set persistent server threads to DEATCHED state so
	 * they release all their resources when they exit.
	 */
	ret = pthread_attr_init(&attr);
	if (ret) {
		fprintf(stderr, "pthread_attr_init failed.\n");
		goto err;
	}
	ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if (ret) {
		fprintf(stderr, "pthread_attr_setdetachstate failed.\n");
		goto err;
	}

	while (1) {
		sem_wait(&listening_cb->sem);
		if (listening_cb->state != CONNECT_REQUEST) {
			fprintf(stderr,
				"Wait for CONNECT_REQUEST state but state is %d\n",
				listening_cb->state);
			ret = -1;
			goto err;
		}

		cb = clone_cb(listening_cb);
		if (!cb) {
			ret = -1;
			goto err;
		}

		ret = pthread_create(&cb->server_thread, &attr, server_thread,
				     cb);
		if (ret) {
			fprintf(stderr, "pthread_create failed.");
			goto err;
		}
	}
	return 0;

err:
	rdma_destroy_id(listening_cb->cm_id);
	rdma_destroy_event_channel(listening_cb->cm_channel);
	return ret;
}

static int bind_client(struct rdma_ch_cb *cb)
{
	int ret;

	if (cb->sin.ss_family == AF_INET)
		((struct sockaddr_in *)&cb->sin)->sin_port = cb->port;
	else
		((struct sockaddr_in6 *)&cb->sin)->sin6_port = cb->port;

	ret = rdma_resolve_addr(cb->cm_id, NULL, (struct sockaddr *)&cb->sin,
				2000);
	if (ret) {
		fprintf(stderr, "rdma_resolve_addr");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != ROUTE_RESOLVED) {
		fprintf(stderr, "waiting for addr/route resolution state %d\n",
			cb->state);
		return -1;
	}

	log_debug("rdma_resolve_addr - rdma_resolve_route successful");
	return 0;
}

static int connect_client(struct rdma_ch_cb *cb)
{
	struct rdma_conn_param conn_param;
	int ret;

	memset(&conn_param, 0, sizeof conn_param);
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;

	ret = rdma_connect(cb->cm_id, &conn_param);
	if (ret) {
		fprintf(stderr, "rdma_connect\n");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != CONNECTED) {
		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
		return -1;
	}

	log_debug("rmda_connect successful");
	return 0;
}

static void format_send(struct rdma_ch_cb *cb, char *buf, struct ibv_mr *mr)
{
	struct comm_rdma_mr_info *info = &cb->send_buf;

	info->buf = htobe64((uint64_t)(unsigned long)buf);
	info->rkey = htobe32(mr->rkey);
	info->size = htobe32(cb->size);

	log_debug("RDMA addr %" PRIx64 " rkey %x len %d", be64toh(info->buf),
		  be32toh(info->rkey), be32toh(info->size));
}

static int test_client(struct rdma_ch_cb *cb)
{
	int ping, start, cc, i, ret = 0;
	struct ibv_send_wr *bad_wr;
	unsigned char c;

	start = 65;
	for (ping = 0; !cb->count || ping < cb->count; ping++) {
		cb->state = RDMA_READ_ADV;

		/* Put some ascii text in the buffer. */
		cc = snprintf(cb->start_buf, cb->size, RPING_MSG_FMT, ping);
		for (i = cc, c = start; i < cb->size; i++) {
			cb->start_buf[i] = c;
			c++;
			if (c > 122)
				c = 65;
		}
		start++;
		if (start > 122)
			start = 65;
		cb->start_buf[cb->size - 1] = 0;

		format_send(cb, cb->start_buf, cb->start_mr);
		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* Wait for server to ACK */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_ADV) {
			fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		format_send(cb, cb->rdma_buf, cb->rdma_mr);
		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* Wait for the server to say the RDMA Write is complete. */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_COMPLETE) {
			fprintf(stderr,
				"wait for RDMA_WRITE_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		if (cb->validate)
			if (memcmp(cb->start_buf, cb->rdma_buf, cb->size)) {
				fprintf(stderr, "data mismatch!\n");
				ret = -1;
				break;
			}

		if (cb->verbose)
			printf("ping data: %s\n", cb->rdma_buf);
	}

	return (cb->state == DISCONNECTED) ? 0 : ret;
}

static int run_client(struct rdma_ch_cb *cb)
{
	struct ibv_recv_wr *bad_wr;
	int ret;

	ret = bind_client(cb);
	if (ret)
		return ret;

	ret = setup_qp(cb, cb->cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		return ret;
	}

	ret = setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "setup_buffers failed: %d\n", ret);
		goto err1;
	}

	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		perror("pthread_create");
		goto err2;
	}

	ret = connect_client(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	ret = test_client(cb);
	if (ret) {
		fprintf(stderr, "rping client failed: %d\n", ret);
		goto err4;
	}

	ret = 0;
err4:
	rdma_disconnect(cb->cm_id);
err3:
	pthread_join(cb->cqthread, NULL);
err2:
	free_buffers(cb);
err1:
	free_qp(cb);

	return ret;
}

static int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret;

	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed (%s) - invalid hostname or IP address\n",
		       gai_strerror(ret));
		return ret;
	}

	if (res->ai_family == PF_INET)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	else if (res->ai_family == PF_INET6)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
	else
		ret = -1;

	freeaddrinfo(res);
	return ret;
}

// TODO: Pass mr info.
/**
 * @brief 
 * 
 * @param port 
 * @param server 
 * @param ip_addr Target address (required by client).
 * @return int 
 */
int init_rdma_ch(int port, int server, char *ip_addr, int msgbuf_cnt)
{
	struct rdma_ch_cb *cb;
	int ret;

	cb = calloc(1, sizeof(struct rdma_ch_cb));

	if (!cb)
		return -ENOMEM;

	cb->server = server;
	cb->state = IDLE;
	cb->size = sizeof(struct comm_rdma_mr_info); // FIXME: Where is it used?
	cb->sin.ss_family = AF_INET;
	cb->port = htobe16(port);
	sem_init(&cb->sem, 0, 0); // FIXME: Where is it used?

	cb->count = 1; // TODO: Only used for test. To be deleted.
	cb->verbose++; // TODO: Only used for test. To be deleted.

	if (!cb->server) {
		ret = get_addr(ip_addr, (struct sockaddr *)&cb->sin);
		if (ret)
			goto out;
	}

	cb->cm_channel = create_first_event_channel();
	if (!cb->cm_channel) {
		ret = errno;
		goto out;
	}

	ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);

	if (ret) {
		printf("rdma_create_id failed.\n");
		goto out1;
	}

	ret = pthread_create(&cb->cmthread, NULL, cm_thread, cb);
	if (ret) {
		printf("Creating cm_thread failed.\n");
		goto out2;
	}

	if (cb->server) {
		run_server(cb);
	} else {
		run_client(cb);
	}

	printf("Destroy rdma connections.\n");
	return 0;

out2:
	rdma_destroy_id(cb->cm_id);
out1:
	rdma_destroy_event_channel(cb->cm_channel);
out:
	free(cb);
	return ret;
}