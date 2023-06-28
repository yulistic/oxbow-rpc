// #define _GNU_SOURCE
// #define _BSD_SOURCE
#include <endian.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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
#include "rpc.h"

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
			fprintf(stderr, "rdma_resolve_route\n");
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
	struct rdma_ch_cb *cb;
	int i;

	cb = calloc(1, sizeof *cb);
	if (!cb)
		return NULL;

	*cb = *listening_cb; // shallow copy.
	cb->child_cm_id->context = cb;

	// Alloc new buf_ctxs.
	cb->buf_ctxs = calloc(cb->msgbuf_cnt, sizeof(struct msgbuf_ctx));
	if (!cb->buf_ctxs) {
		free(cb);
		return NULL;
	}

	// FIXME: Maybe we don't need it. Buffers will be allocated later.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		cb->buf_ctxs[i] = listening_cb->buf_ctxs[i]; //shallow copy.
	}

	log_debug("Cloning CB:");
	log_debug("parent_cb=%lx", listening_cb);
	log_debug("parent cb->buf_ctxs=%lx", listening_cb->buf_ctxs);
	log_debug("cloned_cb=%lx", cb);
	log_debug("cloned cb->buf_ctxs=%lx", cb->buf_ctxs);

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

static void setup_wr(struct rdma_ch_cb *cb)
{
	struct msgbuf_ctx *mb_ctx;
	int i;

	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];

		mb_ctx->recv_sgl.addr =
			(uint64_t)(unsigned long)mb_ctx->recv_buf;
		mb_ctx->recv_sgl.length = cb->msgbuf_size;
		mb_ctx->recv_sgl.lkey = mb_ctx->recv_mr->lkey;

		mb_ctx->rq_wr.wr_id = i; // msgbuf_id is stored to wr_id.
		mb_ctx->rq_wr.sg_list = &mb_ctx->recv_sgl;
		mb_ctx->rq_wr.num_sge = 1;

		mb_ctx->send_sgl.addr =
			(uint64_t)(unsigned long)mb_ctx->send_buf;
		mb_ctx->send_sgl.length = cb->msgbuf_size;
		mb_ctx->send_sgl.lkey = mb_ctx->send_mr->lkey;

		mb_ctx->sq_wr.wr_id = i;
		mb_ctx->sq_wr.opcode = IBV_WR_SEND;
		mb_ctx->sq_wr.send_flags = IBV_SEND_SIGNALED;
		mb_ctx->sq_wr.sg_list = &mb_ctx->send_sgl;
		mb_ctx->sq_wr.num_sge = 1;
	}
	// cb->rdma_sgl.addr = (uint64_t)(unsigned long)cb->rdma_buf;
	// cb->rdma_sgl.lkey = cb->rdma_mr->lkey;
	// cb->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
	// cb->rdma_sq_wr.sg_list = &cb->rdma_sgl;
	// cb->rdma_sq_wr.num_sge = 1;
}

static inline int msgheader_size(void)
{
	struct rdma_msg msg;
	return (int)((uint64_t)&msg.data - (uint64_t)&msg);
}

static inline int msgdata_size(int msgbuf_size)
{
	return msgbuf_size - msgheader_size();
}

static inline int msgbuf_size(int msgdata_size)
{
	return msgdata_size + msgheader_size();
}

static inline int valid_msg_size(struct rdma_ch_cb *cb, char *data)
{
	return (cb->msgheader_size + sizeof *data) <= (uint64_t)cb->msgbuf_size;
}

static int alloc_msg_buffers(struct rdma_ch_cb *cb)
{
	struct msgbuf_ctx *mb_ctx;
	int i, ret;

	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];
		// mb_ctx->recv_buf = calloc(1, cb->msgbuf_size);
		mb_ctx->recv_buf = NULL;
		ret = posix_memalign((void **)&mb_ctx->recv_buf,
				     sysconf(_SC_PAGESIZE), cb->msgbuf_size);
		if (ret != 0) {
			// if (!mb_ctx->recv_buf) {
			fprintf(stderr,
				"Allocating message buffer (recv) failed. Error code=%d\n",
				ret);
			return -1;
		}

		log_debug("alloc mb_ctx->recv_buf=%lx", mb_ctx->recv_buf);

		// mb_ctx->send_buf = calloc(1, cb->msgbuf_size);
		mb_ctx->send_buf = NULL;
		ret = posix_memalign((void **)&mb_ctx->send_buf,
				     sysconf(_SC_PAGESIZE), cb->msgbuf_size);
		if (ret != 0) {
			// if (!mb_ctx->send_buf) {
			fprintf(stderr,
				"Allocating message buffer (send) failed. Error code=%d\n",
				ret);
			return -1;
		}

		log_debug("alloc mb_ctx->send_buf=%lx", mb_ctx->send_buf);
	}

	return 0;
}

// TODO: Alloc RDMA buffer for Data Fetcher. Need to implement it the following code is just copied.
// static int alloc_rdma_buffer(struct rdma_ch_cb *cb)
// {
// 	cb->rdma_buf = malloc(cb->size);
// 	if (!cb->rdma_buf) {
// 		fprintf(stderr, "rdma_buf malloc failed\n");
// 		ret = -ENOMEM;
// 		goto err2;
// 	}

// 	// TODO: RDMA is used by DATA FETCHER.
// 	cb->rdma_mr =
// 		ibv_reg_mr(cb->pd, cb->rdma_buf, cb->size,
// 			   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
// 				   IBV_ACCESS_REMOTE_WRITE);
// 	if (!cb->rdma_mr) {
// 		fprintf(stderr, "rdma_buf reg_mr failed\n");
// 		ret = errno;
// 		goto err3;
// 	}

// }

static int setup_buffers(struct rdma_ch_cb *cb)
{
	int ret, i;
	struct msgbuf_ctx *mb_ctx;

	log_debug("setup_buffers called on cb %p", cb);

	// Malloc buffers.
	ret = alloc_msg_buffers(cb);
	if (ret) {
		log_error("Failed to alloc msg buffers.");
		goto err2;
	}

	// Register send/recv buffers to MR.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];

		mb_ctx->recv_mr =
			ibv_reg_mr(cb->pd, mb_ctx->recv_buf, cb->msgbuf_size,
				   IBV_ACCESS_LOCAL_WRITE);
		if (!mb_ctx->recv_mr) {
			log_error("recv_buf reg_mr failed");
			ret = errno;
			goto err1;
		}

		mb_ctx->send_mr = ibv_reg_mr(cb->pd, mb_ctx->send_buf,
					     cb->msgbuf_size, 0);
		if (!mb_ctx->send_mr) {
			log_error("send_buf reg_mr failed");
			ret = errno;
			goto err1;
		}
	}

	setup_wr(cb);
	log_debug("allocated & registered buffers...");
	return 0;

// err5:
// 	free(cb->start_buf);
// err4:
// 	ibv_dereg_mr(cb->rdma_mr);
// err3:
// 	free(cb->rdma_buf);
err1:
	// Deregister MRs.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];
		if (mb_ctx->recv_mr != NULL)
			ibv_dereg_mr(mb_ctx->recv_mr);
		if (mb_ctx->send_mr != NULL)
			ibv_dereg_mr(mb_ctx->send_mr);
	}

err2:
	// Free buffers.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];
		if (mb_ctx->recv_buf != NULL)
			free(mb_ctx->recv_buf);
		if (mb_ctx->send_buf != NULL)
			free(mb_ctx->send_buf);
	}
	return ret;
}

/**
 * @brief Copy the message and invoke a worker thread.
 * 
 * @param cb 
 * @param wc 
 * @return int 
 */
static int receive_msg(struct rdma_ch_cb *cb, struct ibv_wc *wc)
{
	struct msgbuf_ctx *mb_ctx;
	struct rpc_msg_handler_param *rpc_param;
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	int msgbuf_id;
	int ret;

	// Check the size of received data.
	if (wc->byte_len != (uint32_t)cb->msgbuf_size) {
		fprintf(stderr, "Received bogus data, size %d\n", wc->byte_len);
		return -1;
	}

	// These are freed in the handler callback function.
	rpc_param = calloc(1, sizeof *rpc_param);
	if (!rpc_param) {
		ret = -ENOMEM;
		goto err1;
	}
	param = calloc(1, sizeof *param);
	if (!param) {
		ret = -ENOMEM;
		goto err2;
	}
	msg = calloc(1, cb->msgbuf_size);
	if (!msg) {
		ret = -ENOMEM;
		goto err3;
	}

	msgbuf_id = wc->wr_id;

	mb_ctx = &cb->buf_ctxs[msgbuf_id];

	msg->header.seqn = be64toh(mb_ctx->recv_buf->seq_num);
	msg->header.sem = (sem_t *)be64toh(mb_ctx->recv_buf->sem_addr);
	msg->header.client_rpc_ch =
		(struct rpc_ch_info *)be64toh(mb_ctx->recv_buf->rpc_ch_addr);
	// FIXME: Copy fixed size, currently.
	memcpy(&msg->data[0], &mb_ctx->recv_buf->data[0], cb->msgdata_size);

	param->msgbuf_id = msgbuf_id;
	param->ch_cb = (struct rdma_ch_cb *)cb;
	param->msg = msg;

	rpc_param->msgbuf_id = msgbuf_id;
	rpc_param->client_rpc_ch =
		(struct rpc_ch_info *)be64toh(mb_ctx->recv_buf->rpc_ch_addr);
	rpc_param->param = param;
	rpc_param->user_msg_handler_cb = cb->user_msg_handler_cb;

	log_debug(
		"Received msgbuf_id=%d seqn=%lu data=%s rpc_ch_addr=0x%lx sem_addr=0x%lx",
		msgbuf_id, msg->header.seqn, msg->data,
		(uint64_t)rpc_param->client_rpc_ch, (uint64_t)msg->header.sem);

	// Execute RPC callback function in a worker thread.
	thpool_add_work(cb->msg_handler_thpool, cb->rpc_msg_handler_cb,
			(void *)rpc_param);

	// cb->remote_rkey = be32toh(cb->recv_buf.rkey);
	// cb->remote_addr = be64toh(cb->recv_buf.buf);
	// cb->remote_len = be32toh(cb->recv_buf.size);
	// log_debug("Received rkey %x addr %" PRIx64 " len %d from peer",
	// 	  cb->remote_rkey, cb->remote_addr, cb->remote_len);

	// Change server state.
	if (cb->state <= CONNECTED)
		cb->state = WORKING;

	return 0;
err3:
	free(param);
err2:
	free(rpc_param);
err1:
	return ret;
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
			log_error("cq completion failed wc.status=%d",
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
			// cb->state = RDMA_WRITE_COMPLETE;
			// sem_post(&cb->sem);
			break;

		case IBV_WC_RDMA_READ:
			log_debug("rdma read completion");
			// cb->state = RDMA_READ_COMPLETE;
			// sem_post(&cb->sem);
			break;

		case IBV_WC_RECV:
			log_debug("recv completion\n");
			ret = receive_msg(cb, &wc);
			if (ret) {
				log_error("recv wc error: ret=%d", ret);
				goto error;
			}

			ret = ibv_post_recv(
				cb->qp, &cb->buf_ctxs[wc.wr_id].rq_wr,
				&bad_wr); // wc.wr_id stores msgbuf_id.
			if (ret) {
				log_error("post recv error: ret=%d", ret);
				goto error;
			}
			// sem_post(&cb->sem);
			break;

		default:
			log_error("unknown!!!!! completion");
			ret = -1;
			goto error;
		}
	}
	if (ret) {
		log_error("poll error %d", ret);
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
	int i;
	struct msgbuf_ctx *mb_ctx;

	log_debug("free_buffers called on cb %p", cb);

	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];
		ibv_dereg_mr(mb_ctx->recv_mr);
		log_debug("free mb_ctx->recv_buf=%lx", mb_ctx->recv_buf);
		free(mb_ctx->recv_buf);

		ibv_dereg_mr(mb_ctx->send_mr);
		log_debug("free mb_ctx->send_buf=%lx", mb_ctx->send_buf);
		free(mb_ctx->send_buf);

		// ibv_dereg_mr(cb->rdma_mr);
	}

	// free(cb->rdma_buf);
	// if (!cb->server) {
	// 	ibv_dereg_mr(cb->start_mr);
	// 	free(cb->start_buf);
	// }
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
	log_debug("free cb->buf_ctxs=%lx", cb->buf_ctxs);
	free(cb->buf_ctxs);
	log_debug("free cb=%lx", cb);
	free(cb);
}

static void *server_thread(void *arg)
{
	struct rdma_ch_cb *cb = arg;
	struct ibv_recv_wr *bad_wr;
	struct msgbuf_ctx *mb_ctx;
	int i, ret;

	ret = setup_qp(cb, cb->child_cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		goto err0;
	}

	ret = setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "setup_buffers failed: %d\n", ret);
		goto err1;
	}

	// Post recv WR for all the msg buffers.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];

		ret = ibv_post_recv(cb->qp, &mb_ctx->rq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
			goto err2;
		}
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

	while (cb->state != DISCONNECTED) {
		sleep(1);
	}

	rdma_disconnect(cb->child_cm_id);

	// pthread_cancel(cb->cqthread);
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

void *run_server(void *arg)
{
	int ret;
	char res[50];
	struct rdma_ch_cb *cb;
	pthread_attr_t attr;
	struct rdma_ch_cb *listening_cb = (struct rdma_ch_cb *)arg;

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

		// A handler thread is created when there is a new connect request.
		ret = pthread_create(&cb->server_thread, &attr, server_thread,
				     cb);
		if (ret) {
			fprintf(stderr, "pthread_create failed.");
			goto err;
		}
	}

err:
	log_error("Failure in run_server(). ret=%d", ret);

	rdma_destroy_id(listening_cb->cm_id);
	rdma_destroy_event_channel(listening_cb->cm_channel);

	// return -1;

	sprintf(res, "Failure in run_server(). ret=%d", ret);
	pthread_exit(res);
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
		fprintf(stderr, "rdma_resolve_addr\n");
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

// static void format_send(struct rdma_ch_cb *cb, char *buf, struct ibv_mr *mr)
// {
// 	struct comm_rdma_mr_info *info = &cb->send_buf;

// 	info->buf = htobe64((uint64_t)(unsigned long)buf);
// 	info->rkey = htobe32(mr->rkey);
// 	info->size = htobe32(cb->size);

// 	log_debug("RDMA addr %" PRIx64 " rkey %x len %d", be64toh(info->buf),
// 		  be32toh(info->rkey), be32toh(info->size));
// }

static inline uint64_t alloc_seqn(struct msgbuf_ctx *mb_ctx)
{
	uint64_t ret;

	ret = atomic_fetch_add(&mb_ctx->seqn, 1);
	return ret;
}

int send_rdma_msg(struct rdma_ch_cb *cb, void *rpc_ch_addr, char *data,
		  sem_t *sem, int msgbuf_id, uint64_t seqn)
{
	struct ibv_send_wr *bad_wr;
	struct rdma_msg *msg;
	struct msgbuf_ctx *mb_ctx;
	int ret;
	uint64_t new_seqn;
	// uint64_t data_size, remains;

	if (msgbuf_id >= cb->msgbuf_cnt) {
		log_error(
			"msg buffer id(%d) exceeds total msg buffer count(%d).",
			msgbuf_id, cb->msgbuf_cnt);
		return 0;
	}

	mb_ctx = &cb->buf_ctxs[msgbuf_id];

	msg = mb_ctx->send_buf;
	if (!seqn) {
		new_seqn = alloc_seqn(mb_ctx);
		msg->seq_num = htobe64(new_seqn);
	} else {
		msg->seq_num = htobe64(seqn);
	}
	msg->sem_addr = htobe64((uint64_t)(unsigned long)sem);
	msg->rpc_ch_addr = htobe64((uint64_t)(unsigned long)rpc_ch_addr);

	// Copy and send fixed size, currently.
	// FIXME: Can we copy only the meaningful data? memset will be required.
	// memset(&msg->data[0], 0, cb->msgdata_size);
	memcpy(&msg->data[0], data, cb->msgdata_size);

	// No duplicate memset: (how to get data_size?)
	// memcpy(&msg->data[0], data, data_size);
	// remains = cb->msgdata_size - data_size;
	// memset(&msg->data[data_size], 0, remains);

	log_info(
		"Sending RDMA msg: seqn=%lu sem_addr=%lx rpc_ch_addr=%lx data=\"%s\"",
		msg->seq_num, (uint64_t)sem, (uint64_t)rpc_ch_addr, msg->data);

	ret = ibv_post_send(cb->qp, &mb_ctx->sq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "post send error %d\n", ret);
		return 0;
	}

	// Do not wait completion or ack. FIXME: Do we need to wait completion?
	// A msg buffer is being allocated until the client receives a response (or an ack) from the server.

	return cb->msgbuf_size; // Send fixed size, currently.
}

static int run_client(struct rdma_ch_cb *cb)
{
	struct ibv_recv_wr *bad_wr;
	struct msgbuf_ctx *mb_ctx;
	int i, ret;

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

	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];

		ret = ibv_post_recv(cb->qp, &mb_ctx->rq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
			goto err2;
		}
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		fprintf(stderr, "pthread_create\n");
		goto err2;
	}

	ret = connect_client(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	return 0;

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

/**
 * @brief 
 * 
 * @param attr Attributes for an RDMA connection.
 * @return struct rdma_ch_cb* Returns rdma_ch_cb pointer. Return NULL on failure.
 */
struct rdma_ch_cb *init_rdma_ch(struct rdma_ch_attr *attr)
{
	struct rdma_ch_cb *cb;
	struct msgbuf_ctx *mb_ctx;
	int ret, i;

	cb = calloc(1, sizeof(struct rdma_ch_cb));
	if (!cb) {
		ret = -ENOMEM;
		goto out5;
	}

	cb->msgbuf_cnt = attr->msgbuf_cnt;
	cb->msgheader_size = msgheader_size();
	cb->msgdata_size = attr->msgdata_size;
	cb->msgbuf_size = msgbuf_size(attr->msgdata_size);
	cb->rpc_msg_handler_cb = attr->rpc_msg_handler_cb;
	cb->user_msg_handler_cb = attr->user_msg_handler_cb;
	cb->msg_handler_thpool = attr->msg_handler_thpool;

	cb->buf_ctxs = calloc(cb->msgbuf_cnt, sizeof(struct msgbuf_ctx));
	if (!cb->buf_ctxs) {
		ret = -ENOMEM;
		log_error("calloc failed.");
		goto out4;
	}

	// Some initialization.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &cb->buf_ctxs[i];
		mb_ctx->id = i; // Set ID.
		atomic_init(&mb_ctx->seqn, 1); // Start from 1.
	}

	cb->server = attr->server;
	cb->state = IDLE;
	cb->size = cb->msgbuf_size; // msg buffer size.
	cb->sin.ss_family = AF_INET;
	cb->port = htobe16(attr->port);
	sem_init(&cb->sem, 0, 0); // FIXME: Where is it used?

	if (!cb->server) {
		ret = get_addr(attr->ip_addr, (struct sockaddr *)&cb->sin);
		if (ret)
			goto out3;
	}

	cb->cm_channel = create_first_event_channel();
	if (!cb->cm_channel) {
		ret = errno;
		goto out3;
	}

	ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);

	if (ret) {
		printf("rdma_create_id failed.\n");
		goto out2;
	}

	ret = pthread_create(&cb->cmthread, NULL, cm_thread, cb);
	if (ret) {
		printf("Creating cm_thread failed.\n");
		goto out1;
	}

	if (cb->server) {
		ret = pthread_create(&cb->server_daemon, NULL, run_server, cb);
		if (ret) {
			printf("Creating server daemon failed.\n");
			goto out1;
		}
	} else
		run_client(cb);

	return cb;

	printf("Destroy rdma connections.\n");
out1:
	rdma_destroy_id(cb->cm_id);
out2:
	rdma_destroy_event_channel(cb->cm_channel);
out3:
	free(cb->buf_ctxs);
out4:
	free(cb);
out5:
	printf("init_rdma_ch failed. ret=%d\n", ret);
	return NULL;
}

/**
 * @brief Called by client.
 * 
 * @param cb 
 */
void destroy_rdma_client(struct rdma_ch_cb *cb)
{
	if (cb->server) {
		log_warn("destroy_rdma_ch() is for client.");
		return;
	}

	rdma_disconnect(cb->cm_id);
	pthread_join(cb->cqthread, NULL);
	free_buffers(cb);
	free_qp(cb);

	rdma_destroy_id(cb->cm_id);
	rdma_destroy_event_channel(cb->cm_channel);
	free(cb->buf_ctxs);
	free(cb);
}