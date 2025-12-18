#include "network.h"
#include "mailbox.h"
#include "wire_format.h"

#include <zmq.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

// ============================================================================
// Constants
// ============================================================================

#define RAFT_PORT_BASE  5000
#define INV_PORT_BASE   6000
#define MAX_PEERS       16
#define POLL_TIMEOUT_MS 10

// ============================================================================
// Internal Structure
// ============================================================================

struct network {
    // Configuration
    int          node_id;
    peer_info_t  peers[MAX_PEERS];
    int          num_peers;

    // ZeroMQ
    void *zmq_ctx;
    void *raft_router;              // ROUTER - receives Raft msgs
    void *raft_dealers[MAX_PEERS];  // DEALER per peer - sends Raft msgs
    void *inv_pub;                  // PUB - broadcasts INV
    void *inv_sub;                  // SUB - receives INV from all

    // Mailboxes
    mailbox_t *raft_inbox;   // Incoming Raft messages
    mailbox_t *raft_outbox;  // Outgoing Raft messages
    mailbox_t *inv_inbox;    // Incoming INV messages

    // Thread
    pthread_t net_thread;
    volatile int running;
};

// ============================================================================
// Peer Loading
// ============================================================================

int network_load_peers(const char *path, peer_info_t *peers, int max_peers)
{
    if (!path || !peers || max_peers <= 0) {
        return -1;
    }

    FILE *f = fopen(path, "r");
    if (!f) {
        return -1;
    }

    int count = 0;
    char line[256];

    while (fgets(line, sizeof(line), f) && count < max_peers) {
        // Skip comments and empty lines
        if (line[0] == '#' || line[0] == '\n') {
            continue;
        }

        int id;
        char addr[128];

        if (sscanf(line, "%d %127s", &id, addr) == 2) {
            peers[count].id = id;
            snprintf(peers[count].address, sizeof(peers[count].address),
                     "%s", addr);
            snprintf(peers[count].raft_endpoint, sizeof(peers[count].raft_endpoint),
                     "tcp://%s:%d", addr, RAFT_PORT_BASE + id);
            snprintf(peers[count].inv_endpoint, sizeof(peers[count].inv_endpoint),
                     "tcp://%s:%d", addr, INV_PORT_BASE + id);
            count++;
        }
    }

    fclose(f);
    return count;
}

// ============================================================================
// Network Thread
// ============================================================================

static void *network_thread_func(void *arg)
{
    network_t *net = (network_t *)arg;

    uint8_t buf[65536];

    while (net->running) {
        // =====================================================================
        // 1. Drain outbox, send Raft messages
        // =====================================================================
        mail_t mail;
        while (mailbox_pop(net->raft_outbox, &mail) == 0) {
            int peer_id = mail.peer_id;

            // Find dealer for this peer
            void *dealer = NULL;
            for (int i = 0; i < net->num_peers; i++) {
                if (net->peers[i].id == peer_id) {
                    dealer = net->raft_dealers[i];
                    break;
                }
            }

            if (dealer) {
                // Encode wire format
                size_t wire_len = wire_encode(buf, mail.msg_type, net->node_id,
                                              mail.data, mail.len);
                zmq_send(dealer, buf, wire_len, ZMQ_DONTWAIT);
            }

            // Free message data
            if (mail.data) {
                free(mail.data);
            }
        }

        // =====================================================================
        // 2. Poll for incoming messages
        // =====================================================================
        zmq_pollitem_t items[] = {
            { net->raft_router, 0, ZMQ_POLLIN, 0 },
            { net->inv_sub,     0, ZMQ_POLLIN, 0 },
        };

        int rc = zmq_poll(items, 2, POLL_TIMEOUT_MS);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // =====================================================================
        // 3. Receive Raft messages
        // =====================================================================
        if (items[0].revents & ZMQ_POLLIN) {
            // ROUTER gives us [identity][empty][data]
            zmq_msg_t identity, empty, data;
            zmq_msg_init(&identity);
            zmq_msg_init(&empty);
            zmq_msg_init(&data);

            if (zmq_msg_recv(&identity, net->raft_router, 0) >= 0 &&
                zmq_msg_recv(&empty, net->raft_router, 0) >= 0 &&
                zmq_msg_recv(&data, net->raft_router, 0) >= 0) {

                size_t len = zmq_msg_size(&data);
                if (len >= WIRE_HEADER_SIZE) {
                    wire_header_t hdr;
                    const void *payload = wire_decode(zmq_msg_data(&data), len, &hdr);

                    if (payload) {
                        // Copy payload to heap
                        uint8_t *payload_copy = NULL;
                        if (hdr.len > 0) {
                            payload_copy = malloc(hdr.len);
                            if (payload_copy) {
                                memcpy(payload_copy, payload, hdr.len);
                            }
                        }

                        mail_t incoming = {
                            .peer_id = hdr.from_id,
                            .msg_type = hdr.type,
                            .len = hdr.len,
                            .data = payload_copy,
                        };

                        if (mailbox_push(net->raft_inbox, &incoming) != 0) {
                            // Inbox full, drop message
                            free(payload_copy);
                        }
                    }
                }
            }

            zmq_msg_close(&identity);
            zmq_msg_close(&empty);
            zmq_msg_close(&data);
        }

        // =====================================================================
        // 4. Receive INV broadcasts
        // =====================================================================
        if (items[1].revents & ZMQ_POLLIN) {
            int len = zmq_recv(net->inv_sub, buf, sizeof(buf), ZMQ_DONTWAIT);
            if (len >= WIRE_HEADER_SIZE) {
                wire_header_t hdr;
                const void *payload = wire_decode(buf, len, &hdr);

                if (payload && hdr.type == MSG_INV) {
                    uint8_t *key_copy = NULL;
                    if (hdr.len > 0) {
                        key_copy = malloc(hdr.len);
                        if (key_copy) {
                            memcpy(key_copy, payload, hdr.len);
                        }
                    }

                    mail_t incoming = {
                        .peer_id = hdr.from_id,
                        .msg_type = MSG_INV,
                        .len = hdr.len,
                        .data = key_copy,
                    };

                    if (mailbox_push(net->inv_inbox, &incoming) != 0) {
                        free(key_copy);
                    }
                }
            }
        }
    }

    return NULL;
}

// ============================================================================
// Lifecycle
// ============================================================================

network_t *network_create(const network_config_t *cfg)
{
    if (!cfg || !cfg->peers || cfg->num_peers <= 0) {
        return NULL;
    }

    network_t *net = calloc(1, sizeof(network_t));
    if (!net) return NULL;

    net->node_id = cfg->node_id;
    net->num_peers = cfg->num_peers;
    memcpy(net->peers, cfg->peers, cfg->num_peers * sizeof(peer_info_t));

    // Create ZMQ context
    net->zmq_ctx = zmq_ctx_new();
    if (!net->zmq_ctx) {
        goto fail;
    }

    // =========================================================================
    // Raft ROUTER - bind to receive from all peers
    // =========================================================================
    net->raft_router = zmq_socket(net->zmq_ctx, ZMQ_ROUTER);
    if (!net->raft_router) {
        goto fail;
    }

    char bind_addr[256];
    snprintf(bind_addr, sizeof(bind_addr), "tcp://*:%d", RAFT_PORT_BASE + net->node_id);
    if (zmq_bind(net->raft_router, bind_addr) != 0) {
        fprintf(stderr, "Failed to bind ROUTER to %s: %s\n", bind_addr, zmq_strerror(errno));
        goto fail;
    }

    // =========================================================================
    // Raft DEALERs - connect to each peer
    // =========================================================================
    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == net->node_id) {
            // Don't connect to self
            net->raft_dealers[i] = NULL;
            continue;
        }

        net->raft_dealers[i] = zmq_socket(net->zmq_ctx, ZMQ_DEALER);
        if (!net->raft_dealers[i]) {
            goto fail;
        }

        // Set identity so ROUTER knows who we are
        char identity[16];
        snprintf(identity, sizeof(identity), "node%d", net->node_id);
        zmq_setsockopt(net->raft_dealers[i], ZMQ_IDENTITY, identity, strlen(identity));

        if (zmq_connect(net->raft_dealers[i], net->peers[i].raft_endpoint) != 0) {
            fprintf(stderr, "Failed to connect DEALER to %s: %s\n",
                    net->peers[i].raft_endpoint, zmq_strerror(errno));
            goto fail;
        }
    }

    // =========================================================================
    // INV PUB - bind to broadcast invalidations
    // =========================================================================
    net->inv_pub = zmq_socket(net->zmq_ctx, ZMQ_PUB);
    if (!net->inv_pub) {
        goto fail;
    }

    snprintf(bind_addr, sizeof(bind_addr), "tcp://*:%d", INV_PORT_BASE + net->node_id);
    if (zmq_bind(net->inv_pub, bind_addr) != 0) {
        fprintf(stderr, "Failed to bind PUB to %s: %s\n", bind_addr, zmq_strerror(errno));
        goto fail;
    }

    // =========================================================================
    // INV SUB - connect to all peers' PUB sockets
    // =========================================================================
    net->inv_sub = zmq_socket(net->zmq_ctx, ZMQ_SUB);
    if (!net->inv_sub) {
        goto fail;
    }

    // Subscribe to all messages
    zmq_setsockopt(net->inv_sub, ZMQ_SUBSCRIBE, "", 0);

    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == net->node_id) {
            continue;
        }

        if (zmq_connect(net->inv_sub, net->peers[i].inv_endpoint) != 0) {
            fprintf(stderr, "Failed to connect SUB to %s: %s\n",
                    net->peers[i].inv_endpoint, zmq_strerror(errno));
            goto fail;
        }
    }

    // =========================================================================
    // Create mailboxes
    // =========================================================================
    size_t mailbox_size = cfg->mailbox_size > 0 ? cfg->mailbox_size : 256;

    net->raft_inbox = mailbox_create(mailbox_size);
    net->raft_outbox = mailbox_create(mailbox_size);
    net->inv_inbox = mailbox_create(mailbox_size);

    if (!net->raft_inbox || !net->raft_outbox || !net->inv_inbox) {
        goto fail;
    }

    return net;

fail:
    network_destroy(net);
    return NULL;
}

void network_destroy(network_t *net)
{
    if (!net) return;

    network_stop(net);

    // Close sockets
    if (net->raft_router) zmq_close(net->raft_router);
    if (net->inv_pub) zmq_close(net->inv_pub);
    if (net->inv_sub) zmq_close(net->inv_sub);

    for (int i = 0; i < net->num_peers; i++) {
        if (net->raft_dealers[i]) {
            zmq_close(net->raft_dealers[i]);
        }
    }

    // Destroy context
    if (net->zmq_ctx) {
        zmq_ctx_destroy(net->zmq_ctx);
    }

    // Destroy mailboxes
    mailbox_destroy(net->raft_inbox);
    mailbox_destroy(net->raft_outbox);
    mailbox_destroy(net->inv_inbox);

    free(net);
}

int network_start(network_t *net)
{
    if (!net || net->running) return -1;

    net->running = 1;

    if (pthread_create(&net->net_thread, NULL, network_thread_func, net) != 0) {
        net->running = 0;
        return -1;
    }

    return 0;
}

void network_stop(network_t *net)
{
    if (!net || !net->running) return;

    net->running = 0;
    pthread_join(net->net_thread, NULL);
}

// ============================================================================
// Send/Receive
// ============================================================================

int network_send_raft(network_t *net, int peer_id, uint8_t msg_type,
                      const void *data, size_t len)
{
    if (!net) return -1;

    // Copy data to heap
    uint8_t *data_copy = NULL;
    if (data && len > 0) {
        data_copy = malloc(len);
        if (!data_copy) return -1;
        memcpy(data_copy, data, len);
    }

    mail_t mail = {
        .peer_id = peer_id,
        .msg_type = msg_type,
        .len = (uint32_t)len,
        .data = data_copy,
    };

    if (mailbox_push(net->raft_outbox, &mail) != 0) {
        free(data_copy);
        return -1;
    }

    return 0;
}

int network_recv_raft(network_t *net, int *from_id, uint8_t *msg_type,
                      void *buf, size_t buf_cap)
{
    if (!net) return -1;

    mail_t mail;
    if (mailbox_pop(net->raft_inbox, &mail) != 0) {
        return 0;  // Empty
    }

    if (from_id) *from_id = mail.peer_id;
    if (msg_type) *msg_type = mail.msg_type;

    size_t copy_len = mail.len < buf_cap ? mail.len : buf_cap;
    if (buf && mail.data && copy_len > 0) {
        memcpy(buf, mail.data, copy_len);
    }

    free(mail.data);
    return (int)mail.len;
}

int network_broadcast_inv(network_t *net, const void *key, size_t klen)
{
    if (!net || !net->inv_pub) return -1;

    uint8_t buf[1024];
    size_t wire_len = wire_encode(buf, MSG_INV, net->node_id, key, (uint16_t)klen);

    // Fire and forget
    zmq_send(net->inv_pub, buf, wire_len, ZMQ_DONTWAIT);
    return 0;
}

int network_recv_inv(network_t *net, int *from_id, void *key_buf, size_t buf_cap)
{
    if (!net) return -1;

    mail_t mail;
    if (mailbox_pop(net->inv_inbox, &mail) != 0) {
        return 0;  // Empty
    }

    if (from_id) *from_id = mail.peer_id;

    size_t copy_len = mail.len < buf_cap ? mail.len : buf_cap;
    if (key_buf && mail.data && copy_len > 0) {
        memcpy(key_buf, mail.data, copy_len);
    }

    free(mail.data);
    return (int)mail.len;
}

// ============================================================================
// Utilities
// ============================================================================

int network_get_node_id(const network_t *net)
{
    return net ? net->node_id : -1;
}

int network_get_peer_count(const network_t *net)
{
    return net ? net->num_peers : 0;
}

int network_peer_connected(const network_t *net, int peer_id)
{
    if (!net) return 0;

    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == peer_id) {
            // ZMQ auto-connects, so if we have a dealer, we're "connected"
            return net->raft_dealers[i] != NULL;
        }
    }
    return 0;
}