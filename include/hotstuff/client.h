/**
 * Copyright 2018 VMware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _HOTSTUFF_CLIENT_H
#define _HOTSTUFF_CLIENT_H

#include "salticidae/msg.h"
#include "hotstuff/type.h"
#include "hotstuff/entity.h"
#include "hotstuff/consensus.h"

#define HOTSTUFF_CMD_REQSIZE 1024

namespace hotstuff {

struct MsgReqCmd {
    static const opcode_t opcode = 0x4;
//    int cid;
    DataStream serialized;
    command_t cmd;
    MsgReqCmd(const Command &cmd) { serialized << cmd; }
//    MsgReqCmd(const Command &cmd, int cmd_cid) { serialized << cmd; cid = cmd_cid; }
    MsgReqCmd(DataStream &&s): serialized(std::move(s)) {}
};

struct MsgRespCmd {
    static const opcode_t opcode = 0x5;
    DataStream serialized;
#if HOTSTUFF_CMD_RESPSIZE > 0
    uint8_t payload[HOTSTUFF_CMD_RESPSIZE];
#endif
    Finality fin;
    MsgRespCmd(const Finality &fin) {
        serialized << fin;
#if HOTSTUFF_CMD_RESPSIZE > 0
        serialized.put_data(payload, payload + sizeof(payload));
#endif
    }
    MsgRespCmd(DataStream &&s) {
        s >> fin;
    }
};

//#ifdef HOTSTUFF_AUTOCLI
//struct MsgDemandCmd {
//    static const opcode_t opcode = 0x6;
//    DataStream serialized;
//    size_t ncmd;
//    MsgDemandCmd(size_t ncmd) { serialized << ncmd; }
//    MsgDemandCmd(DataStream &&s) { s >> ncmd; }
//};
//#endif

class CommandDummy: public Command {
    uint32_t cid;
    uint32_t n;

    uint32_t ip_p1;
    uint32_t ip_p2;
    uint32_t ip_p3;
    uint32_t ip_p4;
    uint32_t new_node_id;
    uint32_t new_node_cluster_id;
    uint32_t new_node_key1;
    uint32_t new_node_key2;


    uint256_t hash;
#if HOTSTUFF_CMD_REQSIZE > 0
    uint8_t payload[HOTSTUFF_CMD_REQSIZE];
#endif

    public:
    CommandDummy() {}
    ~CommandDummy() override {}

    CommandDummy(uint32_t cid, uint32_t n):
        cid(cid), n(n),  hash(salticidae::get_hash(*this)) {}

    void serialize(DataStream &s) const override {
        s << cid << n ;
#if HOTSTUFF_CMD_REQSIZE > 0
        s.put_data(payload, payload + sizeof(payload));
#endif
    }

    void unserialize(DataStream &s) override {
        s >> cid >> n ;
#if HOTSTUFF_CMD_REQSIZE > 0
        auto base = s.get_data_inplace(HOTSTUFF_CMD_REQSIZE);
        memmove(payload, base, sizeof(payload));
#endif
        hash = salticidae::get_hash(*this);
    }

    const uint256_t &get_hash() const override {
        return hash;
    }

    const uint32_t &get_cid() const {
        return n;
    }

        const uint32_t &get_ip1() const {
            return ip_p1;
        }

        const uint32_t &get_ip2() const {
            return ip_p2;
        }

        const uint32_t &get_ip3() const {
            return ip_p3;
        }

        const uint32_t &get_ip4() const {
            return ip_p4;
        }

    bool verify() const override {
        return true;
    }
};

}

#endif
