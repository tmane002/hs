/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
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

#include <cassert>
#include <random>
#include <memory>
#include <signal.h>
#include <sys/time.h>

#include "salticidae/type.h"
#include "salticidae/netaddr.h"
#include "salticidae/network.h"
#include "salticidae/util.h"

#include "hotstuff/util.h"
#include "hotstuff/type.h"
#include "hotstuff/client.h"

using salticidae::Config;

using hotstuff::ReplicaID;
using hotstuff::NetAddr;
using hotstuff::EventContext;
using hotstuff::MsgReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::CommandDummy;
using hotstuff::HotStuffError;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::command_t;

EventContext ec;
ReplicaID proposer;
size_t max_async_num;
int max_iter_num;
uint32_t cid;
uint32_t cnt = 0;
uint32_t nfaulty;

uint32_t n_clusters;

struct Request {
    command_t cmd;
    size_t confirmed;
    salticidae::ElapsedTime et;
    Request(const command_t &cmd): cmd(cmd), confirmed(0) { et.start(); }
};

using Net = salticidae::MsgNetwork<opcode_t>;

std::unordered_map<ReplicaID, Net::conn_t> conns;
std::unordered_map<ReplicaID, Net::conn_t> my_conns;
std::unordered_map<ReplicaID, Net::conn_t> other_conns;
std::unordered_map<int, int> cluster_map;

std::unordered_map<const uint256_t, Request> waiting;
std::vector<NetAddr> replicas;
std::vector<std::pair<struct timeval, double>> elapsed;
std::unique_ptr<Net> mn;

void connect_all()
{

    cluster_map[0] = 0;
    cluster_map[1] = 0;
    cluster_map[2] = 0;
    cluster_map[3] = 0;
    cluster_map[4] = 1;
    cluster_map[5] = 1;
    cluster_map[6] = 1;
    cluster_map[7] = 1;
    cluster_map[8] = 2;
    cluster_map[9] = 2;
    cluster_map[10] = 2;
    cluster_map[11] = 2;
//    cluster_map[12] = 1;
//    cluster_map[13] = 1;
//    cluster_map[14] = 1;
//    cluster_map[15] = 1;

//    cluster_map[9] = 2;
//    cluster_map[10] = 2;
//    cluster_map[11] = 2;


//    cluster_map[12] = 0;
//    cluster_map[13] = 0;
//    cluster_map[14] = 0;
//    cluster_map[15] = 0;
//    cluster_map[16] = 0;
//    cluster_map[17] = 0;
//    cluster_map[18] = 0;
//    cluster_map[19] = 0;
//
//    cluster_map[20] = 1;
//    cluster_map[21] = 1;
//    cluster_map[22] = 1;
//    cluster_map[23] = 1;
//    cluster_map[24] = 1;
//    cluster_map[25] = 1;
//    cluster_map[26] = 1;
//    cluster_map[27] = 1;
//    cluster_map[28] = 1;
//    cluster_map[29] = 1;
//    cluster_map[30] = 1;
//    cluster_map[31] = 1;
//    cluster_map[32] = 1;
//    cluster_map[33] = 1;
//    cluster_map[34] = 1;
//    cluster_map[35] = 1;
//    cluster_map[36] = 1;
//    cluster_map[37] = 1;
//    cluster_map[38] = 1;
//    cluster_map[39] = 1;


    n_clusters = 0;
    for (auto it : cluster_map)
    {
        if (it.second > n_clusters)
        {
            n_clusters++;
        }
    }
    n_clusters++;


    for (size_t i = 0; i < replicas.size(); i++)
    {
        if (cluster_map[int(i)]==int(cid % n_clusters))
        {
            conns.insert(std::make_pair(i, mn->connect_sync(replicas[i])));
        }
    }
}

bool try_send(bool check = true) {
    if ((!check || waiting.size() < max_async_num) && max_iter_num)
    {
//        HOTSTUFF_LOG_INFO("cid is %d",int(cid));
        auto cmd = new CommandDummy(cid, cnt++);

        MsgReqCmd msg(*cmd);

        int count = 0;

        for (auto &p: conns)
        {
            mn->send_msg(msg, p.second);
            count++;
        }


#ifndef HOTSTUFF_ENABLE_BENCHMARK
        HOTSTUFF_LOG_INFO("send new cmd %.10s",
                          get_hex(cmd->get_hash()).c_str());
#endif

        waiting.insert(std::make_pair(
                cmd->get_hash(), Request(cmd)));
//        waiting.insert(std::make_pair(
//                cmd1->get_hash(), Request(cmd1)));
//

        HOTSTUFF_LOG_INFO("sent message with waiting size =%d and cid:%d with cmd %.10s",
                          waiting.size(), int(cnt),get_hex(cmd->get_hash()).c_str() );

        if (max_iter_num > 0)
        {
            max_iter_num--;
//            max_iter_num--;
        }

        return true;



    }
    return false;
}

void client_resp_cmd_handler(MsgRespCmd &&msg, const Net::conn_t &) {
    auto &fin = msg.fin;
    HOTSTUFF_LOG_INFO("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = fin.cmd_hash;



    auto it = waiting.find(cmd_hash);
    auto &et = it->second.et;

    if (it == waiting.end()) return;
    et.stop();

//    HOTSTUFF_LOG_INFO("got %s, till here with it->second.confirmed: %d, cid:%d",
//                      std::string(msg.fin).c_str(), int(it->second.confirmed),
//                      int(it->second.cmd->get_cid()));
    if (++it->second.confirmed <= nfaulty) return; // wait for f + 1 ack
//#ifndef HOTSTUFF_ENABLE_BENCHMARK
    HOTSTUFF_LOG_INFO("%.6f ",
                        std::string(fin).c_str(),
                        et.elapsed_sec);
//#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    elapsed.push_back(std::make_pair(tv, et.elapsed_sec));
//#endif
//    HOTSTUFF_LOG_INFO("got response for cid:%d", int(it->second.cmd->get_cid()));

    waiting.erase(it);
    while (try_send());
}

std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = salticidae::trim_all(salticidae::split(s, ";"));
    return std::make_pair(ret[0], ret[1]);
}

int main(int argc, char **argv) {
    Config config("hotstuff.gen.conf");

    auto opt_idx = Config::OptValInt::create(0);
    auto opt_replicas = Config::OptValStrVec::create();
    auto opt_max_iter_num = Config::OptValInt::create(100);
    auto opt_max_async_num = Config::OptValInt::create(10);
    auto opt_cid = Config::OptValInt::create(-1);
    auto opt_max_cli_msg = Config::OptValInt::create(65536); // 64K by default

    auto shutdown = [&](int) { ec.stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    mn = std::make_unique<Net>(ec, Net::Config().max_msg_size(opt_max_cli_msg->get()));
    mn->reg_handler(client_resp_cmd_handler);
    mn->start();

    config.add_opt("idx", opt_idx, Config::SET_VAL);
    config.add_opt("cid", opt_cid, Config::SET_VAL);
    config.add_opt("replica", opt_replicas, Config::APPEND);
    config.add_opt("iter", opt_max_iter_num, Config::SET_VAL);
    config.add_opt("max-async", opt_max_async_num, Config::SET_VAL);
    config.add_opt("max-cli-msg", opt_max_cli_msg, Config::SET_VAL, 'S', "the maximum client message size");
    config.parse(argc, argv);
    auto idx = opt_idx->get();
    max_iter_num = opt_max_iter_num->get();
    max_async_num = opt_max_async_num->get();
    std::vector<std::string> raw;

    int countt = 0;
    for (const auto &s: opt_replicas->get())
    {
        HOTSTUFF_LOG_INFO("counting replicas: %d \n", countt);
        auto res = salticidae::trim_all(salticidae::split(s, ","));
        if (res.size() < 1)
            throw HotStuffError("format error");

        if (cluster_map[countt]==int(cid%2))
        {
            raw.push_back(res[0]);
        }

        countt++;
    }

    if (!(0 <= idx && (size_t)idx < raw.size() && raw.size() > 0))
        throw std::invalid_argument("out of range");
    cid = opt_cid->get() != -1 ? opt_cid->get() : idx;
//    HOTSTUFF_LOG_INFO("-> cid = %d", int(nfaulty));
    HOTSTUFF_LOG_INFO("-> cid, idx = %d, %d", int(cid), int((size_t)idx));
    for (const auto &p: raw)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        replicas.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
    }

    nfaulty = 1;//(replicas.size() - 1) / 3;
    HOTSTUFF_LOG_INFO("nfaulty = %zu", nfaulty);
    connect_all();
    while (try_send());
    ec.dispatch();

//#ifdef HOTSTUFF_ENABLE_BENCHMARK
    for (const auto &e: elapsed)
    {
        char fmt[64];
        struct tm *tmp = localtime(&e.first.tv_sec);
        strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
        fprintf(stdout, fmt, e.first.tv_usec, e.second);
    }
//#endif
    return 0;
}
