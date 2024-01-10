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

#include "hotstuff/hotstuff.h"
#include "hotstuff/client.h"
#include "hotstuff/liveness.h"

using salticidae::static_pointer_cast;
using salticidae::get_hex10;


#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_INFO
#define LOG_WARN HOTSTUFF_LOG_INFO

namespace hotstuff {

    const opcode_t MsgPropose::opcode;
    MsgPropose::MsgPropose(const Proposal &proposal) { serialized << proposal; }
    void MsgPropose::postponed_parse(HotStuffCore *hsc) {
        proposal.hsc = hsc;
        serialized >> proposal;
    }



    const opcode_t MsgTest::opcode;
    MsgTest::MsgTest(const std::vector<block_t> &blks) {
        serialized << htole((uint32_t)blks.size());
        for (auto blk: blks) serialized << *blk;
    }

    void MsgTest::postponed_parse(HotStuffCore *hsc) {
        uint32_t size;
        serialized >> size;
        size = letoh(size);
        blks.resize(size);
        for (auto &blk: blks)
        {
            Block _blk;
            _blk.unserialize(serialized, hsc);
            HOTSTUFF_LOG_INFO("MsgTest::postponed_parse: _blk.get_height() is %d",
                              int(_blk.get_height()) );

        }
    }

    const opcode_t MsgVote::opcode;
    MsgVote::MsgVote(const Vote &vote) { serialized << vote; }
    void MsgVote::postponed_parse(HotStuffCore *hsc) {
        vote.hsc = hsc;
        serialized >> vote;
    }

    const opcode_t MsgReqBlock::opcode;
    MsgReqBlock::MsgReqBlock(const std::vector<uint256_t> &blk_hashes) {
        serialized << htole((uint32_t)blk_hashes.size());
        for (const auto &h: blk_hashes)
            serialized << h;
    }

    MsgReqBlock::MsgReqBlock(DataStream &&s) {
        uint32_t size;
        s >> size;
        size = letoh(size);
        blk_hashes.resize(size);
        for (auto &h: blk_hashes) s >> h;
    }

    const opcode_t MsgRespBlock::opcode;
    MsgRespBlock::MsgRespBlock(const std::vector<block_t> &blks) {
        serialized << htole((uint32_t)blks.size());
        for (auto blk: blks) serialized << *blk;
    }

    void MsgRespBlock::postponed_parse(HotStuffCore *hsc) {
        uint32_t size;
        serialized >> size;
        size = letoh(size);
        blks.resize(size);
        for (auto &blk: blks)
        {
            Block _blk;
            _blk.unserialize(serialized, hsc);
            HOTSTUFF_LOG_INFO("MsgRespBlock::postponed_parse: _blk.get_height() is %d",
                              int(_blk.get_height()) );

            blk = hsc->storage->add_blk(std::move(_blk), hsc->get_config());
        }
    }

// TODO: improve this function
    void HotStuffBase::exec_command(uint256_t cmd_hash, int key, int val, commit_cb_t callback) {

        HOTSTUFF_LOG_INFO("exec_command, adding to cmd_pending with current size with key= %d, val = %d",
                          key, val);


//        key_val_store[cmd_hash] = std::pair(key,val);
//
//        HOTSTUFF_LOG_INFO("exec_cmd with key, val = %d, %d", key_val_store[cmd_hash].first, key_val_store[cmd_hash].second);


        cmd_pending.enqueue(std::make_pair(std::make_pair(cmd_hash,std::make_pair(key, val)), callback));


    }

    void HotStuffBase::on_fetch_blk(const block_t &blk) {
#ifdef HOTSTUFF_BLK_PROFILE
        blk_profiler.get_tx(blk->get_hash());
#endif
        LOG_DEBUG("fetched %.10s", get_hex(blk->get_hash()).c_str());
        part_fetched++;
        fetched++;
        //for (auto cmd: blk->get_cmds()) on_fetch_cmd(cmd);
        const uint256_t &blk_hash = blk->get_hash();
        auto it = blk_fetch_waiting.find(blk_hash);
        if (it != blk_fetch_waiting.end())
        {
            it->second.resolve(blk);
            blk_fetch_waiting.erase(it);
        }
    }

    bool HotStuffBase::on_deliver_blk(const block_t &blk) {
        const uint256_t &blk_hash = blk->get_hash();
        bool valid;
        /* sanity check: all parents must be delivered */
        for (const auto &p: blk->get_parent_hashes())
            assert(storage->is_blk_delivered(p));
        if ((valid = HotStuffCore::on_deliver_blk(blk)))
        {
            LOG_DEBUG("block %.10s delivered",
                      get_hex(blk_hash).c_str());
            part_parent_size += blk->get_parent_hashes().size();
            part_delivered++;
            delivered++;
        }
        else
        {
            LOG_WARN("dropping invalid block");
        }

        bool res = true;
        auto it = blk_delivery_waiting.find(blk_hash);
        if (it != blk_delivery_waiting.end())
        {
            auto &pm = it->second;
            if (valid)
            {
                pm.elapsed.stop(false);
                auto sec = pm.elapsed.elapsed_sec;
                part_delivery_time += sec;
                part_delivery_time_min = std::min(part_delivery_time_min, sec);
                part_delivery_time_max = std::max(part_delivery_time_max, sec);

                pm.resolve(blk);
            }
            else
            {
                pm.reject(blk);
                res = false;
                // TODO: do we need to also free it from storage?
            }
            blk_delivery_waiting.erase(it);
        }
        return res;
    }

    promise_t HotStuffBase::async_fetch_blk(const uint256_t &blk_hash,
                                            const PeerId *replica,
                                            bool fetch_now) {
        if (storage->is_blk_fetched(blk_hash))
            return promise_t([this, &blk_hash](promise_t pm){
                pm.resolve(storage->find_blk(blk_hash));
            });
        auto it = blk_fetch_waiting.find(blk_hash);
        if (it == blk_fetch_waiting.end())
        {
#ifdef HOTSTUFF_BLK_PROFILE
            blk_profiler.rec_tx(blk_hash, false);
#endif
            it = blk_fetch_waiting.insert(
                    std::make_pair(
                            blk_hash,
                            BlockFetchContext(blk_hash, this))).first;
        }
        if (replica != nullptr)
            it->second.add_replica(*replica, fetch_now);
        return static_cast<promise_t &>(it->second);
    }

    promise_t HotStuffBase::async_deliver_blk(const uint256_t &blk_hash,
                                              const PeerId &replica) {
        if (storage->is_blk_delivered(blk_hash))
            return promise_t([this, &blk_hash](promise_t pm) {
                pm.resolve(storage->find_blk(blk_hash));
            });
        auto it = blk_delivery_waiting.find(blk_hash);
        if (it != blk_delivery_waiting.end())
            return static_cast<promise_t &>(it->second);
        BlockDeliveryContext pm{[](promise_t){}};
        it = blk_delivery_waiting.insert(std::make_pair(blk_hash, pm)).first;
        /* otherwise the on_deliver_batch will resolve */
        async_fetch_blk(blk_hash, &replica).then([this, replica](block_t blk) {
            /* qc_ref should be fetched */
            std::vector<promise_t> pms;
            const auto &qc = blk->get_qc();
            assert(qc);
            if (blk == get_genesis())
                pms.push_back(promise_t([](promise_t &pm){ pm.resolve(true); }));
            else
                pms.push_back(blk->verify(this, vpool));
            pms.push_back(async_fetch_blk(qc->get_obj_hash(), &replica));
            /* the parents should be delivered */
            for (const auto &phash: blk->get_parent_hashes())
                pms.push_back(async_deliver_blk(phash, replica));
            promise::all(pms).then([this, blk](const promise::values_t values) {

                HOTSTUFF_LOG_INFO("promise::any_cast<bool>(values[0]) is %d and this->on_deliver_blk(blk) is %d",
                                  promise::any_cast<bool>(values[0]), this->on_deliver_blk(blk));
                auto ret = promise::any_cast<bool>(values[0]) && this->on_deliver_blk(blk);
                if (!ret)
                    HOTSTUFF_LOG_WARN("verification failed during async delivery");
            });
        });
        return static_cast<promise_t &>(pm);
    }

    void HotStuffBase::propose_handler(MsgPropose &&msg, const Net::conn_t &conn) {
        const PeerId &peer = conn->get_peer_id();




        if (peer.is_null()) return;


        msg.postponed_parse(this);
        auto &prop = msg.proposal;
        block_t blk = prop.blk;


        if (prop.msg_type==9)
        {
            LOG_WARN("will return due to null block with msg type: %d", prop.msg_type);
            HOTSTUFF_LOG_INFO("informed join msg details are: cluster_number, node_id_in_cluster, original_idx are %d, %d, %d",
                              prop.cluster_number, prop.pre_amp_cluster_number,prop.other_cluster_block_height);


            cluster_map[prop.other_cluster_block_height] = prop.cluster_number;

            if (cluster_id == prop.cluster_number)
            {

                int i = prop.other_cluster_block_height;

                auto &addr = std::get<0>(all_replicas_h[i]);
                auto cert_hash = std::move(std::get<2>(all_replicas_h[i]));
                valid_tls_certs.insert(cert_hash);
                auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
                HotStuffCore::add_replica(i, peer, std::move(std::get<1>(all_replicas_h[i])));
                if (addr != listen_addr)
                {
                    peers.push_back(peer);
                    pn.add_peer(peer);
                    pn.set_peer_addr(peer, addr);
                    pn.conn_peer(peer);
                }
            }
            else{


                int i = prop.other_cluster_block_height;

                auto &addr = std::get<0>(all_replicas_h[i]);
                auto cert_hash = std::move(std::get<2>(all_replicas_h[i]));
                valid_tls_certs.insert(cert_hash);
                auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
                HotStuffCore::add_replica(i, peer, std::move(std::get<1>(all_replicas_h[i])));
                if (addr != listen_addr)
                {
                    other_peers.push_back(peer);
                    pn.add_peer(peer);
                    pn.set_peer_addr(peer, addr);
                    pn.conn_peer(peer);
                }

            }


            return;
        }



        if (!blk) {
            LOG_WARN("Returning due to null block with msg type: %d", prop.msg_type);
            return;
        }

        if ((cluster_id==prop.cluster_number) && (peer != get_config().get_peer_id(prop.proposer)) && (prop.msg_type<3))
        {
            LOG_WARN("invalid proposal from %d, prop.msg_type: %d", prop.proposer, prop.msg_type);
            return;
        }

//        LOG_WARN("2: proposal from %d, prop.msg_type: %d, cluster_id = %d, prop.cluster_number = %d, int(cluster_id==prop.cluster_number) = %d",
//                 prop.proposer, prop.msg_type, cluster_id, prop.cluster_number, int(cluster_id==prop.cluster_number));



        // receiving message of this type from new node

        if (prop.msg_type==6)
        {
            // sending tentative set to leader
            Proposal prop_same_cluster(id, blk, nullptr, cluster_id, prop.cluster_number, prop.other_cluster_block_height, 7);
            LOG_INFO("LatencyPlot: Received first message with new node info") ;

            do_broadcast_proposal(prop_same_cluster);
            return;
        }

        if (prop.msg_type==7)
        {
            LOG_INFO("LatencyPlot: Leader received with tentative sets from all peer nodes") ;
            // Adding to tentative set

            tentative_join_set[int(prop.cluster_number)] = prop.pre_amp_cluster_number;
            return;
        }


        if (prop.msg_type==1)
        {

            if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Received 1st MC message") ;

//            LOG_INFO("1st MC message: Reached here proposer %d, cluster_id = %d, prop.cluster number  = %d, prop.other_cluster_block_height=%d, height = %d, storage->get_blk_cache_size is %d, cluster_msg_count is %d, prop.other_cluster_block_height > cluster_msg_count = %d ",
//                     prop.proposer, cluster_id, prop.cluster_number, prop.other_cluster_block_height, blk->get_height(), int(storage->get_blk_cache_size()), cluster_msg_count, int(prop.other_cluster_block_height > cluster_msg_count) );

            Proposal prop_same_cluster(id, blk, nullptr, cluster_id, prop.cluster_number, prop.other_cluster_block_height, 2);

            auto finished_mc_cids_it = finished_mc_cids.find(int(prop.other_cluster_block_height));


            if (finished_mc_cids_it!=finished_mc_cids.end())
            {
                return;
            }

            do_broadcast_proposal(prop_same_cluster);

            if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Sent 2nd MC message") ;

            return;
        }


        if ((prop.msg_type==2))
        {

            if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Receieved 2nd MC message") ;


            LOG_INFO("2nd MC message: Reached here proposer %d, cluster number,  = %d, prop.pre_amp_cluster_number = %d, prop.other_cluster_block_height = %d, height = %d",
                     prop.proposer, prop.cluster_number, prop.pre_amp_cluster_number,prop.other_cluster_block_height, blk->get_height());

            auto finished_mc_cids_it = finished_mc_cids.find(int(prop.other_cluster_block_height));

            if (finished_mc_cids_it==finished_mc_cids.end())
            {

                auto it = cid_to_cluster_tracker_array.find(prop.other_cluster_block_height);
                if (it == cid_to_cluster_tracker_array.end())
                {
                    cluster_tracker_array = std::vector<int>();
                    for(int x;x < n_clusters; x++ ) cluster_tracker_array.push_back(0);
                    cluster_tracker_array[cluster_id] = 1;
                }
                else
                {
                    cluster_tracker_array = cid_to_cluster_tracker_array[prop.other_cluster_block_height];
                }


                cluster_tracker_array[prop.pre_amp_cluster_number] = cluster_tracker_array[prop.cluster_number] + 1;
                bool test_flag = true;

                for (int biter = 0; biter < n_clusters; biter++)
                {
                    if (cluster_tracker_array[biter] == 0)
                    {
                        test_flag = false;
                    }
                }

                LOG_INFO("test_flag = %d,  cluster_tracker_array[0], cluster_tracker_array[1], cluster_tracker_array[2] = %d, %d, %d",
                         test_flag, cluster_tracker_array[0], cluster_tracker_array[1],  cluster_tracker_array[2]);

                if (test_flag)
                {
                    cluster_msg_count = cluster_msg_count + 1;

                    LOG_INFO("Adding to finished_mc_cids for height: %d", int(prop.other_cluster_block_height));
                    finished_mc_cids.insert(int(prop.other_cluster_block_height));

                    if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: going to execute based on 2nd MC message") ;

                    on_receive_other_cluster_(int(prop.other_cluster_block_height));

                    for (int biter = 0; biter < n_clusters; biter++)
                    {
                        cluster_tracker_array[biter] = 0;
                    }
                    cluster_tracker_array[cluster_id] = 1;
                }

                cid_to_cluster_tracker_array[prop.other_cluster_block_height] = cluster_tracker_array;


            }

            return;
        }

        LOG_WARN("4: proposal from %d, prop.msg_type: %d, cluster_id = %d, prop.cluster_number = %d, int(cluster_id==prop.cluster_number) = %d",
                 prop.proposer, prop.msg_type, cluster_id, prop.cluster_number, int(cluster_id==prop.cluster_number));


        if (prop.msg_type==3)
        {

            LOG_WARN("rvc msg cmd_height = %d", int(prop.other_cluster_block_height));




            const uint256_t blk_hash = storage->find_blk_hash_for_cid(int(prop.other_cluster_block_height));

            LOG_WARN("blk_hash found %.10s", get_hex(blk_hash).c_str());

            block_t btemp = storage->find_blk(blk_hash);

            HOTSTUFF_LOG_INFO("proposal from: block_t found for timer_cid: %d with height:%d",
                              int(prop.other_cluster_block_height), btemp->get_height());



            Proposal prop_other_clusters(id, btemp, nullptr, cluster_id, cluster_id, int(prop.other_cluster_block_height), 1);


            bool leader_check = check_leader();
//            LOG_INFO("leader_check is %d", int(leader_check));

//            if (leader_check )
            {
                {
                    do_broadcast_proposal_other_clusters(prop_other_clusters);
                }
            }

            LOG_INFO("remote-view-change-trigger-response Final step");
            return;
        }


        // First join message for every round
        if (prop.msg_type==4)
        {
            if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Received initial join message") ;




            LOG_WARN("Received First join message: cmd_height = %d", int(prop.other_cluster_block_height));


            const uint256_t blk_hash = storage->find_blk_hash_for_cid(int(prop.other_cluster_block_height));

            LOG_WARN("blk_hash found %.10s", get_hex(blk_hash).c_str());

            block_t btemp = storage->find_blk(blk_hash);

            HOTSTUFF_LOG_INFO("proposal from: block_t found for timer_cid: %d with height:%d",
                              int(prop.other_cluster_block_height), btemp->get_height());

            bool j1 = storage->add_cid_join1(int(prop.other_cluster_block_height));

            auto it = finished_mc_cids.find(prop.other_cluster_block_height);


            bool finished_j1 = finished_echo_cids.find(int(prop.other_cluster_block_height))==finished_echo_cids.end();


            HOTSTUFF_LOG_INFO("j1 is %d, it!=finished_mc_cids.end() is %d, finished_j1 is %d"
                              ,int(j1), it!=finished_mc_cids.end(), finished_j1);
            if (j1 && finished_j1)
            {
                HOTSTUFF_LOG_INFO("quorum reached for first join message, with cmd_height = %d"
                                  , int(prop.other_cluster_block_height));
                Proposal prop_j2(id, btemp, nullptr, cluster_id, cluster_id, int(prop.other_cluster_block_height), 5);
                bool leader_check = check_leader();
//                if (leader_check)
                {
                    {
                        do_broadcast_proposal(prop_j2);
                    }
                }
                LOG_INFO("Sent 2nd join message for cmd_height = %d", int(prop.other_cluster_block_height));

                finished_echo_cids.insert(int(prop.other_cluster_block_height));
                if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Sent response to  initial join message") ;

            }




            return;
        }


        // Second join message for every round
        if (prop.msg_type==5)
        {


            if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Received second join message") ;


            LOG_WARN("Received Second join message: cmd_height = %d", int(prop.other_cluster_block_height));
            const uint256_t blk_hash = storage->find_blk_hash_for_cid(int(prop.other_cluster_block_height));

            LOG_WARN("blk_hash found %.10s", get_hex(blk_hash).c_str());

            block_t btemp = storage->find_blk(blk_hash);

            HOTSTUFF_LOG_INFO("proposal from: block_t found for timer_cid: %d with height:%d",
                              int(prop.other_cluster_block_height), btemp->get_height());


            bool j2 = storage->add_cid_join2(int(prop.other_cluster_block_height));
            auto it = finished_mc_cids.find(prop.other_cluster_block_height);

            bool finished_j2 = finished_ready_cids.find(int(prop.other_cluster_block_height))==finished_ready_cids.end();

            HOTSTUFF_LOG_INFO("j2 is %d, it!=finished_mc_cids.end() is %d, finished_j2 is %d"
                              ,int(j2), it!=finished_mc_cids.end(), int(finished_j2));

            if (j2 && finished_j2)
            {
                HOTSTUFF_LOG_INFO("quorum reached for second join message with cluster_id = %d, cmd_height = %d",
                                  cluster_id, int(prop.other_cluster_block_height));
                Proposal prop_other_clusters(id, btemp, nullptr, cluster_id, cluster_id, int(prop.other_cluster_block_height), 1);
                bool leader_check = check_leader();

//                if(leader_check)
                {
                    {
                        do_broadcast_proposal_other_clusters(prop_other_clusters);
                    }
                    LOG_INFO("Sent multicluster message for cmd_height = %d", int(prop.other_cluster_block_height));
                }

                finished_ready_cids.insert(int(prop.other_cluster_block_height));
                if (int(prop.other_cluster_block_height)==6000) LOG_INFO("LatencyPlot: Sent multicluster message") ;

            }
            return;
        }







        promise::all(std::vector<promise_t>{
                async_deliver_blk(blk->get_hash(), peer)
        }).then([this, prop = std::move(prop)]() {
            on_receive_proposal(prop);
        });
    }

    void HotStuffBase::vote_handler(MsgVote &&msg, const Net::conn_t &conn) {

        LOG_INFO("function vote_handler:START");
        const auto &peer = conn->get_peer_id();
        if (peer.is_null()) return;
        msg.postponed_parse(this);
        //auto &vote = msg.vote;
        RcObj<Vote> v(new Vote(std::move(msg.vote)));
        promise::all(std::vector<promise_t>{
                async_deliver_blk(v->blk_hash, peer),
                v->verify(vpool),
        }).then([this, v=std::move(v)](const promise::values_t values) {
            if (!promise::any_cast<bool>(values[1]))
                LOG_WARN("invalid vote from %d", v->voter);
            else
            {
                LOG_INFO("going into on_receive_vote");
                on_receive_vote(*v);
            }
        });

        LOG_INFO("function vote_handler:END");

    }

    void HotStuffBase::req_blk_handler(MsgReqBlock &&msg, const Net::conn_t &conn) {
        const PeerId replica = conn->get_peer_id();


        if (replica.is_null()) return;
        auto &blk_hashes = msg.blk_hashes;
        std::vector<promise_t> pms;
        for (const auto &h: blk_hashes)
            pms.push_back(async_fetch_blk(h, nullptr));
        promise::all(pms).then([replica, this](const promise::values_t values) {
            std::vector<block_t> blks;
            for (auto &v: values)
            {
                auto blk = promise::any_cast<block_t>(v);
                blks.push_back(blk);
            }
            pn.send_msg(MsgRespBlock(blks), replica);
        });
    }

    void HotStuffBase::resp_blk_handler(MsgRespBlock &&msg, const Net::conn_t &) {
        msg.postponed_parse(this);
        for (const auto &blk: msg.blks)
            if (blk) on_fetch_blk(blk);
    }

    bool HotStuffBase::conn_handler(const salticidae::ConnPool::conn_t &conn, bool connected) {
        if (connected)
        {
            if (!pn.enable_tls) return true;
            auto cert = conn->get_peer_cert();
            //SALTICIDAE_LOG_INFO("%s", salticidae::get_hash(cert->get_der()).to_hex().c_str());
            return valid_tls_certs.count(salticidae::get_hash(cert->get_der()));
        }
        return true;
    }

    void HotStuffBase::print_stat() const {
        LOG_INFO("===== begin stats =====");
        LOG_INFO("-------- queues -------");
        LOG_INFO("blk_fetch_waiting: %lu", blk_fetch_waiting.size());
        LOG_INFO("blk_delivery_waiting: %lu", blk_delivery_waiting.size());
        LOG_INFO("decision_waiting: %lu", decision_waiting.size());
        LOG_INFO("-------- misc ---------");
        LOG_INFO("fetched: %lu", fetched);
        LOG_INFO("delivered: %lu", delivered);
        LOG_INFO("cmd_cache: %lu", storage->get_cmd_cache_size());
        LOG_INFO("blk_cache: %lu", storage->get_blk_cache_size());
        LOG_INFO("------ misc (10s) -----");
        LOG_INFO("fetched: %lu", part_fetched);
        LOG_INFO("delivered: %lu", part_delivered);
        LOG_INFO("decided: %lu", part_decided);
        LOG_INFO("nreplicas: %lu", get_config().nreplicas);
        LOG_INFO("gened: %lu", part_gened);
        LOG_INFO("avg. parent_size: %.3f",
                 part_delivered ? part_parent_size / double(part_delivered) : 0);
        LOG_INFO("delivery time: %.3f avg, %.3f min, %.3f max",
                 part_delivered ? part_delivery_time / double(part_delivered) : 0,
                 part_delivery_time_min == double_inf ? 0 : part_delivery_time_min,
                 part_delivery_time_max);

        part_parent_size = 0;
        part_fetched = 0;
        part_delivered = 0;
        part_decided = 0;
        part_gened = 0;
        part_delivery_time = 0;
        part_delivery_time_min = double_inf;
        part_delivery_time_max = 0;
#ifdef HOTSTUFF_MSG_STAT
        LOG_INFO("--- replica msg. (10s) ---");
        size_t _nsent = 0;
        size_t _nrecv = 0;
        for (const auto &replica: peers)
        {
            auto conn = pn.get_peer_conn(replica);
            if (conn == nullptr) continue;
            size_t ns = conn->get_nsent();
            size_t nr = conn->get_nrecv();
            size_t nsb = conn->get_nsentb();
            size_t nrb = conn->get_nrecvb();
            conn->clear_msgstat();
            LOG_INFO("%s: %u(%u), %u(%u), %u",
                     get_hex10(replica).c_str(), ns, nsb, nr, nrb, part_fetched_replica[replica]);
            _nsent += ns;
            _nrecv += nr;
            part_fetched_replica[replica] = 0;
        }
        nsent += _nsent;
        nrecv += _nrecv;
        LOG_INFO("sent: %lu", _nsent);
        LOG_INFO("recv: %lu", _nrecv);
        LOG_INFO("--- replica msg. total ---");
        LOG_INFO("sent: %lu", nsent);
        LOG_INFO("recv: %lu", nrecv);
#endif
        LOG_INFO("====== end stats ======");
    }

    HotStuffBase::HotStuffBase(uint32_t blk_size,
                               ReplicaID rid,
                               int cluster_id,
                               int n_clusters,
                               privkey_bt &&priv_key,
                               NetAddr listen_addr,
                               pacemaker_bt pmaker,
                               EventContext ec,
                               size_t nworker,
                               const Net::Config &netconfig):
            HotStuffCore(rid, cluster_id, std::move(priv_key)),
            listen_addr(listen_addr),
            blk_size(blk_size),
            cluster_id(cluster_id),
            n_clusters(n_clusters),
            ec(ec),
            tcall(ec),
            vpool(ec, nworker),
            pn(ec, netconfig),
            pmaker(std::move(pmaker)),

            fetched(0), delivered(0),
            nsent(0), nrecv(0),
            part_parent_size(0),
            part_fetched(0),
            part_delivered(0),
            part_decided(0),
            part_gened(0),
            part_delivery_time(0),
            part_delivery_time_min(double_inf),
            part_delivery_time_max(0),
            cluster_tracker_array()
    {


//        std::vector<int> cluster_tracker_array(n_clusters, 0);
//        cluster_tracker_array[cluster_id] = 1;

        /* register the handlers for msg from replicas */
        pn.reg_handler(salticidae::generic_bind(&HotStuffBase::propose_handler, this, _1, _2));
        pn.reg_handler(salticidae::generic_bind(&HotStuffBase::vote_handler, this, _1, _2));
        pn.reg_handler(salticidae::generic_bind(&HotStuffBase::req_blk_handler, this, _1, _2));
        pn.reg_handler(salticidae::generic_bind(&HotStuffBase::resp_blk_handler, this, _1, _2));
        pn.reg_conn_handler(salticidae::generic_bind(&HotStuffBase::conn_handler, this, _1, _2));
        pn.reg_error_handler([](const std::exception_ptr _err, bool fatal, int32_t async_id) {
            try {
                std::rethrow_exception(_err);
            } catch (const std::exception &err) {
                HOTSTUFF_LOG_WARN("network async error: %s\n", err.what());
            }
        });

        LOG_INFO("--- rid: %d",int(rid));

        pn.start();
        pn.listen(listen_addr);
    }

    void HotStuffBase::do_broadcast_proposal(const Proposal &prop) {

        HOTSTUFF_LOG_INFO("broadcasting to peers with size is %d\n", peers.size());
        pn.multicast_msg(MsgPropose(prop), peers);

    }


    void HotStuffBase::join_nodes() {
        HOTSTUFF_LOG_INFO("Tentative set size %d\n", tentative_join_set.size());

        for (auto& element : tentative_join_set) {
            int i = element.second;
            int cls_no = element.first;
            if (cls_no==cluster_id)
            {
                auto &addr = std::get<0>(all_replicas_h[i]);
                auto cert_hash = std::move(std::get<2>(all_replicas_h[i]));
                valid_tls_certs.insert(cert_hash);
                auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
                HotStuffCore::add_replica(i, peer, std::move(std::get<1>(all_replicas_h[i])));
                if (addr != listen_addr)
                {
                    peers.push_back(peer);
                    pn.add_peer(peer);
                    pn.set_peer_addr(peer, addr);
                    pn.conn_peer(peer);
                }
            }


        }




        tentative_join_set = std::unordered_map<int,int>();
    }


    void HotStuffBase::do_broadcast_proposal_to_leader(const Proposal &prop) {
        pn.send_msg(MsgPropose(prop), peers[pmaker->get_proposer()]);
    }


    void HotStuffBase::do_inform_reconfig(int cluster_no, int node_id, int orig_node_id)
    {
        HOTSTUFF_LOG_INFO("do_inform_reconfig with cluster_no : %d, node_id: %d", cluster_no, node_id);


        block_t blk = new Block(true, 1);
        Proposal prop_same_cluster(id, blk, nullptr, cluster_no, node_id, orig_node_id, 9);

        do_broadcast_proposal(prop_same_cluster);
        do_broadcast_proposal_other_clusters(prop_same_cluster);

    }



    void HotStuffBase::do_broadcast_proposal_other_clusters(const Proposal &prop) {


        if (other_peers_f_plus_one_init == 0)
        {


            std::unordered_map<int, int> myMap;

            for (const auto& pair : cluster_map) {
                int node_id = pair.first;
                int other_cluster_id = pair.second;

                if(cluster_id!=other_cluster_id)
                {
                    myMap[other_cluster_id]++;
                }

            }



            int nodes_in_curr_cluster;
            int f_curr;
            int curr_iter_node_sum = 0;

            for (int xter = 0; xter < n_clusters; xter++)
            {
                if(cluster_id!=xter)
                {
                    nodes_in_curr_cluster = myMap[xter];
                    f_curr = (nodes_in_curr_cluster-1)/3;

                    for(int temp_iter = 0; temp_iter < f_curr +1; temp_iter++)
                    {
                        HOTSTUFF_LOG_INFO("added node id (to f+1 peers) : %d", curr_iter_node_sum + temp_iter);

                        other_peers_f_plus_one.push_back(other_peers[ curr_iter_node_sum + temp_iter]);
                    }

                    curr_iter_node_sum = curr_iter_node_sum + nodes_in_curr_cluster;

                }

            }
            other_peers_f_plus_one_init = 1;
        }


//        std::vector<PeerId> other_peers_f_plus_one = std::vector<PeerId>(
//                other_peers.begin(), other_peers.begin() + get_config().nreplicas
//        -get_config().nmajority+1);

        HOTSTUFF_LOG_INFO("Broadcasting to other clusters with clustermap[0] = %d, %d, %d, %d, other_peers.size = %d"
                          , cluster_map[0],
                          cluster_map[4],cluster_map[8], cluster_map[9], other_peers.size());


//        HOTSTUFF_LOG_INFO("sending other clusters message with peers.size is %d, nreplicas, nmajority, other_peers_f_plus_one.size() is %d, %d, %d\n"
//                          , peers.size(), get_config().nreplicas, get_config().nmajority, other_peers_f_plus_one.size());

        pn.multicast_msg(MsgPropose(prop), other_peers_f_plus_one);
//        pn.multicast_msg(MsgPropose(prop), other_peers);

    }


    void HotStuffBase::start_remote_view_change_timer(int timer_cid, const block_t btemp)
    {

//        storage->add_cid_join(btemp);


//        remote_view_change_timer
        cid_to_remote_view_change_timer[timer_cid] = TimerEvent(ec, [this, timer_cid, btemp](TimerEvent &)
        {

            HOTSTUFF_LOG_INFO("timer for remote-view-change triggerred with cid:%d", timer_cid);

//            block_t btemp = storage->find_blk_cid(timer_cid);

            HOTSTUFF_LOG_INFO("block_t found for timer_cid: %d with height:%d",
                              timer_cid, btemp->get_height());



            Proposal prop_other_clusters(id, btemp, nullptr, cluster_id, cluster_id, btemp->get_height(), 3);


            bool leader_check = check_leader();



            auto it = finished_mc_cids.find(timer_cid);


            LOG_INFO("leader_check is %d, with rvct_timeout = %d, "
                     "it==finished_mc_cids.end() = %d",
                     int(leader_check), rvct_timeout, int(it==finished_mc_cids.end()));

//            if (leader_check && it==finished_mc_cids.end() && (timer_cid-last_rvc)>0 )
            if (leader_check && it==finished_mc_cids.end() && (timer_cid-last_rvc)>0 )
            {
                {
                    do_broadcast_proposal_other_clusters(prop_other_clusters);
                }
                last_rvc = timer_cid;
            }
//            rvct_timeout = rvct_timeout * 2;


//            reset_remote_view_change_timer();

        });
//        remote_view_change_timer.add(rvct_timeout);
        cid_to_remote_view_change_timer[timer_cid].add(rvct_timeout);


    }

    bool HotStuffBase::did_receive_mc(int blk_height)
    {
        auto it = finished_mc_cids.find(blk_height);
        return (it!=finished_mc_cids.end());
    }


    bool HotStuffBase::did_update(int blk_height)
    {
        auto it = finished_update_cids.find(blk_height);
        return (it!=finished_update_cids.end());
    }


    void HotStuffBase::update_finished_update_cids(int blk_height)
    {
        finished_update_cids.insert(blk_height);
    }





    void HotStuffBase::reset_remote_view_change_timer(int timer_blk_height) {
        HOTSTUFF_LOG_INFO("Deleting timer remote-view-change for blk_height = %d", timer_blk_height);

        cid_to_remote_view_change_timer[timer_blk_height].del();
        cid_to_remote_view_change_timer[timer_blk_height].clear();

        auto it = cid_to_remote_view_change_timer.find(timer_blk_height);
        if (it != cid_to_remote_view_change_timer.end())
        {
            cid_to_remote_view_change_timer.erase(it);
        }

    }


    void HotStuffBase::decide_after_mc(int blk_height) {
        HOTSTUFF_LOG_INFO("decide after mc message START for blk height:%d", blk_height);


            const uint256_t blk_hash = storage->find_blk_hash_for_cid(blk_height);

            static const uint8_t negativeOne[] = { 0xFF };

            if (blk_hash!= negativeOne)
            {

                block_t btemp = storage->find_blk(blk_hash);





                if (did_update(blk_height))
                {
                    for (size_t i = 0; i < (btemp->get_cmds()).size(); i++) {
                        LOG_INFO("Since already updated, do_decide for height:%d, btemp = %s, blk_hash = %d",
                                 int(btemp->get_height()), std::string(*btemp).c_str(), blk_hash);

//                        if ((btemp->get_keys()).empty())
                        {
                            LOG_INFO("btemp keys not found");
                            do_decide(Finality(id, 1, i, btemp->get_height(),
                                               (btemp->get_cmds())[i],
                                               btemp->get_hash()),  (btemp->get_keys())[i], (btemp->get_vals())[i]);

                        }
//                        else
//                        {
//                            LOG_INFO("btemp keys found");
//
//                            do_decide(Finality(id, 1, i, btemp->get_height(),
//                                               (btemp->get_cmds())[i],
//                                               btemp->get_hash()),  (btemp->get_keys())[i], (btemp->get_vals())[i]);
//
//                        }


                    }

                    reset_remote_view_change_timer(blk_height);


                }

                if (int(blk_height)==6000) LOG_INFO("LatencyPlot: Finished execution") ;



            }



    }

    void HotStuffBase::store_in_map_for_mc(const block_t &blk)
    {
        HOTSTUFF_LOG_INFO("storing blk in  %s",std::string(*blk).c_str());

        storage->add_cid_blkhash(blk);
    }





    void HotStuffBase::do_vote(ReplicaID last_proposer, const Vote &vote) {
        pmaker->beat_resp(last_proposer)
                .then([this, vote](ReplicaID proposer) {

                    int self_id = get_id();

                    HOTSTUFF_LOG_INFO("Tejas: Going to vote with proposer: %d, id: %d", int(proposer), self_id);

                    if (proposer == get_id())
                    {
                        //throw HotStuffError("unreachable line");
                        HOTSTUFF_LOG_INFO("Tejas: self receiving vote");
                        on_receive_vote(vote);
                    }
                    else
                    {
                        HOTSTUFF_LOG_INFO("Tejas: sending vote to other nodes of same cluster");

                        pn.send_msg(MsgVote(vote), get_config().get_peer_id(proposer));
                    }
                });
    }

    void HotStuffBase::do_consensus(const block_t &blk) {
        pmaker->on_consensus(blk);
    }


    bool HotStuffBase::check_leader() {
        return (get_id()==pmaker->get_proposer());
    }


    void HotStuffBase::do_decide_read_only(Finality &&fin, int key, int value) {
        HOTSTUFF_LOG_INFO("Tejas: do_decide() START");

        std::string status = "";



        part_decided++;



        state_machine_execute(fin);



//        try
//        {
//            bool cond = key_val_store.find(fin.cmd_hash) != key_val_store.end();
//
//
//            if (cond)
            {
//                std::pair key_val = key_val_store.at(fin.cmd_hash);

//                status =  db_read(key_val.first);
                status =  db_read(key);

                HOTSTUFF_LOG_INFO("do_decide_read_only: key found, status is %s", status);
            }
//            else
//            {
//                HOTSTUFF_LOG_INFO("do_decide_read_only: key not found", status);
//
//            }



//        }








        auto it = decision_waiting.find(fin.cmd_hash);
        if (it != decision_waiting.end())
        {
            it->second(std::move(fin));
            decision_waiting.erase(it);
        }




//    HOTSTUFF_LOG_INFO("Tejas: do_decide() END");

    }



    void HotStuffBase::do_decide(Finality &&fin, int key, int val) {
    HOTSTUFF_LOG_INFO("Tejas: do_decide() START");

        std::string status = "";


        part_decided++;



        state_machine_execute(fin);








//        try
        {


//            bool cond = key_val_store.find(fin.cmd_hash) != key_val_store.end();

//            if (cond)
            {
//                std::pair key_val = key_val_store.at(fin.cmd_hash);
//                status =  db_write(key_val.first, key_val.second);
                status =  db_write(key, val);

                HOTSTUFF_LOG_INFO("do_decide: write completed, status is %s", status.c_str());


            }
//            else
//            {
//                HOTSTUFF_LOG_INFO("do_decide: write incomplete");
//
//            }




        }




        auto it = decision_waiting.find(fin.cmd_hash);
        if (it != decision_waiting.end())
        {
            it->second(std::move(fin));
            decision_waiting.erase(it);
        }
//    HOTSTUFF_LOG_INFO("Tejas: do_decide() END");

    }

    HotStuffBase::~HotStuffBase() {}

    void HotStuffBase::start(
            std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&replicas,
            bool ec_loop) {
        for (size_t i = 0; i < replicas.size(); i++)
        {
            auto &addr = std::get<0>(replicas[i]);
            auto cert_hash = std::move(std::get<2>(replicas[i]));
            valid_tls_certs.insert(cert_hash);
            auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
            HotStuffCore::add_replica(i, peer, std::move(std::get<1>(replicas[i])));
            if (addr != listen_addr)
            {
                peers.push_back(peer);
                pn.add_peer(peer);
                pn.set_peer_addr(peer, addr);
                pn.conn_peer(peer);

            }
        }

        /* ((n - 1) + 1 - 1) / 3 */
        uint32_t nfaulty = peers.size() / 3;
        if (nfaulty == 0)
            LOG_WARN("too few replicas in the system to tolerate any failure");
        on_init(nfaulty);
        pmaker->init(this);

        if (ec_loop)
            ec.dispatch();
//
//        cmd_pending.reg_handler(ec, [this](cmd_queue_t &q) {
//            std::pair<uint256_t, commit_cb_t> e;
//            while (q.try_dequeue(e))
//            {
//                ReplicaID proposer = pmaker->get_proposer();
//
//                LOG_INFO("Leader is %d", (int)proposer);
//
//                const auto &cmd_hash = e.first;
//                auto it = decision_waiting.find(cmd_hash);
//                if (it == decision_waiting.end())
//                    it = decision_waiting.insert(std::make_pair(cmd_hash, e.second)).first;
//                else
//                    e.second(Finality(id, 0, 0, 0, cmd_hash, uint256_t()));
//
//
//                if (proposer != get_id()) continue;
//                cmd_pending_buffer.push(cmd_hash);
//                if (cmd_pending_buffer.size() >= blk_size)
//                {
//                    std::vector<uint256_t> cmds;
//                    for (uint32_t i = 0; i < blk_size; i++)
//                    {
//                        cmds.push_back(cmd_pending_buffer.front());
//                        cmd_pending_buffer.pop();
//                    }
//                    pmaker->beat().then([this, cmds = std::move(cmds)](ReplicaID proposer) {
//                        if (proposer == get_id())
//                            on_propose(cmds, pmaker->get_parents());
//                    });
//                    return true;
//                }
//            }
//            return false;
//        });
    }







    void HotStuffBase::start_mc(
            std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&replicas,
            std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&all_replicas,
            std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&other_replicas,
            std::unordered_map<int, int> cluster_map_input, int join_node_cluster,
            int orig_idx, bool ec_loop) {



        LOG_INFO("replicas sizes are %lu, %lu, %lu", replicas.size(), all_replicas.size(), other_replicas.size());
        LOG_INFO(" saved replicas sizes are %lu, cluster_map_input[8] is %d", all_replicas_h.size(), cluster_map_input[8]);

        cluster_map = cluster_map_input;

        LOG_INFO(" saved cluster_map[8] and join_node_cluster are  %d and %d and orig_idx is %d",
                 cluster_map[8], join_node_cluster, orig_idx);




        for (size_t i = 0; i < replicas.size(); i++)
        {

            auto &addr = std::get<0>(replicas[i]);
            auto cert_hash = std::move(std::get<2>(replicas[i]));
            valid_tls_certs.insert(cert_hash);
            auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
            HotStuffCore::add_replica(i, peer, std::move(std::get<1>(replicas[i])));
            if (addr != listen_addr)
            {
                peers.push_back(peer);
                pn.add_peer(peer);
                pn.set_peer_addr(peer, addr);
                pn.conn_peer(peer);
            }


        }


        for (size_t i = 0; i < other_replicas.size(); i++)
        {
            auto &addr = std::get<0>(other_replicas[i]);
            auto cert_hash = std::move(std::get<2>(other_replicas[i]));
            valid_tls_certs.insert(cert_hash);
            auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
//            HotStuffCore::add_replica(i, peer, std::move(std::get<1>(other_replicas[i])));
            if (addr != listen_addr)
            {
                other_peers.push_back(peer);
                pn.add_peer(peer);
                pn.set_peer_addr(peer, addr);
                pn.conn_peer(peer);
            }
        }



        if (join_node_cluster > 0)
        {
            do_inform_reconfig(join_node_cluster,get_id(), orig_idx);
        }


        /* ((n - 1) + 1 - 1) / 3 */
        uint32_t nfaulty = peers.size() / 3;

        LOG_INFO("nfaulty, peers.size() are %d, %d \n", int(nfaulty), peers.size());
        if (nfaulty == 0)
            LOG_WARN("too few replicas in the system to tolerate any failure");



        on_init(nfaulty);


        pmaker->init(this);
        if (ec_loop)
            ec.dispatch();

        cmd_pending.reg_handler(ec, [this](cmd_queue_t &q) {
            std::pair<std::pair<uint256_t, std::pair<int, int>>, commit_cb_t> e;
            while (q.try_dequeue(e))
            {
                ReplicaID proposer = pmaker->get_proposer();

                LOG_INFO("Leader is %d", (int)proposer);

                const auto &cmd_hash = e.first.first;
                auto it = decision_waiting.find(cmd_hash);



//
//                if (it == decision_waiting.end())
//                    it = decision_waiting.insert(std::make_pair(cmd_hash, e.second)).first;
//                else
//                    e.second(Finality(id, 0, 0, 0, cmd_hash, uint256_t()));



                if (it == decision_waiting.end())
                {
                    HOTSTUFF_LOG_INFO("decision_waiting, cmd_hash absent (this mostly happens)");

                    it = decision_waiting.insert(std::make_pair(cmd_hash, e.second)).first;

                }
                else
                {
                    HOTSTUFF_LOG_INFO("decision_waiting, cmd_hash present");


                    e.second(Finality(id, 0, 0, 0, cmd_hash, uint256_t()) );

                }

                HOTSTUFF_LOG_INFO("key, val is %d, %d", e.first.second.first, e.first.second.second);





//                if (!cond)
//                {
//                    HOTSTUFF_LOG_INFO("key Not FOUND");
//                    do_decide(Finality(id, 1, 0, 0, cmd_hash, uint256_t()) );
//                    continue;
////                    throw std::invalid_argument("Key Not Found,  during executing, did it print? ");
//                }

                int key = e.first.second.first;
                int val = e.first.second.second;

//                HOTSTUFF_LOG_INFO("before db entry");
//
////                key_val_store[cmd_hash] = std::pair(key,val);
//                HOTSTUFF_LOG_INFO("Added to DB successfully");

                
                if (key%100>15)
                {
                    do_decide_read_only(Finality(id, 1, 0, 0, cmd_hash, uint256_t()), key, val );
                }

                if (proposer != get_id()) continue;

                if (key%100<=15)
                {
                    cmd_pending_buffer.push(std::make_tuple(cmd_hash, key, val));
                }


                if (cmd_pending_buffer.size() >= blk_size)
                {
                    HOTSTUFF_LOG_INFO("Block size worth of pending cmds found");

                    std::vector<uint256_t> cmds;
                    std::vector<int> keys;
                    std::vector<int> vals;

                    for (uint32_t i = 0; i < blk_size; i++)
                    {
                        cmds.push_back(std::get<0>(cmd_pending_buffer.front()));
                        keys.push_back(std::get<1>(cmd_pending_buffer.front()));
                        vals.push_back(std::get<2>(cmd_pending_buffer.front()));


                        cmd_pending_buffer.pop();
                    }
                    pmaker->beat().then([this, cmds = std::move(cmds), keys = std::move(keys), vals = std::move(vals)  ](ReplicaID proposer) {
                        if (proposer == get_id())
                        {

                            on_propose(cmds, keys, vals, pmaker->get_parents());

                        }
                    });
                    return true;
                }





            }
            return false;
        });
    }


}
