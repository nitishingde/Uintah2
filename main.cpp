//
// Created by ngs on 2/16/22.
//

#include <stdlib.h>

#include "comm/comm.h"
#include <chrono>
#include <thread>

class CommData: public comm::VarLabel {
public:
    int32_t data;

    explicit CommData(std::string name, int32_t data) : comm::VarLabel(std::move(name)), data(data) {}
    std::ostream &serialize(std::ostream &ostream) const override {
        comm::VarLabel::serialize(ostream);
        ostream << data;
        return ostream;
    }

    static CommData deserialize(std::istringstream istream) {
        std::string name;
        int32_t data;
        istream >> name >> data;
        return CommData(name, data);
    };
};

/**
 * Ring communication example
 * Process0 sends data to Process1
 * Process1 sends data to Process2
 * ...
 * ProcessN sends data to Process0
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[]) {
    comm::MPI_GlobalLockGuard globalLockGuard(&argc, &argv);

    auto nodeId = comm::getMpiNodeId();
    auto numNodes = comm::getMpiNumNodes();

    int32_t srcId = (numNodes+nodeId-1)%numNodes;
    int32_t destId = (nodeId+1)%numNodes;

    auto send = CommData("nodeId" + std::to_string(nodeId), nodeId);
    auto oss = std::ostringstream();
    send.serialize(oss);
    comm::Communicator::sendMessage(oss, destId);

    auto recvVarName = "nodeId" + std::to_string(srcId);
    while(!comm::Communicator::hasMessage(recvVarName, srcId)) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
    }
    auto recv = CommData::deserialize(comm::Communicator::recvMessage(recvVarName, srcId));
    printf("[Process %d] data = %d\n", nodeId, recv.data);

    return EXIT_SUCCESS;
}
