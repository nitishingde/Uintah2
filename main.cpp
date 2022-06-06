//
// Created by ngs on 2/16/22.
//

#include "comm/comm.h"
#include <atomic>
#include <chrono>
#include <thread>

class Serialization {
private:
public:
    Serialization() = default;
    virtual ~Serialization() = default;
    [[nodiscard]] virtual std::string serialize() const = 0;
    virtual void deserialize(std::istream&&) = 0;
};

class CommData: public Serialization {
public:
    int32_t data;
    std::string name;

    explicit CommData(std::string name = "", int32_t data = -1) : name(std::move(name)), data(data) {}

    [[nodiscard]] std::string serialize() const override {
        std::ostringstream oss;
        oss << name << ' ' << data;
        return oss.str();
    }

    void deserialize(std::istream &&istream) override {
        istream >> name >> data;
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
    using namespace std::chrono_literals;
    comm::MPI_GlobalLockGuard globalLockGuard(&argc, &argv, 10ms);

    auto nodeId = comm::getMpiNodeId();
    auto numNodes = comm::getMpiNumNodes();

//    int32_t srcId = (numNodes+nodeId-1)%numNodes;
    int32_t destId = (nodeId+1)%numNodes;

    std::atomic_bool canExit = false;
    comm::connectReceiver([&canExit, &nodeId](const std::shared_ptr<comm::CommPacket>& commPacket) {
        auto data = std::make_shared<CommData>();
        data->deserialize(std::istringstream(commPacket->serializedData));
        printf("[Process %d] receiving {data = %d, name = %s} from srcNode %d\n", nodeId, data->data, data->name.c_str(), commPacket->otherNode);
        std::atomic_store(&canExit, true);
    });

    auto send = CommData("nodeId" + std::to_string(nodeId), nodeId);
    comm::sendMessage(typeid(CommData).hash_code(),  send.serialize(), destId);
    printf("[Process %d] %d --> %d\n", nodeId, nodeId, destId);

    while(!std::atomic_load(&canExit)) {}

    return EXIT_SUCCESS;
}
