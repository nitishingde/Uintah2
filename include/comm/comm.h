#ifndef UINTAH2_COMM_H
#define UINTAH2_COMM_H


#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>

#define ANY_SRC_NODE -1

namespace comm_ {
    /**
     * RAII
     * Works similar to std::lock_guard
     * Create this object only once.
     * It's lifecycle usually should be the entirety of the process execution.
     * Preferably make a MPI_GlobalLockGuard stack object at the beginning of the main function
     */
    class MPI_GlobalLockGuard {
    public:
        MPI_GlobalLockGuard(
                int32_t *argc,
                char ***argv,
                std::chrono::milliseconds timeSlice = std::chrono::milliseconds(16)
        );
        ~MPI_GlobalLockGuard();
    };

    /**
    * Comm Packet is similar to IPv4 Packet Header
    *
    * Similar to a IPv4 packet header, CommPacket has header fields like id (packet identifier) and srcId (source node)
    * and a data field called serializedData.
    */
    struct CommPacket {
        uint32_t id;
        std::string serializedData;
        uint32_t otherNode;

        explicit CommPacket(uint32_t id_, std::string &&serializedData_, uint32_t otherNode_)
                : id(id_), serializedData(serializedData_), otherNode(otherNode_) {}

        CommPacket(CommPacket &&other) noexcept {
            if(this == &other) return;
            this->id = other.id;
            this->serializedData = std::move(other.serializedData);
            this->otherNode = other.otherNode;
        }

        CommPacket& operator=(CommPacket &&other) noexcept {
            if(this == &other) return *this;
            this->id = other.id;
            this->serializedData = std::move(other.serializedData);
            this->otherNode = other.otherNode;

            return *this;
        }
    };
}

namespace comm {
    using MPI_GlobalLockGuard = comm_::MPI_GlobalLockGuard;
    using CommPacket = comm_::CommPacket;
    using SignalType = std::shared_ptr<comm::CommPacket>;

    /**
     * TODO: check if thread safe routines
     * TODO: broadcast, reduction
     */
    int getMpiNodeId();
    int getMpiNumNodes();
    bool isMpiRootPid();
    void sendMessage(uint32_t id, std::string &&message, int32_t destId);
    void connectReceiver(std::function<void(SignalType)> slot);
    template<class T>
    void connectReceiver(void(T::*memberFunction)(SignalType), T *pObject) {
        connectReceiver(std::bind(memberFunction, pObject, std::placeholders::_1));
    }
}


#endif //UINTAH2_COMM_H
