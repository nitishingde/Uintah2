#ifndef UINTAH2_COMM_H
#define UINTAH2_COMM_H


#include <sstream>
#include "sigslot/signal.hpp"

#define ANY_SRC_NODE -1

namespace comm {
    /**
     * TODO: All thread safe I think, need to confirm
     * @return
     */
    bool isMpiRootPid();
    int getMpiNodeId();
    int getMpiNumNodes();

    /**
     * RAII
     * Works similar to std::lock_guard
     * Create this object only once.
     * It's lifecycle usually should be the entirety of the process execution.
     * Preferably make a MPI_GlobalLockGuard stack object at the beginning of the main function
     */
    class MPI_GlobalLockGuard {
    public:
        MPI_GlobalLockGuard(int32_t *argc, char ***argv);
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
        uint32_t srcId;

        explicit CommPacket(uint32_t id_, std::string serializedData_, uint32_t srcId_)
                : id(id_), serializedData(std::move(serializedData_)), srcId(srcId_) {}

        CommPacket(CommPacket &&other) noexcept {
            if(this == &other) return;
            this->id = other.id;
            this->serializedData = std::move(other.serializedData);
            this->srcId = other.srcId;
        }

        CommPacket& operator=(CommPacket &&other) noexcept {
            if(this == &other) return *this;
            this->id = other.id;
            this->serializedData = std::move(other.serializedData);
            this->srcId = other.srcId;

            return *this;
        }
    };

    /**
     * A way to send and receive data over network of clusters
     * Thread safe routines
     */
    class Communicator {
    public:
        static sigslot::signal<std::shared_ptr<comm::CommPacket>> signal;
        static void sendMessage(uint32_t id, std::ostringstream message, int32_t destId);
    };
}


#endif //UINTAH2_COMM_H
