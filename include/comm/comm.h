#ifndef UINTAH2_COMM_H
#define UINTAH2_COMM_H


#include <functional>
#include <memory>
#include <mutex>
#include <sstream>

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
     * Simple/dumb signal slot implementation intended just for Comm
     * Only 1 slot can be connected/registered
     * Connecting multiple slots throws an error
     * Thread safe routines
     *
     * FIXME: use unique_ptr instead of shared_ptr?
    */
    class Signal {
    private:
        using SignalType = std::shared_ptr<comm::CommPacket>;
        std::function<void(SignalType)> slot_;
        std::mutex mutex_ {};

    public:
        Signal() = default;

        ~Signal() {
            disconnect();
        }

        void connect(std::function<void(SignalType)> slot) {
            std::lock_guard lockGuard(mutex_);
            if(slot_ != nullptr) {
                throw std::runtime_error("Slot is already occupied.");
            }
            slot_ = std::move(slot);
        }

        template<class T>
        void connect(void(T::*memberFunction)(SignalType), T *pObject) {
            connect(std::bind(memberFunction, pObject, std::placeholders::_1));
        }

        void disconnect() {
            std::lock_guard lockGuard(mutex_);
            slot_ = nullptr;
        }

        void emit(SignalType signal) {
            std::lock_guard lockGuard(mutex_);
            slot_(std::move(signal));
        }

        bool empty() {
            std::lock_guard lockGuard(mutex_);
            return slot_ == nullptr;
        }
    };

    /**
     * A way to send and receive data over network of clusters
     *
     * Need to register a callback (signal) to get notified on incoming comm messages.
     * Also has a static member function to send comm message to a single node.
     * Thread safe routines
     *
     * TODO: broadcast, reduction
     */
    class Communicator {
    public:
        static comm::Signal signal;
        static void sendMessage(uint32_t id, std::ostringstream message, int32_t destId);
    };
}


#endif //UINTAH2_COMM_H
