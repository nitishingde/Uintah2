#ifndef UINTAH2_COMM_H
#define UINTAH2_COMM_H


#include <sstream>

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
     * An abstract class for serialising/deserialising objects to send using comm::Communicator
     * Do we need it right now? Not really.
     * Future purpose: A way to declare global variables (i.e. scoped over the cluster)
     */
    class VarLabel {
    public:
        std::string name_;
        explicit VarLabel(std::string name);
        ~VarLabel() = default;
        virtual const std::ostream& serialize(std::ostream &oss) const;
    };

    /**
     * A way to send and receive data over network of clusters
     * Thread safe routines
     */
    class Communicator {
    public:
        struct RecvData {
            uint32_t id;
            std::string serializedData;
            uint32_t srcId;
            explicit RecvData(uint32_t id, std::string serializedData, uint32_t srcId)
                    : id(id), serializedData(serializedData), srcId(srcId) {}

            RecvData(RecvData &&other) noexcept {
                if(this == &other) return;
                this->id = other.id;
                this->serializedData = std::move(other.serializedData);
                this->srcId = other.srcId;
            }

            RecvData& operator=(RecvData &&other) noexcept {
                if(this == &other) return *this;
                this->id = other.id;
                this->serializedData = std::move(other.serializedData);
                this->srcId = other.srcId;

                return *this;
            }
        };

        static void sendMessage(uint32_t id, std::ostringstream &message, int32_t destId);
        static comm::Communicator::RecvData recvMessage();
        static bool hasMessage();
    };

    struct RecvData {

    };
}


#endif //UINTAH2_COMM_H
