//
// Created by ngs on 2/16/22.
//
#include "comm/comm.h"
#include <atomic>
#include <list>
#include <mpi.h>
#include <mutex>
#include <queue>
#include <thread>


bool mpiGlobalLockGuardInitialized = false;
int32_t mpiNodeId = -1;
int32_t mpiNumNodes = -1;

namespace comm {
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
        std::function<void(SignalType)> slot_;
        std::mutex mutex_ {};

    public:
        Signal() = default;

        ~Signal() {
            disconnect();
        }

        void connect(std::function<void(SignalType)> slot) {
            std::lock_guard lockGuard(mutex_);
//            FIXME?
//            if(slot_ != nullptr) {
//                throw std::runtime_error("[comm::Signal] slot is already occupied!");
//            }
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
            if(slot_ == nullptr) {
                throw std::runtime_error("[comm::Signal] slot is empty!");
            }
            slot_(std::move(signal));
        }

        bool empty() {
            std::lock_guard lockGuard(mutex_);
            return slot_ == nullptr;
        }
    };
    static comm::Signal signal {};

    struct MetaDatum {
    private:
        uint32_t metaData_[2] = {0};
    public:
        MetaDatum() = default;
        void setBufferSize(uint32_t size) { metaData_[0] = size; }
        uint32_t getBufferSize() { return metaData_[0]; }
        void setTypeId(uint32_t typeId) { metaData_[1] = typeId; }
        uint32_t getTypeId() { return metaData_[1]; }
        void reset() { metaData_[0] = 0; metaData_[1] = 0; }
        uint32_t* data() { return metaData_; }
    };

    /**
     * Handles all the sends/recvs logic
     * A daemon thread is spawned to run in the background
     * Each cycle/epoch it
     *  1. syncs the metadata for sends/recvs (blocking)
     *  2. then issues the Isends/Irecvs (non blocking)
     *  3. cleanup after sending/receiving messages (non blocking)
     *
     * Singleton class
     * No need to expose to application
     */
    class DataWarehouse {
    private:
        struct CommPacketMPIWrapper {
            std::shared_ptr<comm::CommPacket> commPacket;
            MPI_Request request;

            explicit CommPacketMPIWrapper(uint32_t id, std::string buffer, int32_t otherNode, MPI_Request request)
                    : commPacket(std::make_shared<comm::CommPacket>(id, std::move(buffer), otherNode)), request(request) {}

            ~CommPacketMPIWrapper() = default;
            CommPacketMPIWrapper(CommPacketMPIWrapper &other) = delete;
            CommPacketMPIWrapper& operator=(CommPacketMPIWrapper &other) = delete;

            CommPacketMPIWrapper(CommPacketMPIWrapper &&other) noexcept {
                if (this == &other) return;
                this->commPacket = std::move(other.commPacket);
                this->request = other.request;
            }

            CommPacketMPIWrapper& operator=(CommPacketMPIWrapper &&other) noexcept {
                if (this == &other) return *this;
                this->commPacket = std::move(other.commPacket);
                this->request = other.request;

                return *this;
            }
        };

        int32_t numNodes_;
        int32_t nodeId_;
        std::mutex mutex_{};
        std::thread daemonThread_;
        std::atomic_int32_t myVote_ = false;
        std::atomic_bool stopDaemon_ = false;
        std::atomic_bool barrier_ = false;
        std::chrono::milliseconds daemonTimeSlice_ = std::chrono::milliseconds(16);

        // send related
        std::vector<std::queue<std::shared_ptr<CommPacketMPIWrapper>>> sendQueues_;
        std::vector<MetaDatum> sendMetadata_;
        std::list<std::shared_ptr<CommPacketMPIWrapper>> sendTasks_;

        // recv related
        MPI_Win recvMetadataWindow;
        std::vector<MetaDatum> recvMetadata_;// buffer for recvMetadataWindow
        std::list<std::shared_ptr<CommPacketMPIWrapper>> recvTasks_;

    private:
        DataWarehouse();
        void init();
        void daemon();
        void syncMetadata();
        void syncMessages();
        void processSendsAndRecvs();
        void castVote();
        void resetVote();
    public:
        ~DataWarehouse();
        static DataWarehouse *getInstance();
        void setDaemonTimeSlice(std::chrono::milliseconds timeSlice);
        void startDaemon();
        void stopDaemon();
        void setBarrier();
        bool isBarrierUp();
        void sendMessage(uint32_t typeId, std::string &&message, int destId);
    };
}

static comm::DataWarehouse *pDataWarehouse = nullptr;
comm::DataWarehouse* comm::DataWarehouse::getInstance() {
    if(pDataWarehouse == nullptr) {
        pDataWarehouse = new comm::DataWarehouse();
    }
    return pDataWarehouse;
}

comm::DataWarehouse::DataWarehouse() {
    numNodes_ = comm::getMpiNumNodes();
    nodeId_ = comm::getMpiNodeId();
    recvMetadataWindow = MPI_WIN_NULL;
    this->init();
}

void comm::DataWarehouse::init() {
    sendQueues_.resize(numNodes_);
    sendMetadata_.resize(numNodes_);

    recvMetadata_.resize(numNodes_);
    MPI_Win_create(
            recvMetadata_.data(),
            MPI_Aint(recvMetadata_.size() * sizeof(decltype(recvMetadata_)::value_type)),
            sizeof(decltype(recvMetadata_)::value_type),
            MPI_INFO_NULL,
            MPI_COMM_WORLD,
            &recvMetadataWindow
    );
}

void comm::DataWarehouse::daemon() {
    while(true) {
        // FIXME: myVote_ is used for both daemon and barrier functionalities, can cause problems later
        int32_t voteCount = 0;
        MPI_Allreduce(&myVote_, &voteCount, 1, MPI_INT32_T, MPI_SUM, MPI_COMM_WORLD);
        if(voteCount == numNodes_) {
            this->resetVote();
            if(barrier_.load()) {
                barrier_.store(false);
            }
            if(stopDaemon_.load()) {
                return;
            }
        }

        /*SCOPED*/{
            std::lock_guard lc(this->mutex_);
            // send metadata
            syncMetadata();

            // send and receive messages
            syncMessages();

            // process sent and received messages
            processSendsAndRecvs();
        }
        std::this_thread::sleep_for(daemonTimeSlice_);
    }
}

void comm::DataWarehouse::setDaemonTimeSlice(std::chrono::milliseconds daemonTimeSlice) {
    daemonTimeSlice_ = daemonTimeSlice;
}

void comm::DataWarehouse::startDaemon() {
    stopDaemon_.store(false);
    daemonThread_ = std::thread(&comm::DataWarehouse::daemon, this);
}

void comm::DataWarehouse::stopDaemon() {
    stopDaemon_.store(true);
    this->castVote();
    if(daemonThread_.joinable()) {
        daemonThread_.join();
    }
}

void comm::DataWarehouse::setBarrier() {
    barrier_.store(true);
    this->castVote();
}

bool comm::DataWarehouse::isBarrierUp() {
    return barrier_.load();
}

void comm::DataWarehouse::syncMetadata() {
    MPI_Win_fence(0, recvMetadataWindow);
    for(int32_t node = 0; node < numNodes_; ++node) {
        if(sendQueues_[node].empty()) continue;

        auto sendData = sendQueues_[node].front();
        sendMetadata_[node].setBufferSize(sendData->commPacket->serializedData.size());
        sendMetadata_[node].setTypeId(sendData->commPacket->id);
        if(sendMetadata_[node].getBufferSize()) {//FIXME: unnecessary?
            MPI_Put(
                    sendMetadata_[node].data(),
                    2,
                    MPI_UINT32_T,
                    node,
                    nodeId_,
                    2,
                    MPI_UINT32_T,
                    recvMetadataWindow
            );
        }
    }
    MPI_Win_fence(0, recvMetadataWindow);
}

void comm::DataWarehouse::syncMessages() {
    for(int32_t node = 0; node < numNodes_; ++node) {
        if(recvMetadata_[node].getBufferSize()) {
            auto recvData = std::make_shared<CommPacketMPIWrapper>(
                    recvMetadata_[node].getTypeId(),
                    std::string(recvMetadata_[node].getBufferSize(), 0),
                    node,
                    MPI_Request()
            );
            recvTasks_.emplace_back(recvData);

            MPI_Irecv(
                    recvData->commPacket->serializedData.data(),
                    (int)recvData->commPacket->serializedData.size(),
                    MPI_CHAR,
                    node,
                    MPI_ANY_TAG,
                    MPI_COMM_WORLD,
                    &recvData->request
            );
            recvMetadata_[node].reset();
        }

        if(!sendQueues_[node].empty()) {
            auto sendData = sendQueues_[node].front();
            sendQueues_[node].pop();
            sendTasks_.emplace_back(sendData);
            MPI_Isend(
                    sendData->commPacket->serializedData.data(),
                    (int)sendData->commPacket->serializedData.size(),
                    MPI_CHAR,
                    node,
                    nodeId_,
                    MPI_COMM_WORLD,
                    &sendData->request
            );
            sendMetadata_[node].reset();
        }
    }
}

void comm::DataWarehouse::processSendsAndRecvs() {
    int flag;
    MPI_Status status;
    for(auto sendData = sendTasks_.begin(); sendData != sendTasks_.end();) {
        MPI_Test(&(*sendData)->request, &flag, &status);
        if(flag) {
            sendData = sendTasks_.erase(sendData);
        } else {
            sendData++;
        }
    }

    for(auto recvData = recvTasks_.begin(); recvData != recvTasks_.end();) {
        MPI_Test(&(*recvData)->request, &flag, &status);
        if(flag) {
            comm::signal.emit(std::move((*recvData)->commPacket));
            recvData = recvTasks_.erase(recvData);
        }
    }
}

void comm::DataWarehouse::castVote() {
    myVote_.store(true);
}

void comm::DataWarehouse::resetVote() {
    myVote_.store(false);
}

comm::DataWarehouse::~DataWarehouse() {
    MPI_Win_free(&recvMetadataWindow);
    pDataWarehouse = nullptr;
}

void verifyCommInitialization() {
    if(!mpiGlobalLockGuardInitialized) {
        throw std::runtime_error(
            "Declare comm::CommLockGuard RAII object on stack, in main (or lifetime of application), to initialize comm library\n"
            "Usage:\n\n"
            "int main(int argc, char *argv[]) {\n"
            "\tcomm::CommLockGuard commLockGuard(&argc, &argv);\n"
            "\t// Do stuff...\n"
            "}"
        );
    }
}

void comm::DataWarehouse::sendMessage(uint32_t id, std::string &&message, int32_t destId) {
    std::lock_guard lg(mutex_);
    sendQueues_[destId].emplace(std::make_shared<CommPacketMPIWrapper>(
            id,
            std::move(message),
            destId,
            MPI_Request()
    ));
}

bool comm::isInitialized() {
    return mpiGlobalLockGuardInitialized;
}

void comm::setDaemonTimeSlice(std::chrono::milliseconds timeSlice) {
    verifyCommInitialization();
    DataWarehouse::getInstance()->setDaemonTimeSlice(timeSlice);
}

void comm::stopDaemon() {
    DataWarehouse::getInstance()->stopDaemon();
}

int comm::getMpiNodeId() {
    verifyCommInitialization();
    return mpiNodeId;
}

int comm::getMpiNumNodes() {
    verifyCommInitialization();
    return mpiNumNodes;
}

bool comm::isMpiRootPid() {
    verifyCommInitialization();
    return mpiNodeId == 0;
}

comm_::MPI_GlobalLockGuard::MPI_GlobalLockGuard(int32_t *argc, char **argv[]) {
    int32_t flag = false;
    if(auto status = MPI_Initialized(&flag); status != MPI_SUCCESS or flag == false) {
        //TODO: Do we need this?
//        int32_t provided;
//        if(MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided) == MPI_SUCCESS) {
        if(MPI_Init(argc, argv) == MPI_SUCCESS) {
            if (auto status = MPI_Initialized(&flag); status == MPI_SUCCESS and flag) {
                MPI_Comm_rank(MPI_COMM_WORLD, &mpiNodeId);
                MPI_Comm_size(MPI_COMM_WORLD, &mpiNumNodes);
                mpiGlobalLockGuardInitialized = true;
            }
#if not NDEBUG
            if(comm::isMpiRootPid()) printf("[CommLockGuard] MPI initialized\n");
#endif
            comm::DataWarehouse::getInstance()->startDaemon();
        }
    }
}

comm_::MPI_GlobalLockGuard::~MPI_GlobalLockGuard() {
    int32_t flag = false;
    if(auto status = MPI_Initialized(&flag); status == MPI_SUCCESS and flag) {
        comm::DataWarehouse::getInstance()->stopDaemon();
        delete comm::DataWarehouse::getInstance();
        if(MPI_Finalize() == MPI_SUCCESS) {
#if not NDEBUG
            if(comm::isMpiRootPid()) printf("[CommLockGuard] MPI exited\n");
#endif
            mpiGlobalLockGuardInitialized = false;
        }
    }
}

void comm::sendMessage(uint32_t id, std::string &&message, int32_t destId) {
    verifyCommInitialization();
    comm::DataWarehouse::getInstance()->sendMessage(id, std::move(message), destId);
}

void comm::connectReceiver(std::function<void(SignalType)> slot) {
    verifyCommInitialization();
    comm::signal.connect(slot);
}

void comm::barrier() {
    comm::DataWarehouse::getInstance()->setBarrier();
    while(comm::DataWarehouse::getInstance()->isBarrierUp());
}