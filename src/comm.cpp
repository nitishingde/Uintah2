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

namespace comm {
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
        std::atomic_int32_t stopDaemon_ = false;

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
    public:
        ~DataWarehouse();
        static DataWarehouse *getInstance();
        void startDaemon();
        void stopDaemon();
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
        int32_t exitVote = 0;
        MPI_Allreduce(&stopDaemon_, &exitVote, 1, MPI_INT32_T, MPI_SUM, MPI_COMM_WORLD);
        if(exitVote == numNodes_) {// all MPI processes agree to exit
            return;
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
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void comm::DataWarehouse::startDaemon() {
    stopDaemon_ = false;
    daemonThread_ = std::thread(&comm::DataWarehouse::daemon, this);
}

void comm::DataWarehouse::stopDaemon() {
    stopDaemon_ = true;
    daemonThread_.join();
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
            comm::Communicator::signal.emit(std::move((*recvData)->commPacket));
            recvData = recvTasks_.erase(recvData);
        }
    }
}

comm::DataWarehouse::~DataWarehouse() {
    pDataWarehouse = nullptr;
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

static int32_t sIsMpiRootPid = -1;
bool comm::isMpiRootPid() {
    if (sIsMpiRootPid == -1) {
        int32_t flag = false;
        if (auto status = MPI_Initialized(&flag); status == MPI_SUCCESS and flag) {
            int32_t processId;
            MPI_Comm_rank(MPI_COMM_WORLD, &processId);
            sIsMpiRootPid = (processId == 0);
        }
    }
    return sIsMpiRootPid;
}

int comm::getMpiNodeId() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
}

int comm::getMpiNumNodes() {
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return size;
}

comm::MPI_GlobalLockGuard::MPI_GlobalLockGuard(int32_t *argc, char **argv[]) {
    int32_t flag = false;
    if(auto status = MPI_Initialized(&flag); status != MPI_SUCCESS or flag == false) {
        //TODO: Do we need this?
//        int32_t provided;
//        if(MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided) == MPI_SUCCESS) {
        if(MPI_Init(argc, argv) == MPI_SUCCESS) {
            if(isMpiRootPid()) printf("[MPI_GlobalLockGuard] MPI initialized\n");
            comm::DataWarehouse::getInstance()->startDaemon();
        }
    }
}

comm::MPI_GlobalLockGuard::~MPI_GlobalLockGuard() {
    int32_t flag = false;
    if(auto status = MPI_Initialized(&flag); status == MPI_SUCCESS and flag) {
        comm::DataWarehouse::getInstance()->stopDaemon();
        if(MPI_Finalize() == MPI_SUCCESS) {
            if(isMpiRootPid()) printf("[MPI_GlobalLockGuard] MPI exited\n");
            delete comm::DataWarehouse::getInstance();
        }
    }
}

void comm::Communicator::sendMessage(uint32_t id, std::string &&message, int32_t destId) {
    comm::DataWarehouse::getInstance()->sendMessage(id, std::move(message), destId);
}

comm::Signal comm::Communicator::signal {};
