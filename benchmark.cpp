#include "comm/comm.h"
#include <iostream>

/*
 * Ring implementation
 */

constexpr uint32_t arraySize = 1024;

template<typename Type>
std::string serialize(Type *array, size_t size) {
    std::string bytes(sizeof(Type)*size, '*');
    std::copy(
        static_cast<const char*>(static_cast<const void*>(array)),
        static_cast<const char*>(static_cast<const void*>(array)) + sizeof(Type)*size,
        bytes.data()
    );

    return bytes;
}

template<typename Type>
std::vector<Type> deserialize(std::string &bytes) {
    size_t size = bytes.size()/sizeof(Type);
    std::vector<Type> array(size, Type(0));
    std::copy(
        static_cast<const Type*>(static_cast<const void*>(bytes.data())),
        static_cast<const Type*>(static_cast<const void*>(bytes.data())) + size,
        array.data()
    );

    return array;
}


int main(int argc, char *argv[]) {
    comm::MPI_GlobalLockGuard mpiGlobalLockGuard(&argc, &argv);
    int rank = comm::getMpiNodeId(), size = comm::getMpiNumNodes();

    int32_t send[arraySize];
    for(size_t i = 0; i < arraySize; ++i) {
        send[i] = rank;
    }

    printf("[Process %d] sending data to %d\n", rank, (rank+1)%size);
    comm::Communicator::sendMessage(0, serialize(send, arraySize), (rank+1)%size);
    comm::Communicator::signal.connect([rank](std::shared_ptr<comm::CommPacket> commPacket) {
        auto recv = deserialize<int32_t>(commPacket->serializedData);
        if(rank == 0) {
            for(auto el: recv) {
                std::cout << el << ' ';
            }
            std::cout<<std::endl;
        }
    });

    return 0;
}