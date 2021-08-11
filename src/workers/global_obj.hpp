#ifndef GLOBAL_OBJ_HPP
#define GLOBAL_OBJ_HPP

#include "utils/MQ.hpp"

CMessageQueue *ChunkMQ = new CMessageQueue();
CMessageQueue *BatchRecordMQ = new CMessageQueue();

#endif