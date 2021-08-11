#ifndef GLOBAL_OBJ_HPP
#define GLOBAL_OBJ_HPP

#include "../utils/ThreadSafaQueue.h"
#include "../common/FileChunk.h"

ThreadSafeQueue<FileChunk*, queue<FileChunk*>> *ChunkMQ = new ThreadSafeQueue<FileChunk*, queue<FileChunk*>>();
ThreadSafeQueue<LineRecord*, queue<LineRecord*>> *LineRecordMQ = new ThreadSafeQueue<LineRecord*, queue<LineRecord*>>();

#endif