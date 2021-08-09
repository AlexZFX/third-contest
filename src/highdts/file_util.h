#include <pthread.h>
#include <string>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;

static char *file_mmap(const char *path)
{
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd == -1)
    {
        cout << "[ERROR] file can't open : " + *path << endl;
        return nullptr;
    }

    struct stat st;
    int ret = fstat(fd, &st);

    // 开始进行文件 mmap 映射
    char *mem_file = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_FILE, fd, 0);

    close(fd);

    if (mem_file == MAP_FAILED)
    {
        // TODO 这里 mmap 失败，是否直接退出
        cout << "[ERROR] do mmap failed " << endl;
        return nullptr;
    }
}