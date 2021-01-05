#include <string.h>

extern "C" char run_parser(const char* input);

int main(int argc, char* argv[])
{
    if (argc < 2) {
        while (1) {
            int ret = run_parser(nullptr);
            if (ret == 0)
                break;
        }
    }
    return 0;
}
