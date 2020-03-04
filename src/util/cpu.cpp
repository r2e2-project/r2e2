#include "cpu.h"

#include <unistd.h>

#include <fstream>
#include <stdexcept>
#include <string>

using namespace std;

const int64_t CPUStats::TIME_UNIT = sysconf(_SC_CLK_TCK);

CPUStats::CPUStats() {
    ifstream fin{"/proc/stat"};
    string first;

    fin >> first;
    if (first != "cpu") {
        throw runtime_error("unsupported format for /proc/stat");
    }

    fin >> user >> nice >> system >> idle >> iowait >> irq >> soft_irq >>
        steal >> guest >> guest_nice;
}

CPUStats CPUStats::operator-(const CPUStats &other) const {
    CPUStats res;

    res.user = user - other.user;
    res.nice = nice - other.nice;
    res.system = system - other.system;
    res.idle = idle - other.idle;
    res.iowait = iowait - other.iowait;
    res.irq = irq - other.irq;
    res.soft_irq = soft_irq - other.soft_irq;
    res.steal = steal - other.steal;
    res.guest = guest - other.guest;
    res.guest_nice = guest_nice - other.guest_nice;

    return res;
}
