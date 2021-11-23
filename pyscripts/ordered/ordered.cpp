#include <iostream>
#include <cmath>
#include <vector>
#include <chrono>
#include <omp.h>

const static size_t SIZE = 1 << 18;
const static int NTHREADS = 36;

void prefix_sum_serial(std::vector<uint64_t>& vals) {
  std::vector<uint64_t> temp(vals.size());
  for (int i = 1; i < vals.size(); ++i) {
    vals[i] = 47;
    if (i == 0) {
      temp[i] = vals[i];
    } else {
      temp[i] = temp[i-1] + vals[i];
    }
  }

  for (int i = 0; i < vals.size(); ++i) {
    vals[i] = temp[i];
  }
}

void prefix_sum(std::vector<uint64_t>& vals) {
  std::vector<uint64_t> temp(vals.size());
#pragma omp parallel for ordered num_threads(NTHREADS)
  for (int i = 1; i < vals.size(); ++i) {
    vals[i] = 47;
#pragma omp ordered
    if (i == 0) {
      temp[i] = vals[i];
    } else {
      temp[i] = temp[i-1] + vals[i];
    }
  }

#pragma omp parallel for num_threads(NTHREADS)
  for (int i = 0; i < vals.size(); ++i) {
    vals[i] = temp[i];
  }
}


void prefix_sum_fast(std::vector<uint64_t>& vals) {
  size_t len = vals.size();
  std::vector<uint64_t> temp(len);
#pragma omp parallel for num_threads(NTHREADS)
  for (int i = 1; i < len; ++i) {
    vals[i] = 47;
  }

  for (int j = 0; j < log2(len); j++) {
#pragma omp parallel for num_threads(NTHREADS)
    for (int i = 1 << j; i < len; i++) {
      temp[i] = vals[i] + vals[i - (1 << j)];
    }

#pragma omp parallel for num_threads(NTHREADS)
    for (int i = 1 << j; i < len; i++) {
      vals[i] = temp[i];
    }
  }
}

int main() {
  std::vector<uint64_t> vals(SIZE);

  auto start = std::chrono::system_clock::now();
  prefix_sum_serial(vals);
  auto stop = std::chrono::system_clock::now();
  std::cout << vals.back() << "\n";
  std::chrono::duration<double> elapsed_seconds = (stop - start);
  std::cout << "version 0 takes "
            << elapsed_seconds.count() << " seconds\n";

  start = std::chrono::system_clock::now();
  prefix_sum(vals);
  stop = std::chrono::system_clock::now();
  std::cout << vals.back() << "\n";
  elapsed_seconds = (stop - start);
  std::cout << "version 1 takes "
            << elapsed_seconds.count() << " seconds\n";

  start = std::chrono::system_clock::now();
  prefix_sum_fast(vals);
  stop = std::chrono::system_clock::now();
  std::cout << vals.back() << "\n";
  elapsed_seconds = (stop - start);
  std::cout << "version 2 takes "
            << elapsed_seconds.count() << " seconds\n";

}
