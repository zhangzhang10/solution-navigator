#include <iostream>
#include <omp.h>

void foo(int n, int m) {
#pragma omp parallel for schedule(static) num_threads(4) if(m > 4)
  for (int i  = 0; i < m; ++i) {
      if (i == 0 && omp_get_thread_num() == 0 && omp_in_parallel()) {
        std::cout << "outer loop in parallel\n";
      }
#pragma omp parallel for schedule(static) num_threads(4) if(n/m >= 2)
    for (int j = 0; j < n; ++j) {
      if (omp_get_thread_num() == 1) {
        std::cout << j << " ";
      }
    }
    std::cout << std::endl;
  }
}

int main()
{
  omp_set_num_threads(4);
  foo(20, 10);
}
