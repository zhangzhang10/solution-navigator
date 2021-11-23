#include <stdio.h>
#include <math.h>


int main() {
  const int n = 25;
  int i = 0, j = 0;

  for (j = 0; j < log2(n); j++) {
    printf("\nj = %d\n", j);
    for (i = 1 << j; i < n; i++) {
      printf("(%d, %d) ", i, (i-(1<<j)));
    }
  }
}

