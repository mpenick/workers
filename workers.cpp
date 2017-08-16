#include <cstdio>
#include <atomic>
#include <random>
#include <algorithm>
#include <iterator>
#include <cassert>

#include <uv.h>

#include "mpmc_queue.hpp"
#include "hdr_histogram.hpp"

#define NUM_THREADS 4
#define NUM_ITERATIONS 10000000
#define HIGHEST_TRACKABLE_VALUE 3600LL * 1000LL * 1000LL

cass::MPMCQueue<uint64_t> queue(8 * 1024 * 1024);

struct BaseWorker {
  BaseWorker() {
    hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histogram);
  }

  virtual ~BaseWorker()  {
    free(histogram);
  }

  void record(int64_t value) {
    hdr_record_value(histogram, value);
  }

  void dump() {
    int64_t max = hdr_max(histogram);
    int64_t min = hdr_min(histogram);
    double mean = hdr_mean(histogram);
    double stddev = hdr_stddev(histogram);
    int64_t median = hdr_value_at_percentile(histogram, 50.0);
    int64_t percentile_75th = hdr_value_at_percentile(histogram, 75.0);
    int64_t percentile_95th = hdr_value_at_percentile(histogram, 95.0);
    int64_t percentile_98th = hdr_value_at_percentile(histogram, 98.0);
    int64_t percentile_99th = hdr_value_at_percentile(histogram, 99.0);
    int64_t percentile_999th = hdr_value_at_percentile(histogram, 99.9);
    printf("final stats (nanoseconds): min %lld max %lld median %lld 75th %lld 95th %lld 98th %lld 99th %lld 99.9th %lld mean: %f stddev: %f\n",
           (long long int)min, (long long int)max,
           (long long int)median, (long long int)percentile_75th,
           (long long int)percentile_95th, (long long int)percentile_98th,
           (long long int)percentile_99th, (long long int)percentile_999th,
           mean, stddev);
  }

  virtual void send() = 0;
  virtual int init()  = 0;
  virtual void join() = 0;

  hdr_histogram* histogram;
};

struct ThreadWorker : public BaseWorker {
  virtual void run() = 0;

  virtual int init() {
    return uv_thread_create(&thread, on_thread, this);
  }

  virtual void join() {
    uv_thread_join(&thread);
  }

  static void on_thread(void* arg) {
    ThreadWorker* worker = (ThreadWorker*)arg;
    worker->run();
  }

  uv_thread_t thread;
};

struct LoopWorker : public ThreadWorker {
  LoopWorker() {
    loop.data = this;
    async.data = this;
  }

  int init() {
    int rc;
    rc = uv_loop_init(&loop);
    if (rc != 0) return rc;
    rc = uv_async_init(&loop, &async, on_async);
    if (rc != 0) return rc;
    rc = ThreadWorker::init();
    if (rc != 0) return rc;
    return rc;
  }

  void join() {
    ThreadWorker::join();
    uv_loop_close(&loop);
  }

  void send() {
    queue.memory_fence();
    uv_async_send(&async);
  }

  void run() {
    uv_run(&loop, UV_RUN_DEFAULT);
  }

  static void on_async(uv_async_t* handle) {
    LoopWorker* worker = (LoopWorker*)handle->data;

    (void)worker;

    bool done = false;

    uint64_t value;
    while (queue.dequeue(value)) {
      //printf("%lld %lld %p\n", uv_hrtime(), value, (void*)uv_thread_self());
      if (value ==  (uint64_t)-1) {
        done = true;
        break;
      } else {
        worker->record((int64_t)uv_hrtime() - value);
        //sched_yield();
      }
    }

    if (done) {
      worker->dump();
      //printf("Finished\n");
      uv_close((uv_handle_t*)handle, NULL);
    }
  }

  uv_async_t async;
  uv_loop_t loop;
};

#define USE_PENDING

struct SemWorker : public ThreadWorker {
#ifdef USE_PENDING
  SemWorker() : pending(false) { }
#endif

  int init() {
    int rc;
    rc = uv_sem_init(&sem, 0);
    if (rc != 0) return rc;
    return ThreadWorker::init();
  }

  void join() {
    ThreadWorker::join();
    uv_sem_destroy(&sem);
  }

  void send() {
#ifdef USE_PENDING
    queue.memory_fence();

    if (pending.load(std::memory_order_relaxed)) {
      return;
    }

    bool was_pending = false;
    if (pending.compare_exchange_strong(was_pending, true)) {
      uv_sem_post(&sem);
    }
#else
    uv_sem_post(&sem);
#endif
  }

  void run() {
    bool done = false;
    while(!done) {
      uint64_t value;
      uv_sem_wait(&sem);

#ifdef USE_PENDING
      bool was_pending = true;
      if (!pending.compare_exchange_strong(was_pending, false)) {
        continue;
      }
#endif
      assert(was_pending && "It was supposed to be pending");

      while (queue.dequeue(value)) {
        //printf("%d %p\n", value, (void*)uv_thread_self());
        if (value ==  (uint64_t)-1) {
          done = true;
          break;
        } else {
          record((int64_t)uv_hrtime() - value);
          //sched_yield();
        }
      }
    }
    dump();
    //printf("Finished\n");
  }

  uv_sem_t sem;
#ifdef USE_PENDING
  std::atomic<bool> pending;
#endif
};

struct BusyWorker : public ThreadWorker {
  void send() { }

  void run() {
    bool done = false;
    while (!done) {
      uint64_t value;
      while (queue.dequeue(value)) {
        //printf("%d %p\n", value, (void*)uv_thread_self());
        if (value ==  (uint64_t)-1) {
          done = true;
          break;
        } else {
          record((int64_t)uv_hrtime() - value);
          //sched_yield();
        }
      }
    }
    dump();
    //printf("Finished\n");
  }
};

void run_test(BaseWorker** workers, const char* name) {
  for (int i = 0; i < NUM_THREADS; ++i) {
    workers[i]->init();
  }

  size_t index = 0;

  uint64_t start = uv_hrtime();
  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    if(!queue.enqueue(uv_hrtime())) {
      assert(false && "Unable to queue");
    }
    workers[index++ % NUM_THREADS]->send();
  }
  double elapsed = (double)(uv_hrtime() - start) / (1000 * 1000 * 1000);
  printf("Test \"%s\": Elapsed: %f seconds, Rate: %f queues/second\n", name, elapsed, (double)NUM_ITERATIONS / elapsed);

  for (int i = 0; i < NUM_THREADS; ++i) {
    if(!queue.enqueue((uint64_t)-1)) {
      assert(false && "Unable to queue");
    }
    workers[index++ % NUM_THREADS]->send();
  }

  for (int i = 0; i < NUM_THREADS; ++i) {
    workers[i]->join();
  }
}

void run_sem_test() {
  BaseWorker* workers[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; ++i) {
    workers[i] = new SemWorker();
  }
  run_test(workers, "sema");
}

void run_busy_test() {
  BaseWorker* workers[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; ++i) {
    workers[i] = new BusyWorker();
  }
  run_test(workers, "busy");
}

void run_loop_test() {
  BaseWorker* workers[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; ++i) {
    workers[i] = new LoopWorker();
  }
  run_test(workers, "loop");
}

typedef void (*TestFunc)();

int main() {
  TestFunc tests[] = {
    run_sem_test,
    run_busy_test,
    run_loop_test,
    NULL,
  };

  std::random_device rd;
  std::mt19937 rng(rd());
  std::shuffle(tests, tests + 3, rng);

  for (TestFunc* func = tests; *func != NULL; func++) {
    (*func)();
  }
}
