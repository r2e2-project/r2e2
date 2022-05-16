
/*
    pbrt source code is Copyright(c) 1998-2016
                        Matt Pharr, Greg Humphreys, and Wenzel Jakob.

    This file is part of pbrt.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:

    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */

#include "parallel.hh"

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>

using namespace std;

namespace r2e2 {
namespace parallel {

class Barrier
{
public:
  Barrier( const int c )
    : count( c )
  {
    CHECK_GT( count, 0 );
  }

  ~Barrier() { CHECK_EQ( count, 0 ); }
  void Wait();

private:
  mutex mtx {};
  condition_variable cv {};
  int count;
};

// Parallel Local Definitions
static int threadCount = 0;
static vector<thread> threads;
static bool shutdownThreads = false;
class ParallelForLoop;
static ParallelForLoop* workList = nullptr;
static mutex workListMutex;

class ParallelForLoop
{
public:
  // ParallelForLoop Public Methods
  ParallelForLoop( function<void( int64_t )> func, int64_t maxIdx, int chunk )
    : func1D( move( func ) )
    , maxIndex( maxIdx )
    , chunkSize( chunk )
  {}

public:
  // ParallelForLoop Private Data
  function<void( int64_t )> func1D;
  const int64_t maxIndex;
  const int chunkSize;
  int64_t nextIndex = 0;
  int activeWorkers = 0;
  ParallelForLoop* next = nullptr;
  int nX = -1;

  ParallelForLoop( const ParallelForLoop& ) = delete;
  ParallelForLoop& operator=( const ParallelForLoop& ) = delete;

  // ParallelForLoop Private Methods
  bool Finished() const { return nextIndex >= maxIndex && activeWorkers == 0; }
};

void Barrier::Wait()
{
  unique_lock<mutex> lock( mtx );
  CHECK_GT( count, 0 );
  if ( --count == 0 )
    // This is the last thread to reach the barrier; wake up all of the
    // other ones before exiting.
    cv.notify_all();
  else
    // Otherwise there are still threads that haven't reached it. Give
    // up the lock and wait to be notified.
    cv.wait( lock, [this] { return count == 0; } );
}

static condition_variable workListCondition;

static void workerThreadFunc( int tIndex, shared_ptr<Barrier> barrier )
{
  LOG( INFO ) << "Started execution in worker thread " << tIndex;
  ThreadIndex = tIndex;

  // The main thread sets up a barrier so that it can be sure that all
  // workers have called ProfilerWorkerThreadInit() before it continues
  // (and actually starts the profiling system).
  barrier->Wait();

  // Release our reference to the Barrier so that it's freed once all of
  // the threads have cleared it.
  barrier.reset();

  unique_lock<mutex> lock( workListMutex );
  while ( !shutdownThreads ) {
    if ( !workList ) {
      // Sleep until there are more tasks to run
      workListCondition.wait( lock );
    } else {
      // Get work from _workList_ and run loop iterations
      ParallelForLoop& loop = *workList;

      // Run a chunk of loop iterations for _loop_

      // Find the set of loop iterations to run next
      int64_t indexStart = loop.nextIndex;
      int64_t indexEnd = min( indexStart + loop.chunkSize, loop.maxIndex );

      // Update _loop_ to reflect iterations this thread will run
      loop.nextIndex = indexEnd;
      if ( loop.nextIndex == loop.maxIndex )
        workList = loop.next;
      loop.activeWorkers++;

      // Run loop indices in _[indexStart, indexEnd)_
      lock.unlock();
      for ( int64_t index = indexStart; index < indexEnd; ++index ) {
        loop.func1D( index );
      }
      lock.lock();

      // Update _loop_ to reflect completion of iterations
      loop.activeWorkers--;
      if ( loop.Finished() )
        workListCondition.notify_all();
    }
  }
  LOG( INFO ) << "Exiting worker thread " << tIndex;
}

// Parallel Definitions
void For( function<void( int64_t )> func, int64_t count, int chunkSize )
{
  CHECK( threads.size() > 0 || MaxThreadIndex() == 1 );

  // Run iterations immediately if not using threads or if _count_ is small
  if ( threads.empty() || count < chunkSize ) {
    for ( int64_t i = 0; i < count; ++i )
      func( i );
    return;
  }

  // Create and enqueue _ParallelForLoop_ for this loop
  ParallelForLoop loop( move( func ), count, chunkSize );
  workListMutex.lock();
  loop.next = workList;
  workList = &loop;
  workListMutex.unlock();

  // Notify worker threads of work to be done
  unique_lock<mutex> lock( workListMutex );
  workListCondition.notify_all();

  // Help out with parallel loop iterations in the current thread
  while ( !loop.Finished() ) {
    // Run a chunk of loop iterations for _loop_

    // Find the set of loop iterations to run next
    int64_t indexStart = loop.nextIndex;
    int64_t indexEnd = min( indexStart + loop.chunkSize, loop.maxIndex );

    // Update _loop_ to reflect iterations this thread will run
    loop.nextIndex = indexEnd;
    if ( loop.nextIndex == loop.maxIndex )
      workList = loop.next;
    loop.activeWorkers++;

    // Run loop indices in _[indexStart, indexEnd)_
    lock.unlock();
    for ( int64_t index = indexStart; index < indexEnd; ++index ) {
      loop.func1D( index );
    }
    lock.lock();

    // Update _loop_ to reflect completion of iterations
    loop.activeWorkers--;
  }
}

thread_local int ThreadIndex;

int MaxThreadIndex()
{
  return ( threadCount == 0 ) ? NumSystemCores() : threadCount;
}

int NumSystemCores()
{
  return max( 1u, thread::hardware_concurrency() );
}

void Init( const int n_threads )
{
  CHECK_EQ( threads.size(), 0 );
  threadCount = n_threads;
  int nThreads = MaxThreadIndex();
  ThreadIndex = 0;

  // Create a barrier so that we can be sure all worker threads get past
  // their call to ProfilerWorkerThreadInit() before we return from this
  // function.  In turn, we can be sure that the profiling system isn't
  // started until after all worker threads have done that.
  shared_ptr<Barrier> barrier = make_shared<Barrier>( nThreads );

  // Launch one fewer worker thread than the total number we want doing
  // work, since the main thread helps out, too.
  for ( int i = 0; i < nThreads - 1; ++i )
    threads.push_back( thread( workerThreadFunc, i + 1, barrier ) );

  barrier->Wait();
}

void Cleanup()
{
  if ( threads.empty() )
    return;

  {
    lock_guard<mutex> lock( workListMutex );
    shutdownThreads = true;
    workListCondition.notify_all();
  }

  for ( thread& thread : threads )
    thread.join();
  threads.erase( threads.begin(), threads.end() );
  shutdownThreads = false;
}

}
}
