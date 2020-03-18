/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <csignal>
#include <cstring>

#include "exception.hh"
#include "signalfd.hh"

using namespace std;

/* add given signals to the mask */
SignalMask::SignalMask( const initializer_list<int> signals )
  : mask_()
{
  CheckSystemCall( "sigemptyset", sigemptyset( &mask_ ) );

  for ( const auto signal : signals ) {
    CheckSystemCall( "sigaddset", sigaddset( &mask_, signal ) );
  }
}

/* get the current mask */
SignalMask SignalMask::current_mask( void )
{
  SignalMask mask = {};

  CheckSystemCall( "sigprocmask",
                   sigprocmask( SIG_BLOCK, nullptr, &mask.mask_ ) );

  return mask;
}

/* challenging to compare two sigset_t's for equality */
bool SignalMask::operator==( const SignalMask& other ) const
{
  for ( int signum = 0; signum < SIGRTMAX; signum++ ) {
    if ( sigismember( &mask_, signum )
         != sigismember( &other.mask_, signum ) ) {
      return false;
    }
  }

  return true;
}

/* mask these signals from interrupting our process */
/* (because we'll use a signalfd instead to read them */
void SignalMask::set_as_mask( void ) const
{
  CheckSystemCall( "sigprocmask", sigprocmask( SIG_SETMASK, &mask_, nullptr ) );
}

SignalFD::SignalFD( const SignalMask& signals )
  : FileDescriptor(
    CheckSystemCall( "signalfd", signalfd( -1, &signals.mask(), 0 ) ) )
{}

/* read one signal */
signalfd_siginfo SignalFD::read_signal( void )
{
  signalfd_siginfo delivered_signal;

  const size_t length = read( reinterpret_cast<char*>( &delivered_signal ) );

  if ( length != sizeof( signalfd_siginfo ) ) {
    throw runtime_error( "signalfd read size mismatch" );
  }

  return delivered_signal;
}
