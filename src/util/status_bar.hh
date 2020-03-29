/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <cstdio>
#include <string>
#include <sys/ioctl.h>

class StatusBar
{
private:
  std::string text_ {};
  winsize window_size_ {};
  StatusBar();

  void init();
  void remove();

public:
  ~StatusBar();

  StatusBar( const StatusBar& ) = delete;
  void operator=( const StatusBar& ) = delete;

  static StatusBar& get();

  static void redraw();
  static void set_text( const std::string& text );
};
