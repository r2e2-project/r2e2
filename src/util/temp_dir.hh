/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <vector>
#include <string>

class UniqueDirectory
{
private:
  std::vector<char> mutable_temp_dirname_;

protected:
  bool moved_away_;

public:
  UniqueDirectory( const std::string & dirname_template );
  virtual ~UniqueDirectory() {}

  std::string name( void ) const;

  /* ban copying */
  UniqueDirectory( const UniqueDirectory & other ) = delete;
  UniqueDirectory & operator=( const UniqueDirectory & other ) = delete;

  /* allow move constructor */
  UniqueDirectory( UniqueDirectory && other );

  /* ... but not move assignment operator */
  UniqueDirectory & operator=( UniqueDirectory && other ) = delete;
};

/* TempDirectory is deleted when object destroyed */
class TempDirectory : public UniqueDirectory
{
public:
  using UniqueDirectory::UniqueDirectory;

  /* allow move constructor */
  TempDirectory( TempDirectory && other ) : UniqueDirectory( std::move( other ) ) {}

  ~TempDirectory();
};
