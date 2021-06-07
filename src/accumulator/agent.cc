#include "accumulator.hh"

#include <algorithm>
#include <cmath>

using namespace std;

namespace r2t2 {

TileHelper::TileHelper( const uint32_t accumulators,
                        const pbrt::Bounds2i& sample_bounds,
                        const uint32_t spp )
  : accumulators_( accumulators )
  , bounds_( sample_bounds )
  , extent_( bounds_.Diagonal() )
  , spp_( spp )
{
  tile_size_ = ceil( sqrt( extent_.x * extent_.y / accumulators_ ) );

  while ( ceil( 1.0 * extent_.x / tile_size_ )
            * ceil( 1.0 * extent_.y / tile_size_ )
          > accumulators_ ) {
    tile_size_++;
  }

  active_accumulators_
    = static_cast<uint32_t>( ceil( 1.0 * extent_.x / tile_size_ )
                             * ceil( 1.0 * extent_.y / tile_size_ ) );

  n_tiles_ = { ( extent_.x + tile_size_ - 1 ) / tile_size_,
               ( extent_.y + tile_size_ - 1 ) / tile_size_ };
}

uint32_t TileHelper::tile_id( const pbrt::Sample& sample ) const
{
  const auto pixel = sample.SamplePixel(
    { static_cast<int>( extent_.x ), static_cast<int>( extent_.y ) }, spp_ );
  return ceil( 1.0 * pixel.x / tile_size_ )
         + ceil( 1.0 * pixel.y / tile_size_ ) * n_tiles_.x;
}

pbrt::Bounds2<uint32_t> TileHelper::bounds( const uint32_t tile_id ) const
{
  if ( tile_id >= active_accumulators_ ) {
    throw runtime_error( "empty bounds" );
  }

  const uint32_t tile_x = tile_id % n_tiles_.x;
  const uint32_t tile_y = tile_id / n_tiles_.x;

  const uint32_t x0 = bounds_.pMin.x + tile_x * tile_size_;
  const uint32_t x1 = min( x0 + tile_size_, bounds_.pMax.x );
  const uint32_t y0 = bounds_.pMin.y + tile_y * tile_size_;
  const uint32_t y1 = min( y0 + tile_size_, bounds_.pMax.y );

  return { { x0, y0 }, { x1, y1 } };
}

}
