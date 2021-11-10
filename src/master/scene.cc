#include <pbrt/core/geometry.h>

#include "lambda-master.hh"
#include "messages/message.hh"
#include "messages/utils.hh"

using namespace std;
using namespace r2t2;
using namespace pbrt;
using namespace meow;
using namespace protoutil;

using OpCode = Message::OpCode;

void LambdaMaster::assign_object( Worker& worker, const SceneObject& object )
{
  worker.objects.insert( object );
}

void LambdaMaster::assign_treelet( Worker& worker, Treelet& treelet )
{
  assign_object( worker, { { ObjectType::Treelet, treelet.id } } );

  unassigned_treelets.erase( treelet.id );
  move_from_pending_to_queued( treelet.id );

  worker.treelets.push_back( treelet.id );
  treelet.workers.insert( worker.id );

  auto& dependencies = scene.base.GetTreeletDependencies( treelet.id );

  for ( const auto& obj : dependencies ) {
    assign_object( worker, { obj } );
  }
}

vector<SceneObject> LambdaMaster::list_base_objects() const
{
  vector<SceneObject> res;
  res.reserve( SceneData::base_object_types.size() );

  for ( const auto scene_obj : SceneData::base_object_types ) {
    if ( alternative_object_names.count( scene_obj ) ) {
      res.emplace_back( ObjectKey { scene_obj, 0 },
                        alternative_object_names.at( scene_obj ) );
    } else {
      res.emplace_back( ObjectKey { scene_obj, 0 } );
    }
  }

  return res;
}

void LambdaMaster::assign_base_objects( Worker& worker )
{
  for ( const auto& obj : list_base_objects() ) {
    assign_object( worker, obj );
  }
}

LambdaMaster::SceneData::SceneData( const std::string& scene_path,
                                    const int samples_per_pixel,
                                    const optional<Bounds2i>& crop_window )
  : base( scene_path, samples_per_pixel )
  , sample_bounds( crop_window.has_value() ? *crop_window : base.sampleBounds )
  , sample_extent( sample_bounds.Diagonal() )
  , total_paths( sample_extent.x * sample_extent.y * base.samplesPerPixel )
{}

int default_tile_size( int spp )
{
  int bytes_per_sec = 30e+6;
  int avg_ray_bytes = 500;
  int rays_per_sec = bytes_per_sec / avg_ray_bytes;

  return ceil( sqrt( rays_per_sec / spp ) );
}

int auto_tile_size( const Bounds2i& bounds, const long int spp, const size_t N )
{
  int tile_size = ceil( sqrt( bounds.Area() / N ) );
  const Vector2i extent = bounds.Diagonal();
  const int safe_tile_limit = ceil( sqrt( WORKER_MAX_ACTIVE_RAYS / 2 / spp ) );

  while ( ceil( 1.0 * extent.x / tile_size )
            * ceil( 1.0 * extent.y / tile_size )
          > N ) {
    tile_size++;
  }

  tile_size = min( tile_size, safe_tile_limit );

  return tile_size;
}

LambdaMaster::Tiles::Tiles( const int size,
                            const Bounds2i& bounds,
                            const long int spp,
                            const uint32_t num_workers )
  : tile_size( size )
  , sample_bounds( bounds )
  , tile_spp( spp )
{
  if ( tile_size == 0 ) {
    tile_size = default_tile_size( spp );
  } else if ( tile_size == numeric_limits<typeof( tile_size )>::max() ) {
    tile_size = auto_tile_size( bounds, spp, num_workers );
  }

  n_tiles = Point2i( ( bounds.Diagonal().x + tile_size - 1 ) / tile_size,
                     ( bounds.Diagonal().y + tile_size - 1 ) / tile_size );

  tiles_to_render.resize( n_tiles.x * n_tiles.y );
  iota( tiles_to_render.begin(), tiles_to_render.end(), 0 );
  shuffle( tiles_to_render.begin(),
           tiles_to_render.end(),
           mt19937 { random_device {}() } );
}

bool LambdaMaster::Tiles::camera_rays_remaining() const
{
  return cur_tile_idx < ( static_cast<size_t>( n_tiles.x )
                          * static_cast<size_t>( n_tiles.y ) );
}

Bounds2i LambdaMaster::Tiles::next_camera_tile()
{
  const auto t = tiles_to_render[cur_tile_idx++];

  const int tile_x = t % n_tiles.x;
  const int tile_y = t / n_tiles.x;
  const int x0 = sample_bounds.pMin.x + tile_x * tile_size;
  const int x1 = min( x0 + tile_size, sample_bounds.pMax.x );
  const int y0 = sample_bounds.pMin.y + tile_y * tile_size;
  const int y1 = min( y0 + tile_size, sample_bounds.pMax.y );

  return Bounds2i( Point2i { x0, y0 }, Point2i { x1, y1 } );
}

void LambdaMaster::Tiles::send_worker_tile( Worker& worker )
{
  protobuf::GenerateRays proto;

  const Bounds2i next_tile = next_camera_tile();
  proto.set_x0( next_tile.pMin.x );
  proto.set_y0( next_tile.pMin.y );
  proto.set_x1( next_tile.pMax.x );
  proto.set_y1( next_tile.pMax.y );

  worker.ray_counters.camera += next_tile.Area() * tile_spp;

  worker.client.push_request(
    { 0, OpCode::GenerateRays, protoutil::to_string( proto ) } );
}
