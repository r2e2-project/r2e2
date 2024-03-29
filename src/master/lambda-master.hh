#pragma once

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include "common/lambda.hh"
#include "common/stats.hh"
#include "common/tile_helper.hh"
#include "messages/message.hh"
#include "net/address.hh"
#include "net/aws.hh"
#include "net/http_client.hh"
#include "net/http_request.hh"
#include "net/lambda.hh"
#include "net/session.hh"
#include "r2e2.pb.h"
#include "schedulers/scheduler.hh"
#include "storage/backend_s3.hh"
#include "transfer/transfer.hh"
#include "util/eventfd.hh"
#include "util/signalfd.hh"
#include "util/temp_dir.hh"
#include "util/timerfd.hh"
#include "util/util.hh"

#include <pbrt/core/geometry.h>
#include <pbrt/main.h>

namespace r2e2 {

constexpr std::chrono::milliseconds STATUS_PRINT_INTERVAL { 1'000 };
constexpr std::chrono::milliseconds RESCHEDULE_INTERVAL { 1'000 };
constexpr std::chrono::milliseconds WORKER_INVOCATION_INTERVAL { 5'000 };

struct MasterConfiguration
{
  int samples_per_pixel;
  int max_path_depth;
  std::chrono::milliseconds bagging_delay;
  bool collect_debug_logs;
  bool write_stat_logs;
  float ray_log_rate;
  float bag_log_rate;
  std::optional<std::string> auto_name_log_dir_tag;
  std::filesystem::path logs_directory;
  std::optional<pbrt::Bounds2i> crop_window;
  int tile_size;
  std::chrono::seconds timeout;
  std::string job_summary_path;
  uint64_t new_tile_threshold;

  std::optional<std::filesystem::path> alt_scene_file;

  std::vector<std::string> memcached_servers;
  std::vector<std::pair<std::string, uint32_t>> engines;

  bool profiling_run;
};

class LambdaMaster
{
public:
  LambdaMaster( const uint16_t listen_port,
                const uint16_t client_port,
                const uint32_t max_workers,
                const uint32_t ray_generators,
                const uint32_t accumulators,
                const std::string& public_address,
                const std::string& storage_backend,
                const std::vector<std::string>& aws_region,
                std::unique_ptr<Scheduler>&& scheduler,
                const MasterConfiguration& config );

  ~LambdaMaster();

  void run();

  protobuf::JobSummary get_job_summary() const;
  void print_job_summary() const;
  void print_pbrt_stats() const;
  void dump_job_summary( const std::string& path ) const;

private:
  using steady_clock = std::chrono::steady_clock;

  ////////////////////////////////////////////////////////////////////////////
  // Master State                                                           //
  ////////////////////////////////////////////////////////////////////////////

  enum class State
  {
    Active,
    WindingDown,
    Terminated
  };

  State state_ { State::Active };

  ////////////////////////////////////////////////////////////////////////////
  // Job Information                                                        //
  ////////////////////////////////////////////////////////////////////////////

  MasterConfiguration config;
  const UniqueDirectory scene_dir { "/tmp/r2e2-lambda-master" };
  const std::string job_id;

  ////////////////////////////////////////////////////////////////////////////
  // Cloud                                                                  //
  ////////////////////////////////////////////////////////////////////////////

  struct AWSRegion
  {
    std::string name;
    Address address;

    AWSRegion( const std::string& region_name )
      : name( region_name )
      , address( LambdaInvocationRequest::endpoint( name ), "https" )
    {
    }
  };

  const AWSCredentials aws_credentials {};

  const std::string public_address;
  const std::string storage_backend_uri;
  const Storage storage_backend_info;
  S3StorageBackend scene_storage_backend;
  S3StorageBackend job_storage_backend;
  const std::vector<AWSRegion> aws_regions;
  const std::string lambda_function_name {
    safe_getenv_or( "R2E2_LAMBDA_FUNCTION", "r2e2-lambda-function" )
  };

  std::string output_preview_url {};

  ////////////////////////////////////////////////////////////////////////////
  // Workers                                                                //
  ////////////////////////////////////////////////////////////////////////////

  struct Worker
  {
    enum class State
    {
      Initializing,
      Active,
      FinishingUp,
      Terminating,
      Terminated
    };

    enum class Role
    {
      Generator,
      Tracer,
      Accumulator
    };

    Worker( const WorkerId id_, const Role role_, TCPSocket&& sock )
      : id( id_ )
      , role( role_ )
      , client( TCPSession { std::move( sock ) } )
    {
      Worker::active_count[role]++;
    }

    WorkerId id;
    State state { State::Initializing };
    Role role;

    meow::Client<TCPSession> client;

    steady_clock::time_point last_seen {};
    std::string aws_log_stream {};
    bool lagging_worker_logged { false };

    std::optional<TreeletId> treelet {};
    std::set<SceneObject> objects {};

    TileId tile_id {};

    size_t outstanding_ray_bag_count { 0 };
    size_t outstanding_bytes { 0 };

    struct
    {
      uint64_t camera { 0 };
      uint64_t generated { 0 };
      uint64_t dequeued { 0 };

      uint64_t terminated { 0 };
      uint64_t enqueued { 0 };
      uint64_t accumulated { 0 };
    } ray_counters {};

    uint64_t active_rays() const
    {
      return ray_counters.camera + ray_counters.generated
             + ray_counters.dequeued - ray_counters.terminated
             - ray_counters.enqueued - ray_counters.accumulated;
    }

    // Statistics
    bool is_logged { true };
    WorkerStats stats {};
    WorkerStats last_stats {};

    std::vector<RayBagInfo> to_be_assigned {};
    bool marked_free { false };

    std::string to_string() const;

    static std::map<Role, size_t> active_count;
    static WorkerId next_id;
  };

  std::deque<Worker> workers {};
  const uint32_t max_workers;
  const uint32_t ray_generators;
  uint32_t finished_ray_generators { 0 };
  uint32_t accumulators;
  uint32_t started_accumulators { 0 };
  uint32_t initialized_workers { 0 };

  TileHelper tile_helper {};

  ////////////////////////////////////////////////////////////////////////////
  // Treelets                                                               //
  ////////////////////////////////////////////////////////////////////////////

  struct Treelet
  {
    TreeletId id;
    size_t pending_workers { 0 };
    std::set<WorkerId> workers {};
    std::pair<bool, TreeletStats> last_stats { true, {} };

    Treelet( const TreeletId treelet_id )
      : id( treelet_id )
    {
    }
  };

  size_t treelet_count {};
  std::vector<Treelet> treelets {};
  std::vector<TreeletStats> treelet_stats {};

  ////////////////////////////////////////////////////////////////////////////
  // Scheduler                                                              //
  ////////////////////////////////////////////////////////////////////////////

  /* this function is periodically called; it calls the scheduler,
     and if a new schedule is available, it executes it */
  void handle_reschedule();

  void handle_worker_invocation();

  void execute_schedule( const Schedule& schedule );

  /* requests invoking n workers */
  void invoke_workers( const size_t n );

  std::unique_ptr<Scheduler> scheduler;
  std::deque<TreeletId> treelets_to_spawn {};
  std::string invocation_payload {};

  ////////////////////////////////////////////////////////////////////////////
  // Worker <-> Object Assignments                                          //
  ////////////////////////////////////////////////////////////////////////////

  std::vector<SceneObject> list_base_objects() const;

  void assign_object( Worker& worker, const SceneObject& object );
  void assign_base_objects( Worker& worker );
  void assign_treelet( Worker& worker, Treelet& treelet );

  std::map<pbrt::ObjectType, std::string> alternative_object_names {};
  std::set<TreeletId> unassigned_treelets {};

  ////////////////////////////////////////////////////////////////////////////
  // Communication                                                          //
  ////////////////////////////////////////////////////////////////////////////

  /*** Messages *************************************************************/

  /* processes incoming messages; called by handleMessages */
  void process_message( const WorkerId worker_id,
                        const meow::Message& message );

  /*** Ray Bags *************************************************************/

  std::pair<bool, bool> assign_work( Worker& worker );

  void handle_queued_ray_bags();

  /* NOTE: in the following group of queues, the first N queues are for
  treelets, and the next M are for tiles (for accumulation) */

  /* ray bags that are going to be assigned to workers */
  std::vector<std::queue<RayBagInfo>> queued_ray_bags {};
  size_t queued_ray_bags_count { 0 };

  /* ray bags that there are no workers for them */
  std::vector<std::queue<RayBagInfo>> pending_ray_bags {};

  /* sample bags */
  std::vector<RayBagInfo> sample_bags {};

  void move_from_pending_to_queued( const TreeletId treelet_id );
  void move_from_queued_to_pending( const TreeletId treelet_id );

  ////////////////////////////////////////////////////////////////////////////
  // Stats                                                                  //
  ////////////////////////////////////////////////////////////////////////////

  WorkerStats aggregated_stats {};
  pbrt::AccumulatedStats pbrt_stats {};
  double estimated_cost { 0 };

  /*** Outputting stats *****************************************************/

  void record_enqueue( Worker& worker, const RayBagInfo& info );
  void record_assign( Worker& worker, const RayBagInfo& info );
  void record_dequeue( Worker& worker, const RayBagInfo& info );

  /* object for writing worker & treelet stats, and allocations */
  std::ofstream ws_stream {};
  std::ofstream tl_stream {};
  std::ofstream alloc_stream {};
  std::ofstream summary_stream {};

  /* write worker stats periodically */
  void handle_worker_stats();

  /* prints the status message every second */
  void handle_status_message();

  /*** Uploading progress ***************************************************/

  void handle_progress_report();

  std::unique_ptr<TransferAgent> progress_report_transfer_agent {};
  protobuf::ProgressReport progress_report_proto {};
  size_t current_report_id { 0 };
  WorkerStats last_reported_stats {};

  /*** Timepoints ***********************************************************/

  const steady_clock::time_point start_time { steady_clock::now() };
  steady_clock::time_point last_generator_done { start_time };
  steady_clock::time_point scene_initialization_done { start_time };
  steady_clock::time_point last_finished_ray { start_time };
  steady_clock::time_point last_action_time { start_time };
  steady_clock::time_point last_report_time { start_time };

  std::chrono::milliseconds last_workers_logged { 0 }; // relative to start_time

  ////////////////////////////////////////////////////////////////////////////
  // Scene Objects                                                          //
  ////////////////////////////////////////////////////////////////////////////

  /*** Scene Information ****************************************************/

  struct SceneData
  {
  public:
    static inline const std::vector<pbrt::ObjectType> base_object_types {
      pbrt::ObjectType::Manifest,      pbrt::ObjectType::Scene,
      pbrt::ObjectType::Camera,        pbrt::ObjectType::Lights,
      pbrt::ObjectType::AreaLights,    pbrt::ObjectType::Sampler,
      pbrt::ObjectType::InfiniteLights
    };

    pbrt::SceneBase base {};

    pbrt::Bounds2i sample_bounds {};
    pbrt::Vector2i sample_extent {};
    size_t total_paths { 0 };

    SceneData() {}
    SceneData( const std::string& scene_path,
               const int samples_per_pixel,
               const std::optional<pbrt::Bounds2i>& crop_window );
  } scene {};

  /*** Tiles ****************************************************************/

  class Tiles
  {
  public:
    pbrt::Bounds2i next_camera_tile();
    bool camera_rays_remaining() const;
    void send_worker_tile( Worker& worker, WorkerStats& aggregated_stats );

    Tiles() = default;
    Tiles( const int tile_size,
           const pbrt::Bounds2i& bounds,
           const long int spp,
           const uint32_t num_workers );

    int tile_size { 0 };

  private:
    std::vector<size_t> tiles_to_render {};
    pbrt::Bounds2i sample_bounds {};
    pbrt::Point2i n_tiles {};
    size_t cur_tile_idx { 0 };
    size_t tile_spp {};
  } tiles {};

  ////////////////////////////////////////////////////////////////////////////
  // Other Stuff                                                            //
  ////////////////////////////////////////////////////////////////////////////

  void terminate( const std::string& why );

  std::optional<EventLoop::RuleHandle> terminate_rule_handle {};

  EventLoop loop {};
  meow::Client<TCPSession>::RuleCategories worker_rule_categories;

  TCPSocket listener_socket {};
  SignalMask signals { SIGHUP, SIGTERM, SIGQUIT, SIGINT };
  SignalFD signal_fd { signals };

  void handle_signal( const signalfd_siginfo& sig );

  SSLContext ssl_context {};
  std::list<HTTPClient<SSLSession>> https_clients {};
  std::list<decltype( https_clients )::iterator> finished_https_clients {};
  HTTPClient<SSLSession>::RuleCategories https_rule_categories;

  /* Timers */
  TimerFD status_print_timer { STATUS_PRINT_INTERVAL };
  TimerFD worker_invocation_timer { WORKER_INVOCATION_INTERVAL,
                                    std::chrono::milliseconds { 1 } };
  TimerFD reschedule_timer { RESCHEDULE_INTERVAL,
                             std::chrono::milliseconds { 500 } };
  TimerFD worker_stats_write_timer { std::chrono::seconds { 1 },
                                     std::chrono::milliseconds { 1 } };

  TimerFD job_exit_timer { std::chrono::minutes { 15 } };
  TimerFD job_timeout_timer {};

  TimerFD progress_report_timer {};

  std::mt19937 rand_engine { std::random_device {}() };
};

} // namespace r2e2
