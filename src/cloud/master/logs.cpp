#include "cloud/lambda-master.h"

#include <iomanip>

#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;

void LambdaMaster::dumpJobSummary() const {
    protobuf::JobSummary proto;

    proto.set_total_time(
        duration_cast<milliseconds>(lastFinishedRay - startTime).count() /
        1000.0);

    proto.set_launch_time(
        duration_cast<milliseconds>(generationStart - allToAllConnectStart)
                .count() /
            1000.0 +
        duration_cast<milliseconds>(allToAllConnectStart - startTime).count() /
            1000.0);

    proto.set_ray_time(
        duration_cast<milliseconds>(lastFinishedRay - generationStart).count() /
        1000.0);

    proto.set_num_lambdas(numberOfLambdas);
    proto.set_total_paths(totalPaths);
    proto.set_finished_paths(workerStats.finishedPaths());
    proto.set_finished_rays(workerStats.finishedRays());

    ofstream fout{config.jobSummaryPath};
    fout << protoutil::to_json(proto) << endl;
}

void LambdaMaster::printJobSummary() const {
    const static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

    cerr << "* Job summary: " << endl;
    cerr << "  >> Average ray throughput: "
         << (1.0 * workerStats.finishedRays() / numberOfLambdas /
             duration_cast<seconds>(lastFinishedRay - generationStart).count())
         << " rays/core/s" << endl;

    cerr << "  >> Total run time: " << fixed << setprecision(2)
         << (duration_cast<milliseconds>(lastFinishedRay - startTime).count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Launching lambdas & downloading the scene: " << fixed
         << setprecision(2)
         << (duration_cast<milliseconds>(allToAllConnectStart - startTime)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Making all-to-all connections: " << fixed
         << setprecision(2)
         << (duration_cast<milliseconds>(generationStart - allToAllConnectStart)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Tracing rays: " << fixed << setprecision(2)
         << (duration_cast<milliseconds>(lastFinishedRay - generationStart)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "  >> Estimated cost: $" << fixed << setprecision(2)
         << (LAMBDA_UNIT_COST * numberOfLambdas *
             ceil(duration_cast<milliseconds>(lastFinishedRay - startTime)
                      .count() /
                  1000.0))
         << endl;

    cerr << endl;
}
