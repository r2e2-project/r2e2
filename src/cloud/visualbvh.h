#include "accelerators/bvh.h"
#include "cloud/bvh.h"
#include "pbrt.h"
#include "primitive.h"
#include <atomic>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>
namespace pbrt {

class VisualBVH : public BVHAccel {
  	public:
		VisualBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   int maxTreeletBytes,
                   int copyableThreshold,
                   bool rootBVH,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod =SplitMethod::SAH,
                   int depth_limit = 10,
                   const std::string &VisualBVHPath = "");
    private: 
    	bool rootBVH;
    	void VisualBVHSerializer(const std::string &VisualBVHPath, int depth_limit = 10);
    	void VisualBVHDepthLimitedTraversal(const uint32_t curr_idx,
    												   const int depth, 
    												   const int depth_limit,
    												   std::string &output);
    			
};
std::shared_ptr<VisualBVH> CreateVisualBVH(
    	std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);  
}