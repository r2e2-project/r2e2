#include "cloud/visualbvh.h"
#include "cloud/bvh.h"
#include "paramset.h"
#include "stats.h"
#include "messages/utils.h"
#include <fstream>
#include <iostream>
#include <string>
#include <iomanip>
#include <algorithm>

#include "pbrt.pb.h"

using namespace std;
namespace pbrt{
VisualBVH::VisualBVH(vector<shared_ptr<Primitive>> &&p,
                   int maxTreeletBytes,
                   int copyableThreshold,
                   bool rootBVH,
                   int maxPrimsInNode,
                   SplitMethod splitMethod,
                   int depth_limit,
                   const string &VisualBVHPath):
                   BVHAccel(p, maxPrimsInNode, splitMethod),
          		   rootBVH(rootBVH){

    VisualBVHSerializer(VisualBVHPath,depth_limit);

}


void VisualBVH::VisualBVHSerializer(const string &VisualBVHPath,int depth_limit){
	string output = "";
	if(VisualBVHPath !=""){
		VisualBVHDepthLimitedTraversal(0,0,depth_limit,output);
		ofstream file(VisualBVHPath,ofstream::out);
		file << "[";
		file << output;
		file << "]";

	}
	else{
		VisualBVHDepthLimitedTraversal(0,0,depth_limit,output);
		cout << "[";
		cout << output;
		cout << "]";
	}

}
string toString(const Bounds3f &bounds) {
    ostringstream oss;

    oss << fixed << setprecision(3) << "[[" << bounds.pMin.x << ','
        << bounds.pMin.y << ',' << bounds.pMin.z << "],[" << bounds.pMax.x
        << ',' << bounds.pMax.y << ',' << bounds.pMax.z << "]]";

    return oss.str();
}

void VisualBVH::VisualBVHDepthLimitedTraversal(const uint32_t curr_idx,
											   const int depth,
											   const int depth_limit,
											   string &output){
	if (depth == depth_limit) {
        return;
    }
    // save the current node to string output
    const LinearBVHNode &curr_node = nodes[curr_idx];
    output += "[";
    output += "node_idx: " + to_string(curr_idx) + ",";
    output += "Bounds: " + toString(curr_node.bounds) + ",";
    output += "2COffset: " + to_string(curr_node.secondChildOffset) + ",";
    output += "depth: " + to_string(depth);
    output += "],\n";
    
    // go to left value, if there's one
    if(curr_node.nPrimitives == 0){
	    if (curr_idx + 1 < nodeCount) {
	        VisualBVHDepthLimitedTraversal(curr_idx + 1, depth + 1, depth_limit, output);
	    }

	    // curr_node = nodes[curr_idx];

	    // go to right value, if there's one
	    if (curr_node.secondChildOffset != 0) {
	        VisualBVHDepthLimitedTraversal(curr_node.secondChildOffset, depth + 1, depth_limit, output);
	    }
	}
}

shared_ptr<VisualBVH> CreateVisualBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps){
	int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);
    int copyableThreshold = ps.FindOneInt("copyablethreshold", maxTreeletBytes / 2);
    bool rootBVH = ps.FindOneBool("sceneaccelerator", false);
    string splitMethodName = ps.FindOneString("splitmethod", "sah");
	BVHAccel::SplitMethod splitMethod;
	if (splitMethodName == "sah")
	 	splitMethod = BVHAccel::SplitMethod::SAH;
	else if (splitMethodName == "hlbvh")
	    splitMethod = BVHAccel::SplitMethod::HLBVH;
	else if (splitMethodName == "middle")
	    splitMethod = BVHAccel::SplitMethod::Middle;
	else if (splitMethodName == "equal")
	    splitMethod = BVHAccel::SplitMethod::EqualCounts;
	else {
	    Warning("BVH split method \"%s\" unknown.  Using \"sah\".",
	            splitMethodName.c_str());
	    splitMethod = BVHAccel::SplitMethod::SAH;
	}
	int maxPrimsInNode = ps.FindOneInt("maxnodeprims", 4);
	int depth_limit = ps.FindOneInt("depth_limit",10);
	const string VisualBVHPath = ps.FindOneString("visualbvhpath", "");
	if (VisualBVHPath != "") {
            return make_shared<VisualBVH>(move(prims), maxTreeletBytes,
                                               copyableThreshold, rootBVH,
                                               maxPrimsInNode, splitMethod,
                                               depth_limit,
 											   VisualBVHPath);
        } else {
            return make_shared<VisualBVH>(move(prims), maxTreeletBytes,
                                               copyableThreshold, rootBVH,
                                               maxPrimsInNode, splitMethod,
                                               depth_limit,
                                               VisualBVHPath);
}
}



}