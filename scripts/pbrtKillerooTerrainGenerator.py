import numpy as np 
import matplotlib.pyplot as plt
import os
import sys
import functools
import operator
import math
from tqdm import tqdm
import argparse

def HillGenerator(x0,y0,x1,y1,r): # position of hill center,current position and hill radius
    z = r ** 2 - ((x1 - x0) ** 2 + (y1 - y0) ** 2)
    return z
def genHillTerrain(nx=64,ny=64,iters=440,seed=None):
	hill_terrain = np.zeros((nx,ny))
	if seed != None:
	    np.random.seed(seed)
	radii = np.random.randint(0,int(nx/2),iters)
	centers = nx * np.random.randn(iters,2)
	for k in tqdm(range(iters)):
	    radius = radii[k]
	    hill_center = centers[k]
	    accum_terrain = np.zeros((nx,ny)) 
	    x,y = np.meshgrid(range(0,nx),range(0,ny))
	    accum_terrain += HillGenerator(hill_center[0],hill_center[1],x,y,radius)
	    accum_terrain = np.clip(accum_terrain,a_min=0,a_max=None)
	    hill_terrain += accum_terrain

	hill_terrain = (2 * ((hill_terrain - np.min(hill_terrain))/(np.ptp(hill_terrain))) - 1) * 0.9
	hill_terrain = (hill_terrain ** 2  )
	return hill_terrain
def genLandPbrt(filename: str,hill_terrain):
    np.set_printoptions(threshold=sys.maxsize,suppress=True,precision=5)
    nx,ny = hill_terrain.shape
    f = open(filename, 'w')
    fmt_string = 'Shape "heightfield" "integer nu" [{}] "integer nv" [{}] "float Pz" [ '.format(np.int32(nx),
                                                                                                np.int32(ny))
    f.write(fmt_string)
    hill_terrain.flatten().tofile(f,sep=" ")
    f.write("]")
    f.close()
def parameter_numeric(param_name,value):
    if not isinstance(value,list):
        fmt_string =  "\"{}\" [{}]".format(param_name,value)
    else:
        fmt_string =  "\"{}\" ".format(param_name) + "[" +  " ".join([str(elem) for elem in value]) + "]"
    return fmt_string
def parameter_coordinate(value: list):
    if any(isinstance(elem, list) for elem in value):
        value = functools.reduce(operator.iconcat,value, [])
    fmt_str = [str(elem) for elem in value]
    return " ".join(fmt_str)
def parameter_string(value):
    fmt_string ="\"{}\"".format(value)
    return fmt_string
def Attribute_string(Attribute: str,parameter_strings: list = None):
    if parameter_strings != None:
        fmt_string = Attribute + " " + " ".join(parameter_strings) + "\n"
    else:
        fmt_string = Attribute + "\n"
    return fmt_string
def killeroo_string(height_coeff,
                    scale: list = None,
                    rotation: list = None,
                    translation: list = None,
                    color1: list = [.3,.3,.3],
                    color2: list = [.4,.5,.4],
                    roughness: float = .15,
                    is_instance = False,
                    instance_name = "killerooInstance",
                    killeroo_path ="geometry/killeroo.pbrt" ):
    
    fmt_string = Attribute_string("AttributeBegin")
    
    #base scale
    fmt_string += Attribute_string("Scale",[parameter_coordinate([0.01,0.01,0.01])])
    if scale != None:
        if isinstance(scale,list):
            fmt_string += Attribute_string("Scale",[parameter_coordinate(scale)])
        else:
            fmt_string += Attribute_string("Scale",[parameter_coordinate([scale,scale,scale])])
    
    
    #base rotation
    fmt_string += Attribute_string("Rotate",[parameter_coordinate([90,-1 ,0,0])])
    
    if translation != None:
        fmt_string += Attribute_string("Translate",[parameter_coordinate(translation)])
    
    if rotation != None:
        fmt_string += Attribute_string("Rotate",[parameter_coordinate([rotation[0],1,0,0])])
        fmt_string += Attribute_string("Rotate",[parameter_coordinate([rotation[1],0,1,0])])
        fmt_string += Attribute_string("Rotate",[parameter_coordinate([rotation[2],0,0,1])])
    
    #base translation
    fmt_string += Attribute_string("Translate",[parameter_coordinate([0,0,
                                            140])])
    if is_instance == False:
        fmt_string += Attribute_string("Material",[parameter_string("plastic"),
                                                   parameter_numeric("color Kd",color1),
                                                   parameter_numeric("color Kd",color2),
                                                   parameter_numeric("float roughness",roughness)])
        fmt_string += Attribute_string("Include",[parameter_string(killeroo_path)])
    if is_instance == True:
        fmt_string += Attribute_string("ObjectInstance",[parameter_string(instance_name)])
    fmt_string += Attribute_string("AttributeEnd")
            
    return fmt_string
def genkKillerooObj(killeroo_path="geometry/killeroo.pbrt",
                    color1: list = [.7,.3,.3],
                    color2: list = [.7,.2,.1],
                    roughness: float = .15,
                    name = "killerooInstance"):
    fmt_string = Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("ObjectBegin",[parameter_string(name)])
    fmt_string += Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("Material",[parameter_string("plastic"),
                                               parameter_numeric("color Kd",color1),
                                               parameter_numeric("color Kd",color2),
                                               parameter_numeric("float roughness",roughness)])
    fmt_string += Attribute_string("Include",[parameter_string("geometry/killeroo.pbrt")])
    fmt_string += Attribute_string("AttributeEnd")
    fmt_string +=  Attribute_string("ObjectEnd")
    fmt_string += Attribute_string("AttributeEnd\n")
    return fmt_string
    

def genKilleroo(m,n,i,j,
                k_coeff,
                l_scale_coeff,
                height_coeff,
                hill_terrain,
                rotate_val = None,
                is_instance = False,
                killeroo_path = "geometry/killeroo.pbrt"):
    #ensure that indicies are never at the edge of heightmap to exclude boundary cases
    #for inward facing normals
    eps = 1
    maxu = (10 * 5)/k_coeff * l_scale_coeff
    minu = (10 * -5)/k_coeff * l_scale_coeff
    
    #base coord
    tx = (10 * i)/k_coeff * l_scale_coeff
    ty = (10 * j)/k_coeff * l_scale_coeff
    
    u = (m - 1) * (abs(tx - minu)/abs(maxu - minu))
    v = (n - 1) * (abs(ty - minu)/abs(maxu - minu))
    
    #coordinates for u vector
    tx1 = (10 * (i + eps))/k_coeff * l_scale_coeff  
    u1 = (m - 1) * (abs(tx1 - minu)/abs(maxu - minu))
    v1 = v
    
    #coordinates for v vector
    ty2 = (10 * (j + eps))/k_coeff * l_scale_coeff
    u2 = u
    v2 = (n - 1) * (abs(ty2 - minu)/abs(maxu - minu))
    

    #extract height values
    h_val = hill_terrain[int(v),int(u)]
    h_uval = hill_terrain[int(v1),int(u1)]
    h_vval = hill_terrain[int(v2),int(u2)]
    
    tz = (height_coeff * (100/k_coeff )) * h_val 
    tz1 = (height_coeff * (100/k_coeff)) * h_uval
    tz2 = (height_coeff * (100/k_coeff)) * h_uval
    
    #coordinate for original height field index
    
    theta_x = np.degrees(np.arctan2((tz2 - tz),(ty2 - ty)))
    theta_y = np.degrees(np.arctan2((tz1 - tz),(tx1 - tx)))
    
    #rotate about z axis
    theta_z = np.random.uniform(low=0,high=360)
    if rotate_val != None: 
        theta_z = rotate_val
        
    fmt_string = killeroo_string(height_coeff,
                                    scale = k_coeff,
                                    translation=[tx,ty,tz],
                                    rotation = [-theta_x,-theta_y,theta_z],
                                    is_instance=is_instance,
                                    killeroo_path=killeroo_path)
    return fmt_string
def genCamera(LookAt,fov,xres,yres,output_name):
    fmt_string = Attribute_string("LookAt",[parameter_coordinate(LookAt[0]),
                                            parameter_coordinate(LookAt[1]),
                                            parameter_coordinate(LookAt[2])])
    fmt_string += Attribute_string("Camera",[parameter_string("perspective"),
                                             parameter_numeric("float fov",fov)])
    fmt_string += Attribute_string("Film",[parameter_string("image"),
                                           parameter_numeric("integer xresolution",xres),
                                           parameter_numeric("integer yresolution",yres),
                                           parameter_string("string filename"),
                                           parameter_string(output_name)])
    return fmt_string

def genSampler(samples):
    fmt_string = Attribute_string("Sampler", [parameter_string("halton"),
                                               parameter_numeric("integer pixelsamples",samples)])
    fmt_string += Attribute_string("Integrator",[parameter_string("path")])
    
    return fmt_string
def genAboveLight():
    fmt_string = Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("Material",[parameter_string("matte"),
                                               parameter_numeric("color Kd",[0,0,0])])
    fmt_string += Attribute_string("Translate",[parameter_coordinate([0,1000,0])])
    fmt_string += Attribute_string("AreaLightSource",[parameter_string("area"),
                                                      parameter_numeric("color L",[500,500,500]),
                                                      parameter_numeric("integer nsamples",[16])])
    fmt_string += Attribute_string("Shape",[parameter_string("trianglemesh"),
                                            parameter_numeric("integer indices",[0,1,2,0,2,3]),
                                            parameter_numeric("point P",[-60,0,-60,60,0,-60,60,0,60,-60,0,60]),
                                            parameter_numeric("float st",[0,0,1,0,1,1,0,0,1])])
    fmt_string += Attribute_string("AttributeEnd\n")  
    return fmt_string
def genKillerooTerrain(output_filename: str,
                       xres: int,
                       yres: int,
                       landxres: int,
                       landyres: int,
                       landiters: int,
                       LookAt: list = [[0,300,-300],[0,0,0],[0,1,0]],
                       l_scale_coeff: int = 100,
                       height_coeff: int = 30,
                       k_coeff: int = 10,
                       num_killeroos_unique = 25,
                       num_killeroos_instance =25,
                       random_seed: int = None,
                       killeroo_path: str = "geometry/killeroo.pbrt" ,
                       land_filename: str = "./geometry/gen_killeroo_land.pbrt",
                       killeroos_filename: str = "./geometry/gen_killeroo_geometry.pbrt"):
    f = open(output_filename, 'w')
    assert os.path.exists(killeroo_path), "killeroo geometry file not found at: {}".format(killeroo_path)
    #Generate land 
    hill_terrain = [];
    if landiters > 0:
        hill_terrain = genHillTerrain(nx=landxres,ny=landyres,iters=landiters,seed=random_seed)
    else:
        if random_seed != None:
            np.random.seed(random_seed)
        hill_terrain = np.ones((landxres,landyres)) * 0.3
    
    #Camera,Sampling, and Integrator parameters
    fmt_string = genCamera(LookAt,50,xres,yres,output_filename[:output_filename.rfind(".")] + ".exr" )
    fmt_string += genSampler(8)
    fmt_string += Attribute_string("WorldBegin\n")
    
    #Above square trianglemesh light source
    fmt_string += genAboveLight()
    
    #Camera light source 
    fmt_string += Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("CoordSysTransform",[parameter_string("camera")])
    fmt_string += Attribute_string("LightSource",[parameter_string("point"),
                                                  parameter_numeric("color I",[15,15,15])])

    fmt_string += Attribute_string("AttributeEnd\n") 
    
    #Land 
    genLandPbrt(land_filename,hill_terrain);
    fmt_string += Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("Material",[parameter_string("matte"),
                                        parameter_numeric("color Kd",[.4,.2,.1])])
    fmt_string += Attribute_string("Translate",[parameter_coordinate([-l_scale_coeff//2,0,l_scale_coeff//2])])
    fmt_string += Attribute_string("Rotate",[parameter_coordinate([90,-1,0,0])])
    fmt_string += Attribute_string("Scale",[parameter_coordinate([l_scale_coeff,l_scale_coeff,height_coeff])])
    fmt_string += Attribute_string("Include",[parameter_string(land_filename)])
    fmt_string += Attribute_string("AttributeEnd\n") 
     
    #killerooObj gen
    fmt_string += genkKillerooObj(killeroo_path)
    fmt_string += Attribute_string("Include",[parameter_string(killeroos_filename)])
    fmt_string += Attribute_string("WorldEnd")
    f.write(fmt_string)
    
    #kileroo gen
    t = open(killeroos_filename,'w')
    m,n = hill_terrain.shape
    nearby_dict = {}
    near_eps = (2/k_coeff) *  1.25 * (100/l_scale_coeff)
    for k in tqdm(range(int(num_killeroos_unique + num_killeroos_instance))):
        i = np.random.uniform(low=-4,high=4)
        j = np.random.uniform(low=-4,high=4)
        rotate_val = None
        if (i,j) not in nearby_dict:
            nearby_dict[(i,j)] = np.random.uniform(low=0,high=360)
        
        for key in nearby_dict:
            distance = np.sqrt((i - key[0]) ** 2 + (j - key[1]) ** 2 )
            if abs(distance) < near_eps:
                rotate_val = nearby_dict[key]
                break
        if k < num_killeroos_unique:
            t.write(genKilleroo(m,n,i,j,k_coeff,l_scale_coeff,height_coeff,hill_terrain,rotate_val,False,killeroo_path))
        else:
            t.write(genKilleroo(m,n,i,j,k_coeff,l_scale_coeff,height_coeff,hill_terrain,rotate_val,True,killeroo_path))
def main():
	parser = argparse.ArgumentParser(description=("'Generate a parameterized pbrt file of killeroos on hilly terrain"))
	scale_step = 10
	prop = .1
	parser.add_argument('--xres',default =700,help=("x resolution target of pbrt file"))
	parser.add_argument('--yres',default =700,help=("y resolution target of pbrt file"))
	parser.add_argument('--landiters',default =440,help=("number of iterations hill generator should take"))
	parser.add_argument('--k_coeff',default =2,help=("scale of killeroos "))
	parser.add_argument('--LookAt',default =[[0,300,-300],[0,0,0],[0,1,0]],
						help=("LookAt params for pbrt camera"))
	parser.add_argument('--killeroo_path',default="./geometry/killeroo.pbrt",
	                    help=(' input file path for kilerooo pbrt file'
	                        ))
	parser.add_argument('--output_path', default="killeroo_terrain.pbrt",
	                    help=('output file path for main pbrt file'
	                        ))
	parser.add_argument('--output_land_path', default="./geometry/gen_killeroo_land.pbrt",
	                    help=('output file path for hill terrain'
	                        ))
	parser.add_argument('--output_geometry_path', default="./geometry/gen_killeroo_geometry.pbrt",
	                    help=('output file path for generated killeroos'
	                        ))
	parser.add_argument('--num_killeroos_unique',default =(100 * prop) * scale_step)
	parser.add_argument('--num_killeroos_instance',default =100 * (1-prop) * scale_step)
	parser.add_argument('--landxres',default =100 * scale_step,help=("resolution of heightmap x"))
	parser.add_argument('--landyres',default =100 * scale_step,help=("resolution of heightmap y"))
	parser.add_argument('--land_scale',default =100 * scale_step,help=("scale of physical width and breadth of heightmap"))
	parser.add_argument('--height_scale',default =30 * scale_step,help=("scale of physical height of heightmap"))
	parser.add_argument('--random_seed',default =2)

	args = parser.parse_args()
	genKillerooTerrain("killeroo_terrain.pbrt",
	                   xres=int(args.xres),
	                   yres=int(args.yres),
	                   landxres =int(args.landxres),
	                   landyres = int(args.landyres),
	                   landiters=int(args.landiters),
	                   LookAt= args.LookAt,
	                   l_scale_coeff=float(args.land_scale),
	                   height_coeff=float(args.height_scale),
	                   k_coeff=float(args.k_coeff),
	                   num_killeroos_unique=int(args.num_killeroos_unique),
	                   num_killeroos_instance=int(args.num_killeroos_instance),
	                   random_seed=int(args.random_seed),
	                   killeroo_path=args.killeroo_path,
	                   land_filename=args.output_land_path,
                   	   killeroos_filename=args.output_geometry_path)
if __name__ == '__main__':
	main()
