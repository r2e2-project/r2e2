import numpy as np 
from scipy import ndimage
import os
from pathlib import Path
from tempfile import mkdtemp
import matplotlib.pyplot as plt 
import sys
import functools
import operator
import math
from tqdm import tqdm
import argparse
import multiprocessing
from joblib import Parallel,delayed
import noise
def divide_chunks(l, n): 
      
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 
  
def blockshaped(arr, nrows, ncols):
    """
    Return an array of shape (n, nrows, ncols) where
    n * nrows * ncols = arr.size

    If arr is a 2D array, the returned array looks like n subblocks with
    each subblock preserving the "physical" layout of arr.
    """
    h, w = arr.shape
    return (arr.reshape(h//nrows, nrows, -1, ncols)
               .swapaxes(1,2)
               .reshape(-1, nrows, ncols))

def HillGenerator(x0,y0,x1,y1,r): # position of hill center,current position and hill radius
    return r ** 2 - ((x1 - x0) ** 2 + (y1 - y0) ** 2)
def genHillTerrainPerlin(nx,ny):
    scale = 100
    octaves = 5
    persistence = 0.5
    lacunarity = 2.0
    hill_terrain = np.zeros((nx,ny))
    for i in tqdm(range(nx)):
        for j in range(ny):
            hill_terrain[i][j] = noise.pnoise2(i/scale, 
                                        j/scale, 
                                        octaves=octaves, 
                                        persistence=persistence, 
                                        lacunarity=lacunarity, 
                                        repeatx=1024, 
                                        repeaty=1024, 
                                        base=2)
    return hill_terrain
def genHillTerrain(nx,ny,x,y,radii,centers,iters):
    filename = os.path.join(mkdtemp(),'newfile.dat')
    hill_terrain = np.memmap(filename,dtype='float32',mode='w+',shape =(nx,ny))
    num_cores = multiprocessing.cpu_count()
    # hill_terrain = np.zeros((nx,ny))
    n = int(10)
    l = int(100)
    radii = radii.tolist()
    centers = centers.tolist()
    for k in range(iters):
        radius = radii[k]
        hill_center = centers[k]
        accum_terrain = np.zeros((nx,ny))
        accum_terrain = HillGenerator(hill_center[0],hill_center[1],x,y,radius)
        accum_terrain = np.clip(accum_terrain,a_min=0,a_max=None)
        hill_terrain += accum_terrain    
    # plt.imshow(hill_terrain)
    # plt.show()
    return hill_terrain
def erodeTerrainThermal(hill_terrain,iters):
    c = 0.5
    T = 1e-3
    m,n = hill_terrain.shape
    for j in tqdm(range(iters)):
        # d1 = np.roll(hill_terrain,1,0) - hill_terrain 
        # d2 = np.roll(hill_terrain,1,1) - hill_terrain 
        # d3 = np.roll(hill_terrain,-1,1) - hill_terrain
        # d4 = np.roll(hill_terrain,-1,0) - hill_terrain 
        # di = np.dstack(d1,d2,d3,d4)
        # d_tot = np.sum(np.where(di < T,di,0),axis=2)
        # d_max = np.amax(di,axis=2)
        # for i in range(di.shape[2]):

        for x in range(1,m - 1):
            for y in range(1,n - 1):
                h = hill_terrain[x,y]
                xyi = [(x - 1,y),(x,y - 1),(x,y + 1),(x + 1,y)]
                di = [h - hill_terrain[x - 1,y],
                      h - hill_terrain[x,y - 1],
                      h - hill_terrain[x,y + 1],
                      h - hill_terrain[x + 1,y]]
                d_tot = sum([ val  for val in di if val > T])
                d_max = max(di)
                # print(di)
                for i in range(len(xyi)):
                    if di[i] > T:
                        # print("hit this")
                        hill_terrain[xyi[i]] = hill_terrain[xyi[i]] + c * (d_max - T) * (di[i]/d_tot)
                        hill_terrain[x,y] = hill_terrain[x,y] - (c * (d_max - T) * (di[i]/d_tot))
    return hill_terrain
def erodeTerrainDroplet(hill_terrain,iters=100,seed = None):
    if seed != None:
        np.random.seed(seed)
    Kq=10
    Ki = 0.1
    Kw=0.001
    Kr=0.9
    Kd=0.02
    minSlope=0.05
    g=20
    Kg=g*2
    eps = 1e-2
    m,n = hill_terrain.shape
    print("eroding terrain...")
    print("sum hill terrain: {} ".format(np.sum(hill_terrain)))
    print("max hill terrain: {}".format(np.amax(hill_terrain)))
    print("min hill terrain: {}".format(np.amin(hill_terrain)))
    h_0 = np.copy(hill_terrain[0,:])
    h_1 = np.copy(hill_terrain[-1,:])
    h_2 = np.copy(hill_terrain[:,0])
    h_3 = np.copy(hill_terrain[:,-1])
    #guards to block sediment from being placed on edges and building spikes 
    hill_terrain[0,:] = 0.99 * np.ones(m)
    hill_terrain[-1,:] = 0.99 * np.ones(m)
    hill_terrain[:,0] = 0.99 * np.ones(n)
    hill_terrain[:,-1] = 0.99 * np.ones(n)
    max_path_length = 4 * m 

    def deposit_at(x,y,W,ds):
        delta = ds * W
        hill_terrain[x,y] = hill_terrain[x,y] +  delta;
    def ERODE(x,y,W,ds):
        delta=ds*(W);
        hill_terrain[x,y] = hill_terrain[x,y] - delta;
    def DEPOSIT(H,xi,zi,xf,zf,ds):
        deposit_at(xi  , zi  , 0.25,ds) 
        deposit_at(xi+1, zi  , 0.25,ds) 
        deposit_at(xi  , zi+1, 0.25,ds) 
        deposit_at(xi+1, zi+1, 0.25,ds)
        H += ds 
        return H
    
    for i in tqdm(range(iters)):
        # - 2 to not have to deal with boundary effects 
        # also, whole of terrain not in use for killeroos in any case
        xi = int(np.random.uniform(1,m - 2))
        zi = int(np.random.uniform(1,n - 2))

        xp = xi; zp = zi
        xf = 0; zf = 0 
        h = np.copy(hill_terrain[xi,zi]);
        s = 0; v = 0; w = 1;
        h00 = h
        h10 = hill_terrain[xi + 1, zi]
        h01 = hill_terrain[xi, zi + 1]
        h11 = hill_terrain[xi + 1, zi + 1]
        dx = 0; dz = 0
        for numMoves in range(max_path_length):
            # grad calc
            gx=h00+h01-h10-h11;
            gz=h00+h10-h01-h11;

            # calculate next position
            dx=(dx-gx)*Ki+gx;
            dz=(dz-gz)*Ki+gz;

            dl=math.sqrt(dx*dx+dz*dz)
            if dl <= eps: 
                a = np.random.uniform(0,1) * 2 * np.pi
                dx=np.cos(a);
                dz=np.sin(a);
            else:
                dx/=dl;
                dz/=dl;

            nxp=xp+dx;
            nzp=zp+dz; 

            # sample next height (clamped to ensure no out of array vals)
            nxi=int(math.floor(nxp))
            nzi=int(math.floor(nzp));
            nxf=nxp-nxi;
            nzf=nzp-nzi;

            #new height and nearby vals 
            nh00=hill_terrain[nxi  , nzi  ];
            nh10=hill_terrain[nxi+1, nzi  ];
            nh01=hill_terrain[nxi  , nzi+1];
            nh11=hill_terrain[nxi+1, nzi+1];

            nh=(nh00*0.25+nh10*0.25)+(nh01*0.25+nh11*0.25)
            # if higher than current, try to deposit sediment up to neighbour height
            if nh >= h: 
                ds=(nh-h)
                if (ds>=s):
                # deposit all sediment and stop
                    ds=s;
                    h  = DEPOSIT(h,xi,zi,xf,zf,ds)
                    s=0;
                    break;
                h = DEPOSIT(h,xi,zi,xf,zf,ds)
                s -= ds
                v = 0
            # compute transport capacity
            dh=h-nh
            slope=dh
            q=max(slope, minSlope)*v*w*Kq;

            # deposit/erode (don't erode more than dh)
            ds=s-q;
            if ds >= 0:
                # deposit 
                ds*=Kd;
                dh = DEPOSIT(dh,xi,zi,xf,zf,ds)
                # dh += ds
                # DEPOSIT(dh,xi,zi,xf,zf,ds)
                s-=ds;
            else:
                # erode 
                ds*=-Kr;
                ds=min(ds, dh*0.99);
                w_sum  = 0 
                ERODE(xi  , zi  , 0.25,ds)
                ERODE(xi+1, zi  , 0.25,ds)
                ERODE(xi  , zi+1, 0.25,ds)
                ERODE(xi+1, zi+1, 0.25,ds)
                # for z in range(zi-1,min(zi+3,m - 1)):
                #     # z = min(z,n - 1)
                #     zo=z-zp;
                #     zo2=zo*zo;
                #     for x in range(xi-1,min(xi+3,n - 1)):
                #         # x = min(x,m - 1)
                #         xo=x-xp;
                #         w=1-(xo*xo+zo2)*0.25
                #         if (w<=0):
                #             continue;
                #         w*=0.1591549430918953;
                #         w_sum += w 
                        # ERODE(x, z, w,ds)
                # print(w_sum)
                dh-=ds;
                assert(dh >= 0)
                # print(dh)
                s+=ds;
            # move to the neighbour
            dh = max(dh,0)
            v=math.sqrt(v*v+Kg*dh);
            w*=1-Kw;
            xp=nxp; zp=nzp;
            xi=nxi; zi=nzi;
            xf=nxf; zf=nzf;
            h=nh;
            h00=nh00;
            h10=nh10;
            h01=nh01;
            h11=nh11;
        # print(numMoves)
    # guards removed and replaced with previous edge value
    hill_terrain[0,:] = h_0
    hill_terrain[-1,:] = h_1
    hill_terrain[:,0] = h_2
    hill_terrain[:,-1] = h_3
    print("sum hill terrain: {} ".format(np.sum(hill_terrain)))
    print("max hill terrain: {}".format(np.amax(hill_terrain)))
    print("min hill terrain: {}".format(np.amin(hill_terrain)))

    hill_terrain = ndimage.gaussian_filter(hill_terrain,2)
    return hill_terrain

def loadFromLand(land_filename,x,y):
    f = open(land_filename,"r")
    line= f.readline()
    arr_line = np.fromstring(line[line.rfind("[")+1:line.rfind("]")],sep=' ')
    # print(arr_line)
    # heightfield = np.array(list(arr_line),dtype=float)
    heightfield = np.reshape(arr_line,(x,y))
    return heightfield
'''
instead of simply chunking input terrain, we will now generate all of that terrain in 
genChunkTerrain and write to disk immediately 
'''

def genChunkTerrain(nx,ny,num_chunks,iters,seed,l_scale_coeff,height_coeff,land_filename,output_filename):
        num_cores = multiprocessing.cpu_count()
        # resolution of each subset 
        subset_res_x = nx//num_chunks
        subset_res_y = ny//num_chunks
        print("chunk resolution: ({},{})".format(subset_res_x,subset_res_y))
        if seed != None:
            np.random.seed(seed)

        radii = np.random.randint(0,int(nx/2),iters)
        centers = nx * np.random.randn(iters,2)

        s_scale_coeff =  l_scale_coeff / num_chunks
        fmt_strings = []
        #for every chunk
        def genHillSubset(i):
            x_index = (i % num_chunks) * subset_res_x 
            y_index = (i // num_chunks) * subset_res_y 
            meshx,meshy = np.meshgrid(range(x_index,x_index + subset_res_x),
                                      range(y_index,y_index + subset_res_y))
            # note this is unnormalized terrain, will need to go back in with
            # global max and min to normalize and apply postprocessing
            hill_terrain_subset =  genHillTerrain(subset_res_x,
                                                  subset_res_y,
                                                  meshx,meshy,radii,
                                                  centers,iters)
            subset_filename = land_filename + str(i) + ".pbrt"
            genLandPbrt(subset_filename,hill_terrain_subset);
            #calculate max and min for future use
            return [np.min(hill_terrain_subset),np.max(hill_terrain_subset)]
        #parallel calculation of chunks
        max_min_list = Parallel(n_jobs=num_cores)(delayed(genHillSubset)(i) for i in tqdm(range(int(num_chunks ** 2 ))))
        max_hill = np.max(max_min_list)
        min_hill = np.min(max_min_list)
        for i in tqdm(range(int(num_chunks ** 2 ))):
            #load land back in from text file
            subset_filename = land_filename + str(i) + ".pbrt"
            hill_terrain_subset =  loadFromLand(subset_filename,subset_res_x,subset_res_y)
            #apply global normalization + post processsing
            hill_terrain_subset = (2 * ((hill_terrain_subset - min_hill)/((max_hill-min_hill))) - 1) * 0.99
            hill_terrain_subset = (hill_terrain_subset ** 2  )
            #rewrite back to textfile
            genLandPbrt(subset_filename,hill_terrain_subset);

            #for inclusion into chunk master 
            fmt_string = ""
            fmt_string += Attribute_string("AttributeBegin")
            fmt_string += Attribute_string("Material",[parameter_string("matte"),
                                                parameter_numeric("color Kd",[.4,.2,.1])])
            # translate to terrain space 
            fmt_string += Attribute_string("Translate",[parameter_coordinate([-(s_scale_coeff * num_chunks)//2,0,(s_scale_coeff * num_chunks )//2])])
            # translate within terrain space
            y = i //num_chunks;
            x = i % num_chunks; 
            fmt_string += Attribute_string("Translate",[parameter_coordinate([(s_scale_coeff * x),0,-(s_scale_coeff * y )])])
            fmt_string += Attribute_string("Rotate",[parameter_coordinate([90,-1,0,0])])
            fmt_string += Attribute_string("Scale",[parameter_coordinate([s_scale_coeff,s_scale_coeff,height_coeff])])
            fmt_string += Attribute_string("Include",[parameter_string(os.path.relpath(subset_filename,
                                                                        os.path.dirname(output_filename)))])
            fmt_string += Attribute_string("AttributeEnd\n") 
            fmt_strings.append(fmt_string)
        return fmt_strings
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

def killeroo_string_object(instance_name: str = "killerooInstance",
                           color1: list = [.3,.3,.3],
                           color2: list = [.4,.5,.4],
                           roughness: float = .15,
                           killeroo_path: str = "geometry/killeroo.pbrt"):
    fmt_string = Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("ObjectBegin",[parameter_string(instance_name)])
    fmt_string += Attribute_string("Material",[parameter_string("plastic"),
                                                   parameter_numeric("color Kd",color1),
                                                   parameter_numeric("color Kd",color2),
                                                   parameter_numeric("float roughness",roughness)])
    # fmt_string += Attribute_string("Include",[parameter_string(killeroo_path)])
    fmt_string += Attribute_string("Shape",[parameter_string("plymesh"),
                                            parameter_string("string filename"),
                                            parameter_string(killeroo_path)])
    fmt_string +=  Attribute_string("ObjectEnd")
    fmt_string += Attribute_string("AttributeEnd")
    return fmt_string

def killeroo_string_instance(height_coeff,
                    scale: list = None,
                    rotation: list = None,
                    translation: list = None,
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
    fmt_string += Attribute_string("ObjectInstance",[parameter_string(instance_name)])
    fmt_string += Attribute_string("AttributeEnd")
            
    return fmt_string
    

def genKillerooInstance(i,j,num_chunks,
                land_idx,
                subset_hill_terrain,
                k_coeff,
                l_scale_coeff,
                height_coeff,
                rotate_val,
                instance_name = "killerooInstance",
                killeroo_path = "geometry/killeroo.pbrt"):
    #ensure that indicies are never at the edge of heightmap to exclude boundary cases
    #for inward facing normals

    eps = 1
    i1 = i + 1
    j1 = j + 1

    subset_res_x,subset_res_y = subset_hill_terrain.shape 
    landxres,landyres = (subset_res_x * num_chunks,subset_res_y * num_chunks)

    land_x = (land_idx % num_chunks) * subset_res_x 
    land_y = (land_idx // num_chunks) * subset_res_y 
    

    #physical extent of land in x and y 
    maxu =  maxv = ((10 * 5)/k_coeff) * l_scale_coeff
    minu =  minv = ((10 * -5)/k_coeff) * l_scale_coeff
    
    #position in total hill terrain
    position_x = land_x + i
    position_y = land_y + j

    position_x1 = land_x + i1
    position_y1 = position_y

    position_x2 = position_x 
    position_y2 = land_y + j1
    #position of new killeroo in terms of physical extent
    extent_space_x = (position_x/landxres) * 10 - 5
    extent_space_y = (position_y/landyres) * 10 - 5

    extent_space_x1 = (position_x1/landxres) * 10 - 5
    extent_space_y2 = (position_y2/landyres) * 10 - 5

    tx = (10 * extent_space_x)/k_coeff * l_scale_coeff
    ty = (10 * extent_space_y)/k_coeff * l_scale_coeff

    tx1 = (10 * (extent_space_x1 + eps))/k_coeff * l_scale_coeff  

    #coordinates for v vector
    ty2 = (10 * (extent_space_y2 + eps))/k_coeff * l_scale_coeff

    #extract height values
    h_val  = subset_hill_terrain[int(j),int(i)]
    h_uval = subset_hill_terrain[int(j),int(i1)]
    h_vval = subset_hill_terrain[int(j1),int(i)]

    tz =  (height_coeff * (100/k_coeff )) * h_val 
    tz1 = (height_coeff * (100/k_coeff )) * h_uval 
    tz2 = (height_coeff * (100/k_coeff )) * h_vval 

    theta_x = np.degrees(np.arctan2((tz2 - tz),(ty2 - ty)))
    theta_y = np.degrees(np.arctan2((tz1 - tz),(tx1 - tx)))
    theta_z = rotate_val
    # theta_z = 0

    fmt_string = killeroo_string_instance(height_coeff,
                                scale = k_coeff,
                                translation=[tx,ty,tz],
                                rotation = [theta_x,theta_y,theta_z],
                                instance_name = instance_name,
                                killeroo_path=killeroo_path)
    return fmt_string
def genCamera(LookAt,height_coeff,fov,xres,yres,output_name):
    LookAt[0] = [i *  height_coeff for i in LookAt[0] ]
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
def genAboveLight(height_coeff):
    fmt_string = Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("LightSource",[parameter_string("distant"),
                                                  parameter_numeric("point from", [0,1.5*height_coeff,0]),
                                                  parameter_numeric("point to", [0,0,0]),
                                                   parameter_numeric("color L",[10,
                                                                                10,
                                                                                10])])
    fmt_string += Attribute_string("AttributeEnd\n")  
    return fmt_string
def genAccelerator(maxtreeletbytes: int = 1000000000):
    fmt_string = Attribute_string("Accelerator",[parameter_string("bvh"),
                                                parameter_numeric("integer maxtreeletbytes",[maxtreeletbytes]),
                                                parameter_numeric("string partition",['"nvidia"']),
                                                parameter_numeric("string traversal",['"sendcheck"'])])
    return fmt_string
def genKillerooTerrain(output_filename: str,
                       xres: int,
                       yres: int,
                       landxres: int,
                       landyres: int,
                       landiters: int,
                       numDroplets: int = None,
                       LookAt: list = [[0,2.5,-0.5],[0,0,0],[0,1,0]],
                       l_scale_coeff: int = 100,
                       height_coeff: int = 30,
                       k_coeff: int = 10,
                       num_killeroos = 25,
                       num_killeroo_instances = 10,
                       num_chunks = 2,
                       prop = 0.1,
                       random_seed: int = None,
                       killeroo_path: str = "geometry/killeroo.pbrt" ,
                       land_filename: str = "./geometry/gen_killeroo_land",
                       killeroos_filename: str = "./geometry/gen_killeroo_geometry",
                       chunks_filename: str = "./geometry/gen_killeroo_master"):
    f = open(output_filename, 'w')
    assert os.path.exists(killeroo_path), "killeroo geometry file not found at: {}".format(killeroo_path)
    
    #Camera,Sampling, and Integrator parameters
    fmt_string = genCamera(LookAt,height_coeff,50,xres,yres,output_filename[:output_filename.rfind(".")] + ".exr" )
    fmt_string += genSampler(8)
    fmt_string += genAccelerator();
    fmt_string += Attribute_string("WorldBegin\n")
    
    #Above square trianglemesh light source
    fmt_string += genAboveLight(height_coeff)
    
    #Camera light source 
    fmt_string += Attribute_string("AttributeBegin")
    fmt_string += Attribute_string("CoordSysTransform",[parameter_string("camera")])
    fmt_string += Attribute_string("LightSource",[parameter_string("point"),
                                                  parameter_numeric("color I",[15,15,15])])

    fmt_string += Attribute_string("AttributeEnd\n") 
    
    #Land 
    print("Generating Land...")
    #Generate land 
    # hill_terrain = [];
    # if landiters > 0:
    #     hill_terrain = genHillTerrain(nx=landxres,ny=landyres,iters=landiters,seed=random_seed)
    #     if numDroplets != None and numDroplets != -1:
    #         hill_terrain = erodeTerrainDroplet(hill_terrain,numDroplets)
    #     elif numDroplets != -1:
    #         hill_terrain = erodeTerrainDroplet(hill_terrain,int(0.625 * landxres * landyres))

    #     # plt.figure()
    #     # plt.imshow(hill_terrain * 255,cmap='gray')
    #     # plt.colorbar()
    #     # plt.show()
    # else:
    #     if random_seed != None:
    #         np.random.seed(random_seed)
    #     # hill_terrain = np.ones((landxres,landyres)) * 0.3
    #     hill_terrain = genHillTerrainPerlin(landxres,landyres)
    #     hill_terrain = (((hill_terrain - np.min(hill_terrain))/(np.ptp(hill_terrain)))) * 0.90
    #     hill_terrain = (hill_terrain ** 2  )
    #     # hill_terrain = ndimage.gaussian_filter(hill_terrain,2)
    #     # hill_terrain = erodeTerrainDroplet(hill_terrain,int(0.01 * landxres * landyres))
    #     plt.imshow(hill_terrain)
    #     plt.show()
    # nx,ny,num_chunks,iters,seed,l_scale_coeff,height_coeff,land_filename,output_filename
    fmt_strings = genChunkTerrain(landxres,landyres,
                                  num_chunks,landiters,
                                  random_seed,l_scale_coeff,
                                  height_coeff,land_filename,
                                  output_filename)

    for i in range(num_chunks ** 2):
        open(killeroos_filename + str(i) + ".pbrt", 'w').close()
        s = open(chunks_filename + str(i) + ".pbrt",'w')
        #include chunk terrain to chunk master
        s.write(fmt_strings[i])


    #kileroo gen    

    print("creating killeroo instances...")
    for i in tqdm(range(num_killeroo_instances)):
        instance_name = "killerooInstance" + str(i)
        color1 = np.random.uniform(0,1,(3)).tolist()
        color2 = np.random.uniform(0,1,(3)).tolist()
        fmt_string += killeroo_string_object(instance_name,
                                             color1,
                                             color2,
                                             killeroo_path=
                                             os.path.relpath(killeroo_path,
                                             os.path.dirname(output_filename)))

    m,n = (landxres,landyres)
    total_killeroos = num_killeroos

    subset_res_x,subset_res_y = (m //num_chunks,n // num_chunks)

    num_killeroos_per_chunk = int(total_killeroos/(num_chunks ** 2))
    num_groups_per_chunk = max(1,int(num_killeroos_per_chunk // 20))
    num_killeroos_per_group = int(num_killeroos_per_chunk/num_groups_per_chunk)

    s = 8
    #generate killeroos per chunk 
    print("placing killeroos...")
    for k in tqdm(range(num_chunks ** 2)): 
        # open killeroo file 
        t = open(killeroos_filename + str(k) + ".pbrt",'a')
        # grab random instance name
        #grab terrain 
        subset_hill_terrain = loadFromLand(land_filename + str(k) + '.pbrt',subset_res_x,subset_res_y)

        # middle of terrain
        samples,step = np.linspace(subset_res_x/s,
                                  subset_res_x * (s - 1)/s,
                                  max(2,int(num_groups_per_chunk ** 0.5)),retstep=True)
        # print(step)
        # print(num_groups_per_chunk ** 0.5)
        grid = np.meshgrid(samples,
                           samples)
        sparse_positions = np.vstack([grid[0].flatten(),grid[1].flatten()])
        for m in range(sparse_positions.shape[1]):
            instance_name = "killerooInstance" + str(np.random.randint(0,num_killeroo_instances))
            # random position in terrain for leader
            # i = np.random.randint(subset_res_x/10,subset_res_x * 9/10)
            # j = np.random.randint(subset_res_y/10,subset_res_y * 9/10)
            i,j = sparse_positions[:,m].astype(int)
            i = np.clip(i + np.random.uniform(-step/2,step/2),subset_res_x/s,subset_res_x * (s - 1)/s)
            j = np.clip(j + np.random.uniform(-step/2,step/2),subset_res_y/s,subset_res_y * (s - 1)/s)
            rotate_val = np.random.uniform(0,360)
            t.write(genKillerooInstance(i,j,num_chunks,k,
                                            subset_hill_terrain,
                                            k_coeff,l_scale_coeff,
                                            height_coeff,rotate_val,
                                            instance_name = instance_name,
                                            killeroo_path = killeroo_path))
            for l in range(num_killeroos_per_group):
                #random placement of group near leader 
                u = np.clip(i + np.random.randint(-subset_res_x/s,subset_res_x/s),0,subset_res_x - 2)
                v = np.clip(j + np.random.randint(-subset_res_y/s,subset_res_y/s),0,subset_res_y - 2)
                t.write(genKillerooInstance(u,v,num_chunks,k,
                                            subset_hill_terrain,
                                            k_coeff,l_scale_coeff,
                                            height_coeff,rotate_val,
                                            instance_name = instance_name,
                                            killeroo_path = killeroo_path))



    # samples,step = np.linspace(-4,4,int(num_sparse_killeroos ** 0.5),retstep=True)
    # grid = np.meshgrid(samples,
    #                    samples)
    # sparse_positions = np.vstack([grid[0].flatten(),grid[1].flatten()])
    # print("Placing killeroos...")
    # for k in tqdm(range(sparse_positions.shape[1])):
    #     i,j = sparse_positions[:,k]
    #     i = np.clip(i + np.random.uniform(-step/2,step/2),-4,4)
    #     j = np.clip(j + np.random.uniform(-step/2,step/2),-4,4)
    #     #convert to [0,1] range
    #     i_norm = (i + 4)/8
    #     j_norm = (j + 4)/8
    #     #convert to [0,num_chunks-1] range
    #     i_scale = i_norm * (num_chunks - 1)
    #     j_scale = j_norm * (num_chunks - 1)
    #     #1d coordinate
    #     coord = int(round(j_scale) * num_chunks + round(i_scale))
    #     # print("i_scale: {}, j_scale: {}, coord: {}".format(i_scale,j_scale,coord))
    #     t = open(killeroos_filename + str(coord) + ".pbrt",'a')

    #     rotate_val = np.random.uniform(0,360)
    #     instance_name = "killerooInstance" + str(np.random.randint(0,num_killeroo_instances))
    #     t.write(genKillerooInstance(m,n,i,j,k_coeff,
    #                         l_scale_coeff,height_coeff,
    #                         hill_terrain,rotate_val,
    #                         instance_name,os.path.relpath(killeroo_path,os.path.dirname(output_filename))))
    #     for r in range(num_killeroos_per_sparse):
    #         u = np.clip(i + np.random.uniform(-step/3,step/3),-4,4)
    #         v = np.clip(j + np.random.uniform(-step/3,step/3),-4,4)
    #         t.write(genKillerooInstance(m,n,u,v,k_coeff,
    #                             l_scale_coeff,height_coeff,
    #                             hill_terrain,rotate_val,
    #                             instance_name,os.path.relpath(killeroo_path,os.path.dirname(output_filename))))

    for i in range(num_chunks ** 2):
        s = open(chunks_filename + str(i) + ".pbrt",'a')
        #include killeroo file to chunk master 
        s.write(Attribute_string("Include",[parameter_string(os.path.relpath(killeroos_filename + str(i) + ".pbrt",
                                                                                    os.path.dirname(output_filename)))]))
        #include chunk master to top master
        fmt_string += Attribute_string("Include",[parameter_string(os.path.relpath(chunks_filename + str(i) + ".pbrt",
                                                                                    os.path.dirname(output_filename)))])
    fmt_string += Attribute_string("WorldEnd")
    f.write(fmt_string)
    
def main():
    parser = argparse.ArgumentParser(description=("'Generate a parameterized pbrt file of killeroos on hilly terrain"))
    parser.add_argument('--xres',default =700,help=("x resolution target of pbrt file"))
    parser.add_argument('--yres',default =700,help=("y resolution target of pbrt file"))
    parser.add_argument('--landiters',default = 440,help=("number of iterations hill generator should take"))
    parser.add_argument('--numDroplets',default = None,help=("number of droplets to simulate (set to -1 for no erosion)"))
    # parser.add_argument('--k_coeff',default = 2,help=("scale of killeroos "))
    # parser.add_argument('--LookAt',default =[[0,500,-500],[0,0,0],[0,1,0]],
    #                     help=("LookAt params for pbrt camera"))
    parser.add_argument('--killeroo_path',default="./geometry/killeroo.pbrt",
                        help=(' input file path for kilerooo pbrt file'
                            ))
    parser.add_argument('--output_path', default="./",
                        help=('output folder path for all files  generated by script main pbrt file'
                            ))
    parser.add_argument('--num_killeroos',default =(10000))
    parser.add_argument('--num_killeroo_instances',default=(int(10000/100)),help=('number of killeroo instances to choose from when placing killeroos'))
    parser.add_argument('--num_chunks',default =(2),help=('number of geometry and killeroo chunks per x and y dim'
                            ))
    parser.add_argument('--landxres',default =1000 ,help=("resolution of heightmap x"))
    parser.add_argument('--landyres',default =1000 ,help=("resolution of heightmap y"))
    parser.add_argument('--land_scale',default =1000 ,help=("scale of physical width and breadth of heightmap"))
    parser.add_argument('--random_seed',default =2)


    args = parser.parse_args()
    if not os.path.exists(args.output_path):
        os.mkdir(args.output_path)

    if args.numDroplets == None:
        numDroplets = None
    else:
        numDroplets = int(args.numDroplets)
    k_coeff = float(20000/int(args.num_killeroos))
    genKillerooTerrain(os.path.join(args.output_path,"killeroo_terrain.pbrt"),
                       xres=int(args.xres),
                       yres=int(args.yres),
                       landxres =int(args.landxres),
                       landyres = int(args.landyres),
                       landiters=int(args.landiters),
                       numDroplets=numDroplets,
                       l_scale_coeff=float(args.land_scale),
                       height_coeff=0.5 * float(args.land_scale),
                       k_coeff=k_coeff,
                       num_killeroos=int(args.num_killeroos),
                       num_killeroo_instances=int(args.num_killeroo_instances),
                       num_chunks = int(args.num_chunks),
                       # prop = float(args.unique_prop),
                       random_seed=int(args.random_seed),
                       killeroo_path=args.killeroo_path,
                       land_filename=os.path.join(args.output_path,"gen_killeroo_land"),
                       killeroos_filename=os.path.join(args.output_path,"gen_killeroo_geometry"),
                       chunks_filename = os.path.join(args.output_path,"gen_killeroo_master"))
if __name__ == '__main__':
    main()
