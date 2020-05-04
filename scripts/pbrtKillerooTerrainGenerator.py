import numpy as np 
from scipy import ndimage
import os
from pathlib import Path
# import matplotlib.pyplot as plt 
import sys
import functools
import operator
import math
from tqdm import tqdm
import argparse


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

    hill_terrain = (2 * ((hill_terrain - np.min(hill_terrain))/(np.ptp(hill_terrain))) - 1) * 0.99
    hill_terrain = (hill_terrain ** 2  )
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

def chunkTerrain(num_chunks,hill_terrain,l_scale_coeff,height_coeff,land_filename,output_filename):
        m,n = hill_terrain.shape
        hill_terrain_blocked = blockshaped(hill_terrain,m//num_chunks,n//num_chunks)
        print(hill_terrain_blocked.shape)
        s_scale_coeff =  l_scale_coeff / num_chunks
        fmt_strings = []
        for i in range(hill_terrain_blocked.shape[0]):
            hill_terrain_subset = hill_terrain_blocked[i,:,:]
            subset_filename = land_filename + str(i) + ".pbrt"
            genLandPbrt(subset_filename,hill_terrain_subset);
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
    if is_instance == False:
        fmt_string += Attribute_string("ObjectBegin",[parameter_string(instance_name)])
        fmt_string += Attribute_string("Material",[parameter_string("plastic"),
                                                   parameter_numeric("color Kd",color1),
                                                   parameter_numeric("color Kd",color2),
                                                   parameter_numeric("float roughness",roughness)])
        fmt_string += Attribute_string("Include",[parameter_string(killeroo_path)])
        fmt_string +=  Attribute_string("ObjectEnd")
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
    

def genKilleroo(m,n,i,j,
                k_coeff,
                color1,
                color2,
                l_scale_coeff,
                height_coeff,
                hill_terrain,
                rotate_val = None,
                is_instance = False,
                instance_name = "killerooInstance",
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
                                    color1 = color1,
                                    color2 = color2,
                                    is_instance=is_instance,
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
    fmt_string = Attribute_string("Accelerator",[parameter_string("treeletdumpbvh"),
                                                parameter_numeric("integer maxtreeletbytes",[maxtreeletbytes]),
                                                parameter_numeric("string partition",['"nvidia"']),
                                                parameter_numeric("string traversal",['"sendcheck"']),
                                                parameter_numeric("string splitmethod",['"hlbvh"'])])
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
                       num_chunks = 2,
                       prop = 0.1,
                       random_seed: int = None,
                       killeroo_path: str = "geometry/killeroo.pbrt" ,
                       land_filename: str = "./geometry/gen_killeroo_land",
                       killeroos_filename: str = "./geometry/gen_killeroo_geometry",
                       chunks_filename: str = "./geometry/gen_killeroo_master"):
    f = open(output_filename, 'w')
    assert os.path.exists(killeroo_path), "killeroo geometry file not found at: {}".format(killeroo_path)
    #Generate land 
    hill_terrain = [];
    if landiters > 0:
        hill_terrain = genHillTerrain(nx=landxres,ny=landyres,iters=landiters,seed=random_seed)
        if numDroplets != None and numDroplets != -1:
            hill_terrain = erodeTerrainDroplet(hill_terrain,numDroplets)
        elif numDroplets != -1:
            hill_terrain = erodeTerrainDroplet(hill_terrain,int(0.625 * landxres * landyres))

        # plt.figure()
        # plt.imshow(hill_terrain * 255,cmap='gray')
        # plt.colorbar()
        # plt.show()
    else:
        if random_seed != None:
            np.random.seed(random_seed)
        hill_terrain = np.ones((landxres,landyres)) * 0.3
    
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
    fmt_strings = chunkTerrain(num_chunks,
                               hill_terrain,
                               l_scale_coeff,
                               height_coeff,
                               land_filename,
                               output_filename)

    for i in range(num_chunks ** 2):
        open(killeroos_filename + str(i) + ".pbrt", 'w').close()
        s = open(chunks_filename + str(i) + ".pbrt",'w')
        #include chunk terrain to chunk master
        s.write(fmt_strings[i])
    #kileroo gen    
    print("Placing killeroos...")

    m,n = hill_terrain.shape
    total_killeroos = num_killeroos
    num_sparse_killeroos = int(math.ceil(prop * total_killeroos))
    num_killeroos_per_sparse = int((total_killeroos - num_sparse_killeroos)/(num_sparse_killeroos))

    samples,step = np.linspace(-4,4,int(num_sparse_killeroos ** 0.5),retstep=True)
    grid = np.meshgrid(samples,
                       samples)
    sparse_positions = np.vstack([grid[0].flatten(),grid[1].flatten()])
    for k in tqdm(range(sparse_positions.shape[1])):
        i,j = sparse_positions[:,k]
        i = np.clip(i + np.random.uniform(-step/2,step/2),-4,4)
        j = np.clip(j + np.random.uniform(-step/2,step/2),-4,4)
        #convert to [0,1] range
        i_norm = (i + 4)/8
        j_norm = (j + 4)/8
        #convert to [0,num_chunks-1] range
        i_scale = i_norm * (num_chunks - 1)
        j_scale = j_norm * (num_chunks - 1)
        #1d coordinate
        coord = int(round(j_scale)* num_chunks + round(i_scale))
        # print("i_scale: {}, j_scale: {}, coord: {}".format(i_scale,j_scale,coord))
        t = open(killeroos_filename + str(coord) + ".pbrt",'a')
        rotate_val = np.random.uniform(0,360)
        instance_name = "killerooInstance" + str(k)
        color1 = np.random.uniform(0,1,(3)).tolist()
        color2 = np.random.uniform(0,1,(3)).tolist()
        fmt_string += genKilleroo(m,n,i,j,k_coeff,color1,color2,
                            l_scale_coeff,height_coeff,
                            hill_terrain,rotate_val,False,
                            instance_name,os.path.relpath(killeroo_path,os.path.dirname(output_filename)))
        for r in range(num_killeroos_per_sparse):
            u = np.clip(i + np.random.uniform(-step/3,step/3),-4,4)
            v = np.clip(j + np.random.uniform(-step/3,step/3),-4,4)
            t.write(genKilleroo(m,n,u,v,k_coeff,color1,color2,
                                l_scale_coeff,height_coeff,
                                hill_terrain,rotate_val,True,
                                instance_name,os.path.relpath(killeroo_path,os.path.dirname(output_filename))))

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
    parser.add_argument('--k_coeff',default = 2,help=("scale of killeroos "))
    # parser.add_argument('--LookAt',default =[[0,500,-500],[0,0,0],[0,1,0]],
    #                     help=("LookAt params for pbrt camera"))
    parser.add_argument('--killeroo_path',default="./geometry/killeroo.pbrt",
                        help=(' input file path for kilerooo pbrt file'
                            ))
    parser.add_argument('--output_path', default="./",
                        help=('output folder path for all files  generated by script main pbrt file'
                            ))
    parser.add_argument('--num_killeroos',default =(10000))
    parser.add_argument('--num_chunks',default =(2),help=('number of geometry and killeroo chunks per x and y dim'
                            ))
    parser.add_argument('--unique_prop',default =.01,help=("proportion of killeroos that are unique"))
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
    genKillerooTerrain(os.path.join(args.output_path,"killeroo_terrain.pbrt"),
                       xres=int(args.xres),
                       yres=int(args.yres),
                       landxres =int(args.landxres),
                       landyres = int(args.landyres),
                       landiters=int(args.landiters),
                       numDroplets=numDroplets,
                       l_scale_coeff=float(args.land_scale),
                       height_coeff=0.5 * float(args.land_scale),
                       k_coeff=float(args.k_coeff),
                       num_killeroos=int(args.num_killeroos),
                       num_chunks = int(args.num_chunks),
                       prop = float(args.unique_prop),
                       random_seed=int(args.random_seed),
                       killeroo_path=args.killeroo_path,
                       land_filename=os.path.join(args.output_path,"gen_killeroo_land"),
                       killeroos_filename=os.path.join(args.output_path,"gen_killeroo_geometry"),
                       chunks_filename = os.path.join(args.output_path,"gen_killeroo_master"))
if __name__ == '__main__':
    main()
