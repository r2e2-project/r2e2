#!/usr/bin/env python3

import os
import sys
import pprint
import json

from itertools import islice

class Treelet:
    def __init__(self, tid, size=0):
        self.id = tid
        self.size = size
        self.bounds = [[0, 0, 0], [0, 0, 0]]
        self.rays = 0
        self.level = 0
        self.nodes = []
        self.parent = None
        self.children = []
        self.instances = []
        self.instanced_in = []
        self.main = True

    def __str__(self):
        return pprint.pformat({'tid': self.id,
                               'main': self.main,
                               'size': sizeof_fmt(self.size),
                               'bounds': self.bounds,
                               'rays': self.rays,
                               'level': self.level,
                               'nodes': self.nodes,
                               'parent': self.parent,
                               'children': self.children,
                               'instances': self.instances,
                               'instanced_in': self.instanced_in})

    def __repr__(self):
            return self.__str__()

def get_main_treelets(data):
    main = []
    q = [0]

    while len(q):
        node = q.pop()
        main.append(node)

        for x in data[node].children:
            q.append(x)

    return main

def assign_levels(treelets):
    q = [0]

    while len(q):
        node = q.pop()

        for x in treelets[node].children:
            treelets[x].level = treelets[node].level + 1
            q += [x]

def parse_treelet_data(path):
    TREELET_LINE = 0
    BOUNDS_LINE = 1
    BVH_NODES_LINE = 2
    CHILD_LINE = 3
    INSTANCE_LINE = 4

    with open(path) as fin:
        count = int(fin.readline().strip().split(" ")[1])
        treelets = []

        for i in range(count):
            treelets.append(Treelet(i))

        while True:
            line = list(islice(fin, 5))

            if not line:
                break

            for i in [TREELET_LINE, CHILD_LINE, INSTANCE_LINE]:
                line[i] = [int(x) for x in line[i].strip().split()[1:]]

            tid = line[TREELET_LINE][0]
            treelets[tid].size = line[TREELET_LINE][1]

            line[BOUNDS_LINE] = line[BOUNDS_LINE].split(" ")[-1]
            treelets[tid].bounds = json.loads(line[BOUNDS_LINE])

            line[BVH_NODES_LINE] = line[BVH_NODES_LINE].split(" ")[-1]
            treelets[tid].nodes = json.loads(line[BVH_NODES_LINE])

            for t in line[CHILD_LINE]:
                treelets[tid].children.append(t)
                treelets[t].parent = tid

            for t in line[INSTANCE_LINE]:
                treelets[tid].instances.append(t)
                treelets[t].instanced_in += [tid]

    assign_levels(treelets)

    # Mark main & instance treelets
    main_treelets = set(get_main_treelets(treelets))

    for T in treelets:
        if T.id not in main_treelets:
            T.main = False
        else:
            T.main = True

        T.children.sort(key=lambda x: treelets[x].rays, reverse=True)
        T.instances.sort(key=lambda x: treelets[x].rays, reverse=True)

    return treelets

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("{} TREELETS-FILE".format(sys.argv[0]))
        exit(1)

    print(json.dumps([t.__dict__ for t in parse_treelet_data(sys.argv[1])]))
