import zipfile as zf
import os
import re

nodes = set()
path = "./raw_pm/"
zips = [path + i for i in os.listdir(path) if i.endswith(".zip")]

for z in zips:
    cur = zf.ZipFile(z)
    nodenames = set([x.group(1) for x in [re.search(r"(.+)(\_pm.+).csv",n) for n in cur.namelist()] if x is not None])
    nodes = nodes.union(nodenames)
    cur.close()

out = open("node_list.txt","w") 
for each in sorted(nodes):
    out.write(each + "\n")
out.close()