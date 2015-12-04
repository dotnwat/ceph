#!/bin/env python

import os
import sys
import platform

MYPATH = os.path.abspath(__file__)
MYDIR = os.path.dirname(MYPATH)
DEVMODEMSG = '*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***'

def respawn_in_path(lib_path, pybind_path):
    execv_cmd = ['python']
    if 'CEPH_DBG' in os.environ:
        execv_cmd += ['-mpdb']

    if platform.system() == "Darwin":
        lib_path_var = "DYLD_LIBRARY_PATH"
    else:
        lib_path_var = "LD_LIBRARY_PATH"

    py_binary = os.environ.get("PYTHON", "python")

    if lib_path_var in os.environ:
        if lib_path not in os.environ[lib_path_var]:
            os.environ[lib_path_var] += ':' + lib_path
            print >> sys.stderr, DEVMODEMSG
            os.execvp(py_binary, execv_cmd + sys.argv)
    else:
        os.environ[lib_path_var] = lib_path
        print >> sys.stderr, DEVMODEMSG
        os.execvp(py_binary, execv_cmd + sys.argv)
    sys.path.insert(0, os.path.join(MYDIR, pybind_path))

if MYDIR.endswith('src') and \
   os.path.exists(os.path.join(MYDIR, '.libs')) and \
   os.path.exists(os.path.join(MYDIR, 'pybind')):

    respawn_in_path(os.path.join(MYDIR, '.libs'), "pybind")
    if os.environ.has_key('PATH') and MYDIR not in os.environ['PATH']:
        os.environ['PATH'] += ':' + MYDIR

elif os.path.exists(os.path.join(os.getcwd(), "CMakeCache.txt")) \
     and os.path.exists(os.path.join(os.getcwd(), "init-ceph")):
    src_path = None
    for l in open("./CMakeCache.txt").readlines():
        if l.startswith("Ceph_SOURCE_DIR:STATIC="):
            src_path = l.split("=")[1].strip()

    if src_path is None:
        # Huh, maybe we're not really in a cmake environment?
        pass
    else:
        # Developer mode, but in a cmake build dir instead of the src dir
        lib_path = os.path.join(os.getcwd(), "src")
        pybind_path = os.path.join(src_path, "src", "pybind")
        respawn_in_path(lib_path, pybind_path)

    sys.path.insert(0, os.path.join(MYDIR, pybind_path))

    # Add src/ to path for e.g. ceph-conf
    if os.environ.has_key('PATH') and lib_path not in os.environ['PATH']:
        os.environ['PATH'] += ':' + lib_path

import sys
import json
from rados import Rados

rados = Rados(conffile='', rados_id='admin')
rados.connect()
pool = sys.argv[1]

script = sys.argv[2]
if script == "-":
    script = sys.stdin.read()

def set_lua_script(rados, pool, script, timeout=30):
    cmd = {
      "prefix": "osd pool set-class",
      "pool":    pool,
      "class":   "not-currently-used",
      "script":  script,
    }
    return rados.mon_command(json.dumps(cmd), '', timeout)

print set_lua_script(rados, pool, script)
