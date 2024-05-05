#!/usr/bin/env python
 
import json
import sys
from datetime import datetime
 
if len(sys.argv) < 3:
    print("Usage: python proc.py <log> <delay>")
else:
    f = open(sys.argv[1], 'w')
    start = round(datetime.now().timestamp())
    delay = int(sys.argv[2])
    wm = start - delay
    
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        d = json.loads(line)
        e = d.get('type')
        ts = d.get('ts')
        u = d.get('user')
        b = d.get('bot')
        res = "{:s}, tp={:d}, ts={:d}, user=\"{:s}\", bot={:b}".format(e,ts,ts,u,b)
        print(res, file=f)