import sys
from os.path import exists
import json

assert len(sys.argv) > 1, "Not enough arguments supplied, usage: 'python3 jsonify_breadcrumb.py <filename>'"
filename = sys.argv[1]
assert exists(filename), f"Did not find file: {filename}"

with open(filename) as f:
  breadcrumb_data = json.load(f)
  print(breadcrumb_data[0])

# TODO send to Kafka here.