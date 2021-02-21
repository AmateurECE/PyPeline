import sys, os
sys.path.append(os.path.realpath('.'))
from pypeline import Pipeline
sys.argv = ['pypeline/Pipeline.py', '-f', './pipeline.yaml']
Pipeline.main()
