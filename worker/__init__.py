import os
import sys

# Ensure worker directory is in sys.path for direct imports
_worker_dir = os.path.dirname(__file__)
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)
