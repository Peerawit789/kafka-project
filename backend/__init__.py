import os
import sys

# Ensure backend directory is in sys.path for direct imports
_backend_dir = os.path.dirname(__file__)
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
