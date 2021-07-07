import json
import pickle

from plugins.config import cfg
with open(cfg.app.paths.regions, 'rb') as f:
    regions = pickle.load(f)

with open(cfg.app.paths.locality, 'rb') as f:
    locality = pickle.load(f)

with open(cfg.app.paths.categories) as f:
    category = {int(k): int(v) for k, v in json.load(f).items()}

with open(cfg.app.paths.vacs_test, 'rb') as f:
    vacs = pickle.load(f)

with open(cfg.app.paths.index_test, 'rb') as f:
    index_cache = pickle.load(f)
