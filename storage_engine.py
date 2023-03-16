"""
Code for storage engine and related implementations
"""

import os
import hashlib

class DataStorageEngine:
    def __init__(self, base_dir, num_shards=10):
        self.base_dir = base_dir
        self.num_shards = num_shards
        self.shards = [self._get_shard_filename(i) for i in range(num_shards)]

    def _get_shard_filename(self, shard_id):
        return os.path.join(self.base_dir, f"data_{shard_id}.txt")

    def _get_shard_index(self, key):
        hashed_key = hashlib.sha256(key.encode()).hexdigest()
        return int(hashed_key, 16) % self.num_shards

    def load_data(self):
        self.data = {}
        for shard_filename in self.shards:
            if not os.path.isfile(shard_filename):
                continue

            with open(shard_filename, 'r') as f:
                for line in f:
                    key, value = line.rstrip().split(':', 1)
                    self.data[key] = value

    def save_data(self):
        shard_data = [{} for _ in range(self.num_shards)]

        for key, value in self.data.items():
            shard_id = self._get_shard_index(key)
            shard_data[shard_id][key] = value

        for i, shard_filename in enumerate(self.shards):
            with open(shard_filename, 'w') as f:
                for key, value in shard_data[i].items():
                    f.write(f"{key}:{value}\n")

    def get(self, key):
        shard_id = self._get_shard_index(key)
        shard_filename = self.shards[shard_id]
        if not os.path.isfile(shard_filename):
            return None

        with open(shard_filename, 'r') as f:
            for line in f:
                shard_key, value = line.rstrip().split(':', 1)
                if key == shard_key:
                    return value

        return None

    def set(self, key, value):
        self.data[key] = value
        self.save_data()

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            self.save_data()
