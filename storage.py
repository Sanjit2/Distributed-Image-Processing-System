from pathlib import Path
import shutil
import uuid


class LocalStorage:
    """Local implementation of the image storage boundary.

    The master is the only service that needs this storage today. An S3-backed
    implementation can provide the same operations later without changing the
    Kafka or worker pipeline.
    """

    def __init__(self, upload_folder, results_folder, temp_tiles_folder,
                 cache_folder):
        self.upload_folder = Path(upload_folder)
        self.results_folder = Path(results_folder)
        self.temp_tiles_folder = Path(temp_tiles_folder)
        self.cache_folder = Path(cache_folder)
        for folder in (self.upload_folder, self.results_folder,
                       self.temp_tiles_folder, self.cache_folder):
            folder.mkdir(parents=True, exist_ok=True)

    def upload_path(self, job_id, filename):
        return self.upload_folder / f'{job_id}_{filename}'

    def tile_folder(self, job_id):
        folder = self.temp_tiles_folder / job_id
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def tile_path(self, job_id, tile_id):
        return self.tile_folder(job_id) / f'tile_{tile_id}.jpg'

    def result_path(self, job_id):
        return self.results_folder / f'{job_id}.jpg'

    def cache_path(self, cache_key):
        return self.cache_folder / f'{cache_key}.jpg'

    def store_cached_result(self, result_path, cache_key):
        """Atomically add a completed result to the persistent image cache."""
        cache_path = self.cache_path(cache_key)
        if cache_path.exists():
            return cache_path

        temporary_path = self.cache_folder / f'.{cache_key}.{uuid.uuid4().hex}.tmp'
        try:
            shutil.copyfile(result_path, temporary_path)
            try:
                temporary_path.replace(cache_path)
            except FileExistsError:
                # Another request completed the same cache entry first.
                pass
        finally:
            temporary_path.unlink(missing_ok=True)
        return cache_path

    def cleanup_tiles(self, job_id):
        shutil.rmtree(self.temp_tiles_folder / job_id, ignore_errors=True)