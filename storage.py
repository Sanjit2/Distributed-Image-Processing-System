from pathlib import Path


class LocalStorage:
    """Local implementation of the image storage boundary.

    The master is the only service that needs this storage today. An S3-backed
    implementation can provide the same operations later without changing the
    Kafka or worker pipeline.
    """

    def __init__(self, upload_folder, results_folder, temp_tiles_folder):
        self.upload_folder = Path(upload_folder)
        self.results_folder = Path(results_folder)
        self.temp_tiles_folder = Path(temp_tiles_folder)
        for folder in (self.upload_folder, self.results_folder, self.temp_tiles_folder):
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

    def cleanup_tiles(self, job_id):
        import shutil
        shutil.rmtree(self.temp_tiles_folder / job_id, ignore_errors=True)