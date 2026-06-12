"""Utilities for creating docker images for workers."""

import argparse
import hashlib
import json
import logging
import os
import platform
import sys
import tarfile
import tempfile
from typing import Optional
import uuid

import apache_beam as beam
from google.cloud import storage  # type: ignore

try:
    from google.cloud.devtools import cloudbuild  # type: ignore
except ImportError:
    cloudbuild = None  # type: ignore

_CACHE_DIR = os.path.join(tempfile.gettempdir(), 'ds_sdk')
_CACHE_LOCATION = os.path.join(_CACHE_DIR, 'image_cache.json')
_BLOCKSIZE = 64 * 1024


def hash_dir(directory: str) -> str:
    hashes = []
    for root, _, files in os.walk(directory, topdown=True):
        files.sort()
        for file_name in files:
            hasher = hashlib.md5()
            with open(os.path.join(root, file_name), 'rb') as f:
                while True:
                    data = f.read(_BLOCKSIZE)
                    if not data:
                        break
                    hasher.update(data)
            hashes.append(hasher.hexdigest())
    hasher = hashlib.md5()
    for h in hashes:
        hasher.update(h.encode('utf-8'))
    return hasher.hexdigest()


class _DockerImageCache:
    """Cache for previously built docker images."""

    def __init__(self) -> None:

        try:
            os.makedirs(_CACHE_DIR, exist_ok=True)
            with open(_CACHE_LOCATION, 'r', encoding='UTF-8') as f:
                self._check_sum_to_image = json.load(f)
        except FileNotFoundError:
            self._check_sum_to_image = {}

    def get_image(self, checksum: str) -> Optional[str]:
        return self._check_sum_to_image.get(checksum, None)

    def add_image(self, checksum: str, image: str):
        self._check_sum_to_image[checksum] = image
        with open(_CACHE_LOCATION, 'w+', encoding='UTF-8') as f:
            json.dump(self._check_sum_to_image, f)


_image_cache = _DockerImageCache()

_DOCKER_FILE_WITH_REQUIREMENTS = """
FROM apache/beam_python{py_version}_sdk:{beam_version}

WORKDIR /code

COPY ./ ./

RUN pip install --upgrade pip setuptools wheel
RUN pip install twine keyrings.google-artifactregistry-auth
RUN pip install --extra-index-url=https://us-central1-python.pkg.dev/verily-datascience-prod/python-repo/simple -r requirements.txt

"""


def should_create_worker_image() -> bool:
    """Returns boolean indicating whether if a docker image should be built.

  Returns true if both the --setup_file and --requirements_file are NOT set.
  """
    args = sys.argv

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--setup_file',
        help='Setup file used by dataflow to install dependencies on workers.',
        required=False,
        default='')
    parser.add_argument(
        '--requirements_file',
        help='Requirements file used by dataflow to install dependencies.',
        required=False,
        default='')
    parser.add_argument('--sdk_container_image',
                        help='Container image to use for dataflow workers.',
                        required=False,
                        default='')

    parsed_args, _ = parser.parse_known_args(args)

    return (not parsed_args.setup_file and not parsed_args.requirements_file and
            not parsed_args.sdk_container_image)


def _upload_source_tar(job_name: str) -> str:
    logging.info(
        'Creating source distribution for docker image from directory: %s',
        os.getcwd())
    file_name = f'{job_name}_{str(uuid.uuid4())}.tar.gz'
    tar_file = os.path.join(tempfile.gettempdir(), file_name)
    # Create tar file with source code.
    with tarfile.open(tar_file, 'w:gz') as tar:
        for root, _, files in os.walk('.'):
            for f in files:
                tar.add(os.path.join(root, f))

    # Upload tar to GCS.
    storage_client = storage.Client()
    bucket = storage_client.bucket('datascience-sdk_cloudbuild')
    blob = bucket.blob(os.path.join('source', file_name))
    blob.upload_from_filename(tar_file, timeout=300)
    logging.info('Succesfully uploaded %s to bucket: %s', blob.name,
                 bucket.name)
    return blob.name


def build_docker_image(job_name: str) -> str:
    """Builds docker image and returns the path to the upload docker image."""

    if cloudbuild is None:
        raise ValueError(
            'Could not import cloudbuild. This may be because you are in '
            'google3 where docker images are not supported.')

    if not os.path.exists('requirements.txt'):
        raise ValueError(
            'No requirements file found for generating a docker image. Please '
            'consider using pip-tools to generate a requirements file.')

    checksum = hash_dir('.')
    image_name = _image_cache.get_image(checksum)
    if image_name is not None:
        logging.info(
            ('Found existing docker image for source at: %s. '
             'If this is unexpected you can clear the cache by deleting: %s'),
            image_name, _CACHE_LOCATION)
        return image_name

    file_contents = _DOCKER_FILE_WITH_REQUIREMENTS
    major, minor, _ = platform.python_version_tuple()
    file_contents = file_contents.format(py_version=f'{major}.{minor}',
                                         beam_version=beam.__version__)
    docker_file_name = 'Dockerfile.workerimage'
    image_name = f'gcr.io/datascience-sdk/ds-sdk-docker-dev/{job_name}'

    client = cloudbuild.CloudBuildClient()

    build = cloudbuild.Build()

    dockerfile_args = f'echo "{file_contents}" > /workspace/{docker_file_name}'
    build_args = (
        f'build --network=cloudbuild -t {image_name} -f {docker_file_name} .')

    build.steps = [
        cloudbuild.BuildStep(
            name='ubuntu',
            entrypoint='bash',
            args=['-c', dockerfile_args],
        ),
        cloudbuild.BuildStep(
            name='gcr.io/cloud-builders/docker',
            args=build_args.split(' '),
        ),
    ]
    build.images = [image_name]

    build.source = cloudbuild.Source(storage_source=cloudbuild.StorageSource(
        bucket='datascience-sdk_cloudbuild',
        object=_upload_source_tar(job_name),
    ))

    operation = client.create_build(project_id='datascience-sdk',
                                    build=build,
                                    timeout=1200)

    logging.info('Logs can be found at: %s', operation.metadata.build.log_url)

    # Kicks off cloud build, waits 40min
    operation.result(timeout=2400)

    logging.info('Succesfully created and uploaded image to: %s', image_name)

    _image_cache.add_image(checksum, image_name)

    return image_name
