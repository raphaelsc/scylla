#!/usr/bin/python3

import os
import sys
import logging
import pytest
import shutil
import tempfile

# use minio_server
sys.path.insert(1, sys.path[0] + '/../..')
from test.pylib.minio_server import MinioServer
from test.pylib.cql_repl import conftest


def pytest_addoption(parser):
    conftest.pytest_addoption(parser)
    # reserved for tests with real S3
    s3_options = parser.getgroup("s3-server", description="S3 Server settings")
    s3_options.addoption('--s3-server-address')
    s3_options.addoption('--s3-server-port', type=int)
    s3_options.addoption('--s3-server-bucket')


class S3_Server:
    def __init__(self, tempdir: str, address: str, port: int, bucket_name):
        self.tempdir = tempdir
        self.address = address
        self.port = port
        self.bucket_name = bucket_name
        self.config_file = self._get_config_file()

    def _get_config_file(self):
        # if the test is started by test.py, minio_server.py should set this
        # env variable for us, but if the test is started manually, there are
        # chances that this env variable is not set, we would have to create it
        # by ourselves, so the tests can consume it.
        conffile = os.environ.get(MinioServer.ENV_CONFFILE)
        if conffile is None:
            conffile = os.path.join(self.tempdir, 'object-storage.yaml')
            MinioServer.create_conf_file(self.address, self.port, conffile)
        return conffile

    async def start(self):
        pass

    async def stop(self):
        pass


@pytest.fixture(scope="function")
def ssl(request):
    yield request.config.getoption('--ssl')


def _remove_all_but(tempdir, to_preserve):
    orig_fn = os.path.join(tempdir, to_preserve)
    # orig_fn does not exist
    if not os.path.exists(orig_fn):
        # it's fine if tempdir does not exist
        shutil.rmtree(tempdir, ignore_errors=True)
        return

    with tempfile.TemporaryDirectory() as backup_tempdir:
        backup_fn = os.path.join(backup_tempdir, to_preserve)
        shutil.move(orig_fn, backup_fn)
        shutil.rmtree(tempdir)
        os.mkdir(tempdir)
        shutil.move(backup_fn, orig_fn)


@pytest.fixture(scope="function")
def test_tempdir(tmpdir):
    tempdir = tmpdir.strpath
    try:
        yield tempdir
    finally:
        _remove_all_but(tempdir, 'log')


@pytest.fixture(scope="function")
async def s3_server(pytestconfig, tmpdir):
    server = None
    s3_server_address = pytestconfig.getoption('--s3-server-address')
    s3_server_port = pytestconfig.getoption('--s3-server-port')
    s3_server_bucket = pytestconfig.getoption('--s3-server-bucket')

    default_address = os.environ.get(MinioServer.ENV_ADDRESS)
    default_port = os.environ.get(MinioServer.ENV_PORT)
    default_bucket = os.environ.get(MinioServer.ENV_BUCKET)

    tempdir = tmpdir.strpath
    if s3_server_address:
        server = S3_Server(tempdir,
                           s3_server_address,
                           s3_server_port,
                           s3_server_bucket)
    elif default_address:
        server = S3_Server(tempdir,
                           default_address,
                           int(default_port),
                           default_bucket)
    else:
        server = MinioServer(tempdir,
                             '127.0.0.1',
                             logging.getLogger('minio'))
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
