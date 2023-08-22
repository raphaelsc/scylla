#!/usr/bin/python3
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Minio server for testing.
   Provides helpers to setup and manage minio server for testing.
"""
import os
import argparse
import asyncio
from asyncio.subprocess import Process
from typing import Generator, Optional
import json
import logging
import pathlib
import random
import subprocess
import shutil
import time
import tempfile
import socket
import yaml
from io import BufferedWriter

class MinioServer:
    ENV_CONFFILE = 'S3_CONFFILE_FOR_TEST'

    log_file: BufferedWriter

    def __init__(self, tempdir_base, address, logger):
        self.srv_exe = shutil.which('minio')
        self.address = address
        self.port = None
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="minio-")
        self.tempdir = pathlib.Path(tempdir)
        self.rootdir = self.tempdir / 'minio_root'
        self.config_file = self.tempdir / 'object-storage.yaml'
        self.mcdir = self.tempdir / 'mc'
        self.logger = logger
        self.cmd: Optional[Process] = None
        self.default_user = 'minioadmin'
        self.default_pass = 'minioadmin'
        self.bucket_name = 'testbucket'
        self.log_filename = (self.tempdir / 'minio').with_suffix(".log")

    def check_server(self, port):
        s = socket.socket()
        try:
            s.connect((self.address, port))
            return True
        except socket.error:
            return False
        finally:
            s.close()

    def log_to_file(self, str):
        self.log_file.write(str.encode())
        self.log_file.write('\n'.encode())
        self.log_file.flush()

    async def mc(self, *args, ignore_failure=False, timeout=0):
        retry_until: float = time.time() + timeout
        retry_step: float = 0.1
        cmd = ['mc',
               '--debug',
               '--config-dir', self.mcdir]
        cmd.extend(args)

        while True:
            try:
                subprocess.check_call(cmd, stdout=self.log_file, stderr=self.log_file)
            except subprocess.CalledProcessError:
                if ignore_failure:
                    self.log_to_file('ignoring')
                    break
                command = ' '.join(args)
                self.log_to_file(f'failed to run "mc {command}"')
                if time.time() >= retry_until:
                    raise
                self.log_to_file(f'retry after {retry_step} seconds')
                await asyncio.sleep(retry_step)
            else:
                break

    def _anonymous_public_policy(self):
        # the default anonymous public policy does not allow us to access
        # the taggings, so let's add the tagging actions manually.
        #
        # the original access policy is dumped using:
        #   mc anonymous set public local/testbucket
        #   mc anonymous get-json local/testbucket
        #
        # we added following actions to the policy for accessing objects in the
        # bucket created for testing:
        # - GetObjectTagging
        # - PutObjectTagging
        # - DeleteObjectTagging
        #
        # the full list of actions can be found at
        #   https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html
        bucket_actions = [
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads",
            "s3:GetBucketLocation",
        ]
        object_actions = [
            "s3:AbortMultipartUpload",
            "s3:DeleteObject",
            "s3:GetObject",
            "s3:ListMultipartUploadParts",
            "s3:PutObject",
            "s3:GetObjectTagging",
            "s3:PutObjectTagging",
            "s3:DeleteObjectTagging"
        ]
        statement = [
            {
                'Action': bucket_actions,
                'Effect': 'Allow',
                'Principal': {'AWS': ['*']},
                'Resource': [ f'arn:aws:s3:::{self.bucket_name}' ]
            },
            {
                'Action': object_actions,
                'Effect': 'Allow',
                'Principal': {'AWS': ['*']},
                'Resource': [ f'arn:aws:s3:::{self.bucket_name}/*' ]
            }
        ]
        return {'Statement': statement,
                'Version': '2012-10-17'}

    def _get_local_ports(self, num_ports: int) -> Generator[int, None, None]:
        with open('/proc/sys/net/ipv4/ip_local_port_range', encoding='ascii') as port_range:
            min_port, max_port = map(int, port_range.read().split())
        for _ in range(num_ports):
            yield random.randint(min_port, max_port)

    @staticmethod
    def create_conf_file(address: str, port: int, path: str):
        with open(path, 'w', encoding='ascii') as config_file:
            endpoint = {'name': address,
                        'port': port}
            yaml.dump({'endpoints': [endpoint]}, config_file)

    async def _run_server(self, port):
        self.logger.info(f'Starting minio server at {self.address}:{port}')
        cmd = await asyncio.create_subprocess_exec(
            self.srv_exe,
            *[ 'server', '--address', f'{self.address}:{port}', self.rootdir ],
            preexec_fn=os.setsid,
            stderr=self.log_file,
            stdout=self.log_file,
        )
        timeout = time.time() + 30
        while time.time() < timeout:
            if cmd.returncode is not None:
                # the minio server exits before it starts to server. maybe the
                # port is used by another server?
                self.logger.info('minio exited with %s', cmd.returncode)
                raise RuntimeError("Failed to start minio server")
            if self.check_server(port):
                self.logger.info('minio is up and running')
                break

            await asyncio.sleep(0.1)

        return cmd

    async def start(self):
        if self.srv_exe is None:
            self.logger.info("Minio not installed, get it from https://dl.minio.io/server/minio/release/linux-amd64/minio and put into PATH")
            return

        self.log_file = self.log_filename.open("wb")
        os.mkdir(self.rootdir)

        retries = 42  # just retry a fixed number of times
        for port in self._get_local_ports(retries):
            try:
                self.cmd = await self._run_server(port)
                self.port = port
            except RuntimeError:
                pass
            else:
                break
        else:
            self.logger.info("Failed to start Minio server")
            return

        self.create_conf_file(self.address, self.port, self.config_file)
        os.environ[self.ENV_CONFFILE] = f'{self.config_file}'
        os.environ['S3_SERVER_ADDRESS_FOR_TEST'] = f'{self.address}'
        os.environ['S3_SERVER_PORT_FOR_TEST'] = f'{self.port}'
        os.environ['S3_PUBLIC_BUCKET_FOR_TEST'] = f'{self.bucket_name}'

        try:
            alias = 'local'
            self.log_to_file(f'Configuring access to {self.address}:{self.port}')
            await self.mc('config', 'host', 'rm', alias, ignore_failure=True)
            # wait for the server to be ready when running the first command which should not fail
            await self.mc('config', 'host', 'add', alias, f'http://{self.address}:{self.port}', self.default_user, self.default_pass, timeout=30)
            self.log_to_file(f'Configuring bucket {self.bucket_name}')
            await self.mc('mb', f'{alias}/{self.bucket_name}')
            with tempfile.NamedTemporaryFile(mode='w', encoding='UTF-8', suffix='.json') as policy_file:
                json.dump(self._anonymous_public_policy(), policy_file, indent=2)
                policy_file.flush()
                await self.mc('anonymous', 'set-json', policy_file.name, f'{alias}/{self.bucket_name}')

        except Exception as e:
            self.logger.info(f'MC failed: {e}')
            await self.stop()

    async def stop(self):
        self.logger.info('Killing minio server')
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            self.logger.info('Killed minio server')
            self.cmd = None
            shutil.rmtree(self.tempdir)


async def main():
    parser = argparse.ArgumentParser(description="Start a MinIO server")
    parser.add_argument('--tempdir')
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(suffix='-minio', dir=args.tempdir) as tempdir:
        if args.tempdir is None:
            print(f'{tempdir=}')
        server = MinioServer(tempdir, args.host, logging.getLogger('minio'))
        await server.start()
        print(f'export S3_SERVER_ADDRESS_FOR_TEST={server.address}')
        print(f'export S3_SERVER_PORT_FOR_TEST={server.port}')
        print(f'export S3_PUBLIC_BUCKET_FOR_TEST={server.bucket_name}')
        print(f'Please run scylla with: --object-storage-config-file {server.config_file}')
        try:
            _ = input('server started. press any key to stop: ')
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
