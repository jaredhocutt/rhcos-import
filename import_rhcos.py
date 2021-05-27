#!/usr/bin/env python3

import argparse
import gzip
import json
import logging
import logging.config
import os
import re
import shutil
import tempfile
import time

import boto3
import requests


LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'simple': {
            'format':
                '%(asctime)-8s | %(levelname)-8s | %(name)-10s | %(message)s',
            'datefmt': '%H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': os.environ.get('IMPORT_RHCOS_LOGLEVEL', 'INFO'),
            'formatter': 'simple',
        },
    },
    'loggers': {
        'app': {
            'level': os.environ.get('IMPORT_RHCOS_LOGLEVEL', 'INFO'),
            'handlers': [
                'console',
            ],
            'propagate': 'no',
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('app')


class OpenShiftRelease(object):
    def __init__(self, version, s3_bucket):
        self.version = version
        self.s3_bucket = s3_bucket

        self._data = None
        self._rhcos_filename = None
        self._rhcos_path = None
        self._rhcos_url = None
        self._rhcos_version = None

    @property
    def data(self):
        if not self._data:
            r = requests.get('http://mirror.openshift.com/pub/openshift-v4/clients/ocp/{}/release.txt'.format(self.version))
            self._data = r.text

        return self._data

    @property
    def rhcos_version(self):
        if not self._rhcos_version:
            # Find the RHCOS version for the release
            m = re.search(r'machine-os ([\d\.\-]+)', self.data, re.MULTILINE)
            if not m:
                logger.info('Unable to find RHCOS version for {}'.format(self.version))
                return None

            self._rhcos_version = m.group(1)
            logger.info('RHCOS version {}'.format(self._rhcos_version))

        return self._rhcos_version

    @property
    def rhcos_url(self):
        # return 'http://mirror.openshift.com/pub/openshift-v4/dependencies/rhcos/4.7/4.7.7/rhcos-4.7.7-x86_64-aws.x86_64.vmdk.gz'

        if not self._rhcos_url:
            base_url = 'https://releases-art-rhcos.svc.ci.openshift.org/art/storage/releases'
            self._rhcos_url = '/'.join([
                base_url,
                'rhcos-{}'.format('.'.join(self.version.split('.')[0:2])),
                self.rhcos_version,
                'x86_64',
                '{}.gz'.format(self.rhcos_filename),
            ])

        return self._rhcos_url

    @property
    def rhcos_filename(self):
        if not self._rhcos_filename:
            self._rhcos_filename = 'rhcos-{}-aws.x86_64.vmdk'.format(self.rhcos_version)

        return self._rhcos_filename
    @property
    def rhcos_path(self):
        if not self._rhcos_path:
            self._rhcos_path = os.path.join(tempfile.gettempdir(), self.rhcos_filename)

        return self._rhcos_path

    def download_rhcos(self):
        if os.path.exists(self.rhcos_path):
            logger.info('Skipping download because {} already exists'.format(self.rhcos_path))
            return

        logger.info('Downloading {}'.format(self.rhcos_url))
        r = requests.get(self.rhcos_url)

        rhcos_gzip_path = '{}.gz'.format(self.rhcos_path)

        with open(rhcos_gzip_path, 'wb') as f:
            logger.info('Saving {}'.format(rhcos_gzip_path))
            f.write(r.content)

        rhcos_size = os.path.getsize(rhcos_gzip_path)
        rhcos_size_mb = rhcos_size / (1024 * 1024)
        if rhcos_size_mb < 100:
            raise RuntimeError('RHCOS file size too small {} bytes ({} MB)'.format(rhcos_size, rhcos_size_mb))

        logger.info('Unpacking {}'.format(rhcos_gzip_path))
        with gzip.open(rhcos_gzip_path, 'rb') as f_in:
            with open(self.rhcos_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(rhcos_gzip_path)

    def upload_rhcos(self):
        s3 = boto3.client('s3')

        if s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.rhcos_filename).get('KeyCount', 0) > 0:
            logger.info('Skipping upload because s3://{}/{} already exists'.format(self.s3_bucket, self.rhcos_filename))
            return

        self.download_rhcos()

        with open(self.rhcos_path, 'rb') as f:
            logger.info('Uploading {} to S3'.format(self.rhcos_path))
            s3.upload_fileobj(f, self.s3_bucket, self.rhcos_filename)
        os.remove(self.rhcos_path)

    def import_snapshot(self):
        self.upload_rhcos()

        description = 'rhcos-{}'.format(self.rhcos_version)

        ec2 = boto3.client('ec2')

        existing_snapshots = ec2.describe_snapshots(
            Filters=[
                {
                    'Name': 'tag:rhcos_version',
                    'Values': [self.rhcos_version],
                }
            ],
            OwnerIds=['self'],
        )
        if len(existing_snapshots['Snapshots']) > 0:
            snapshot_id = existing_snapshots['Snapshots'][0]['SnapshotId']
            logger.info('Skipping snapshot creation because {} already exists'.format(snapshot_id))
            return snapshot_id

        logger.info('Importing snapshot from s3://{}/{}'.format(self.s3_bucket, self.rhcos_filename))
        import_task_id = ec2.import_snapshot(
            Description=description,
            DiskContainer={
                'Description': description,
                'Format': 'vmdk',
                'UserBucket': {
                    'S3Bucket': self.s3_bucket,
                    'S3Key': self.rhcos_filename,
                },
            },
        )['ImportTaskId']

        time_elapsed = 0
        while True:
            logger.info('Checking status of snapshot import task {}'.format(import_task_id))
            snapshot_task = ec2.describe_import_snapshot_tasks(
                ImportTaskIds=[import_task_id],
            )

            if snapshot_task['ImportSnapshotTasks'][0]['SnapshotTaskDetail']['Status'] == 'completed':
                snapshot_id = snapshot_task['ImportSnapshotTasks'][0]['SnapshotTaskDetail']['SnapshotId']

                logger.info('Snapshot {} created'.format(snapshot_id))
                logger.info('Tagging snapshot {} with rhcos_version={}'.format(snapshot_id, self.rhcos_version))

                ec2.create_tags(
                    Resources=[snapshot_id],
                    Tags=[
                        {
                            'Key': 'rhcos_version',
                            'Value': self.rhcos_version,
                        }
                    ],
                )

                return snapshot_id

            logger.info('Snapshot import task {} not complete, waiting 10 seconds to try again'.format(import_task_id))

            time_elapsed += 10
            time.sleep(10)

            if time_elapsed > 60 * 5:
                raise RuntimeError('More than 5 minutes have passed and snapshot import task {} has not completed'.format(import_task_id))

    def register_image(self):
        snapshot_id = self.import_snapshot()

        ec2 = boto3.client('ec2')

        existing_images = ec2.describe_images(
            Filters=[
                {
                    'Name': 'name',
                    'Values': ['rhcos-{}'.format(self.rhcos_version)],
                }
            ],
            Owners=['self'],
        )
        if len(existing_images['Images']) > 0:
            image_id = existing_images['Images'][0]['ImageId']
            logger.info('Skipping image creation because {} already exists'.format(image_id))
            return image_id

        logger.info('Registering image from snapshot {}'.format(snapshot_id))

        image_id = ec2.register_image(
            Name='rhcos-{}'.format(self.rhcos_version),
            Description='OpenShift 4 {}'.format(self.rhcos_version),
            Architecture='x86_64',
            BlockDeviceMappings=[
                {
                    'DeviceName': '/dev/xvda',
                    'Ebs': {
                        'SnapshotId': snapshot_id,
                        'DeleteOnTermination': True,
                        'VolumeType': 'gp2',
                    }
                },
                {
                    'DeviceName': '/dev/xvdb',
                    'VirtualName': 'ephemeral0',
                },
            ],
            EnaSupport=True,
            RootDeviceName='/dev/xvda',
            SriovNetSupport='simple',
            VirtualizationType='hvm',
        )['ImageId']

        logger.info('Created image {}'.format(image_id))

        logger.info('Making image {} public'.format(image_id))
        ec2.modify_image_attribute(
            ImageId=image_id,
            LaunchPermission={
                'Add': [
                    {
                        'Group': 'all',
                    },
                ],
            }
        )

        return image_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('versions_to_upload', nargs='+')
    known_args, extra_args = parser.parse_known_args()

    openshift_versions = []
    versions_to_upload = known_args.versions_to_upload


    for i in versions_to_upload:
        r = requests.get(
            'https://api.openshift.com/api/upgrades_info/v1/graph',
            params={
                'channel': 'stable-{}'.format(i),
            },
            headers={
                'Accept': 'application/json',
            }
        )

        data=r.json()
        for node in data['nodes']:
            version = node['version']
            if '.'.join(version.split('.')[0:2]) in versions_to_upload and re.search(r'^\d+\.\d+\.\d+$', version):
                if version not in openshift_versions:
                    openshift_versions.append(version)

    openshift_versions.sort(reverse=True, key=lambda s: list(map(int, s.split('.'))))

    for v in openshift_versions:
        logger.info('Processing OpenShift {}'. format(v))

        try:
            release = OpenShiftRelease(v, 'io-rhdt-govcloud-vmimport')
            release.register_image()
        except Exception as e:
            logger.error(e)
