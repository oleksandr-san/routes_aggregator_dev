import os
import os.path
import boto3
from botocore.client import Config

from routes_aggregator.model import ModelAccessor


class StorageAdapter:

    def __init__(self):
        pass

    def prepare_filename(self, agent_type):
        return agent_type + '.data'

class FilesystemStorageAdapter(StorageAdapter):

    def __init__(self, base_path):
        super().__init__()

        self.base_path = base_path

    def prepare_object_name(self, agent_type, object_name):
        return os.path.join(
            self.base_path,
            object_name,
            self.prepare_filename(agent_type)
        )

    def save_model(self, model, object_name):
        with open(self.prepare_object_name(model.agent_type, object_name), 'wb') as fileobj:
            model.save_binary(fileobj)

    def load_model(self, agent_type, object_name):
        model = ModelAccessor()
        with open(self.prepare_object_name(model.agent_type, object_name), 'rb') as fileobj:
            model.save_binary(fileobj)
        return model


class S3StorageAdapter(StorageAdapter):

    def __init__(self, credentials):
        super().__init__()

        self.__credentials = credentials
        self.__client = None

    @property
    def client(self):
        if not self.__client:
            self.__client = boto3.client(
                's3',
                aws_access_key_id=self.__credentials[0],
                aws_secret_access_key=self.__credentials[1],
                config=Config(signature_version='s3v4'))
        return self.__client

    def prepare_object_name(self, agent_type, object_name):
        if not object_name.endswith('/'):
            object_name += '/'
        object_name += agent_type + '.data'
        return object_name

    def save_model(self, model, object_name):
        with open('temp.data', 'wb') as fileobj:
            model.save_binary(fileobj)
        with open('temp.data', 'rb') as fileobj:
            self.client.upload_fileobj(
                fileobj,
                'routes-aggregator',
                self.prepare_object_name(model.agent_type, object_name)
            )
        os.remove('temp.data')

    def load_model(self, agent_type, object_name):
        model = ModelAccessor()
        with open('temp.data', 'wb') as fileobj:
            self.client.download_fileobj(
                'routes-aggregator',
                self.prepare_object_name(agent_type, object_name),
                fileobj
            )
        with open('temp.data', 'rb') as fileobj:
            model.restore_binary(fileobj)
        os.remove('temp.data')
        return model