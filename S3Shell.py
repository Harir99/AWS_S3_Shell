import boto3
from botocore.exceptions import NoCredentialsError
import os


class S3Shell:
    def __init__(self):
        self.s3 = None
        self.current_bucket = None
        self.current_path = '/'  # Assuming the root as the starting path
        self.read_config()
        self.check_s3_connection()

    @staticmethod
    def read_config():
        try:
            # Read AWS credentials from the configuration file
            with open("S5-S3.conf", "r") as file:
                lines = file.readlines()
                # create a dictionary, assuming each line contains key-value pairs separated by '='
                credentials = {line.split('=')[0].strip(): line.split('=')[
                    1].strip() for line in lines}
                return credentials

        except FileNotFoundError:
            print(
                "Configuration file (S5-S3.conf) not found. Please create the file with your AWS credentials.")
            exit(1)
        except Exception as e:
            print(f"Error loading configuration: {str(e)}")
            exit(1)

    def check_s3_connection(self):
        credentials = self.read_config()
        if credentials:
            try:
                # Create an S3 client using the credentials from the configuration file
                self.s3 = boto3.client('s3', aws_access_key_id=credentials['aws_access_key_id'],
                                       aws_secret_access_key=credentials['aws_secret_access_key'])

                print(
                    "Welcome to the AWS S3 Storage Shell (S5)\nYou are now connected to your S3 storage")

            except Exception as e:
                print(
                    "Please review procedures for authenticating your account on AWS S3")
                print(f"Error: {str(e)}")
                exit(1)
        else:
            return False

    def create_bucket(self, command):
        try:
            parts = command.split()
            if not parts or not parts[0].startswith('/'):
                raise ValueError(
                    "Invalid command format. Use 'create_bucket /<bucket name>'.")
            bucket_name = parts[0][1:]
            self.s3.create_bucket(Bucket=bucket_name)
            self.current_bucket = bucket_name  # Set the current_bucket attribute
            return 0
        except Exception as e:
            print(f"Cannot create bucket. Error: {str(e)}")
            return 1

    def list(self, path=None):
        try:
            if not self.current_bucket and (path is None or path == '/'):
                response = self.s3.list_buckets()
                for bucket in response['Buckets']:
                    print(f"/{bucket['Name']}")
                return 0

            if not self.current_bucket:
                raise ValueError(
                    "You are not located in a bucket. Cannot list contents.")

            resource_s3 = boto3.resource('s3', aws_access_key_id=self.s3._request_signer._credentials.access_key,
                                         aws_secret_access_key=self.s3._request_signer._credentials.secret_key)
            bucket = resource_s3.Bucket(self.current_bucket)

            for obj in bucket.objects.all():
                print(obj.key)

            return 0

        except NoCredentialsError:
            print(
                "Credentials not available. Please review procedures for authenticating your account on AWS S3.")
            return 1
        except Exception as e:
            print(f"Cannot list contents. Error: {str(e)}")
            return 1

    def locs3cp(self, local_path, s3_path):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        try:
            if not self.current_bucket:
                raise ValueError(
                    "You are not located in a bucket. Cannot copy to S3.")

            # Check if the local file exists
            if not os.path.exists(local_path):
                raise FileNotFoundError(
                    f"Local file '{local_path}' not found.")

            # Normalize the S3 location to ensure it starts with a '/'
            s3_path = s3_path if s3_path.startswith('/') else '/' + s3_path

            # Extract bucket name and object path from the S3 location
            s3_parts = s3_path.split('/')
            s3_bucket = s3_parts[1]
            s3_object_path = '/'.join(s3_parts[2:])

            # Upload the local file to the specified S3 location
            self.s3.upload_file(local_path, s3_bucket, s3_object_path)

            print(
                f"Successfully copied '{local_path}' to S3 location '{s3_path}'.")
            return 0
        except FileNotFoundError as e:
            print(f"Unsuccessful copy. Error: {str(e)}")
            return 1
        except Exception as e:
            print(f"Unsuccessful copy. Error: {str(e)}")
            return 1

    def chlocn(self, path):
        try:
            if path.startswith('/'):
                # Absolute path provided
                parts = path.split('/')
                self.current_bucket = parts[1]
                self.current_path = '/' + '/'.join(parts[2:]) if len(parts) > 2 else '/'
            elif path.startswith('..'):
                # Go up one or more levels
                if self.current_path != '/':
                    parts = self.current_path.strip('/').split('/')
                    levels = path.count('..')
                    self.current_path = '/' + '/'.join(parts[:-levels]) + '/' if len(parts) > levels else '/'
                else:
                    # Special case for going up from root
                    self.current_path = '/'
                    self.current_bucket = None
            else:
                # Relative path provided
                if self.current_path != '/':
                    _, *path_parts = path.split(' ')
                    path = ' '.join(path_parts)
                    self.current_path = '/' + os.path.join(self.current_path.strip('/'), path).replace('\\', '/')

            return 0
        except Exception as e:
            print(f"Cannot change folder. Error: {str(e)}")
            return 1

    def cwlocn(self):
        try:
            if self.current_bucket:
                if self.current_path == "/":
                    location = f"/{self.current_bucket}"
                else:
                    location = f"/{self.current_bucket}{self.current_path}"
                print(location)
            else:
                location = '/'
                print(location)
            return 0
        except Exception as e:
            print(f"Cannot access location in S3 space. Error: {str(e)}")
            return 1

    def run_shell(self):
        while True:
            command = input("S5> ")
            if command.lower() in ['quit', 'exit']:
                print("Exiting AWS S3 Storage Shell (S5)")
                break
            elif command.lower().startswith('create_bucket'):
                _, bucket_name = command.split(' ')
                self.create_bucket(bucket_name)
            elif command.lower().startswith('chlocn'):
                _, *path = command.split(' ')
                path = ' '.join(path)
                self.chlocn(path)
            elif command.lower().startswith('cwlocn'):
                self.cwlocn()
            elif command.lower().startswith('list'):
                _, path = command.split(' ', 1)
                self.list(path)
            elif command.lower().startswith('locs3cp'):
                _, local_path, s3_path = command.split(' ', 2)
                self.locs3cp(local_path, s3_path)
            else:
                # Pass non-Cloud related commands to the session's shell
                os.system(command)


if __name__ == "__main__":
    s3_shell = S3Shell()
    s3_shell.run_shell()
