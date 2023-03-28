import os
import uuid
import boto3


def create_bucket_name(bucket_prefix):
    """
    bucket_prefix : str
        a prefix to be added before uuid
    """
    return ''.join([bucket_prefix, '-', str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    """
    creates a s3 bucket

    bucket_prefix : str
        the prefix for the bucket's name
    s3_connection : obj
        the connection type client/resource (could be either low level or high level)

    Returns
    bucket_name : str
    bucket_response : dict
        information about the created s3-bucket
    """
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response


def create_temp_file(size, file_name, file_content):
    """
        size : int
            number of times the file_content will be multiplied inside the same file
        file_name : str
            prefix for your unique file name
        file_content : str
            data that will be inserted to the file

    Returns
        variable containing a file with a unique name
    """
    random_file_name = 'tmp_'+str(uuid.uuid4().hex[:6]) + '_' + file_name
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


def enable_bucket_versioning(bucket_name):
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)


def delete_all_objects(bucket_name):
    res = []
    bucket = s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})


if __name__ == "__main__":

    s3_resource = boto3.resource('s3',
                               aws_access_key_id='',
                               aws_secret_access_key='',
                               region_name='eu-west-1')
    try:
        bucket_one_name, first_response = create_bucket(
            bucket_prefix='first',
            s3_connection=s3_resource.meta.client)
        print("First bucket created: ", bucket_one_name, first_response)
    except Exception as e:
        print("First bucket creation exception:", str(e))

    try:
        bucket_two_name, second_response = create_bucket(
            bucket_prefix='second', s3_connection=s3_resource)
        print("Second bucket created: ", bucket_two_name, second_response)
    except Exception as e:
        print("Second bucket creation exception:", str(e))


    print('\nCreating a temporary file')
    first_tmp_file = create_temp_file(300, 'firstfile.txt', 'f')
    print('Temporary file name', first_tmp_file)

    print('\nUploading file', first_tmp_file,' to the bucket ', bucket_one_name)
    bucket_one = s3_resource.Bucket(name=bucket_one_name)
    file2upload = s3_resource.Object(
        bucket_name=bucket_one_name, key=first_tmp_file)
    try:
        file2upload.upload_file(first_tmp_file)
    except Exception as e:
        print("An exception has occurred:", str(e))

    print('\nDownloading file', first_tmp_file,' from the bucket ', bucket_one_name)
    try:
        s3_resource.Object(bucket_one_name, first_tmp_file).download_file(
            f'/tmp/{first_tmp_file}')
    except Exception as e:
        print("An exception has occurred:", str(e))

    print('\nCopying file', first_tmp_file, ' from the bucket ', bucket_one_name, 'to the bucket', bucket_two_name)
    copy_to_bucket(bucket_one_name, bucket_two_name, first_tmp_file)

    print('\nDeleting file', first_tmp_file, ' from the bucket ', bucket_two_name)
    s3_resource.Object(bucket_two_name, first_tmp_file).delete()

    print('\nCreating a second temporary file')
    second_tmp_file = create_temp_file(400, 'secondfile.txt', 's')
    print('Second temporary file name', first_tmp_file)

    second_file2upload = s3_resource.Object(bucket_one.name, second_tmp_file)
    second_file2upload.upload_file(second_tmp_file, ExtraArgs={
        'ACL': 'public-read'})
    print('Second temporary file uploaded to', bucket_one.name)

    second_file_acl = second_file2upload.Acl()
    print('Second file ACL:', second_file_acl.grants)

    print('\nSetting permission to private for the file', second_tmp_file)
    response = second_file_acl.put(ACL='private')
    print('Second file grants:', second_file_acl.grants)

    third_tmp_file = create_temp_file(300, 'thirdfile.txt', 't')
    print('Uploading', third_tmp_file, 'to',bucket_one_name, 'with encryption')
    third_file2upload = s3_resource.Object(bucket_one_name, third_tmp_file)
    third_file2upload.upload_file(third_tmp_file, ExtraArgs={
        'ServerSideEncryption': 'AES256'})
    print(third_file2upload.server_side_encryption)

    print('Uploading', third_tmp_file, 'with STANDARD_IA and encryption')
    third_file2upload.upload_file(third_tmp_file, ExtraArgs={
        'ServerSideEncryption': 'AES256',
        'StorageClass': 'STANDARD_IA'})
    third_file2upload.reload()
    print('Third tmp file storage class:', third_file2upload.storage_class)

    print('Enabling versioning on', bucket_one_name)
    enable_bucket_versioning(bucket_one_name)

    s3_resource.Object(bucket_one_name, second_tmp_file).upload_file(
        second_tmp_file)
    print('Version ID of ', first_tmp_file, ' within ',bucket_one_name,':', s3_resource.Object(bucket_one_name, first_tmp_file).version_id)

    print('\nListing all buckets using resource')
    for bucket in s3_resource.buckets.all():
        print(bucket.name)

    print('\nListing all buckets using client')
    for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
        print(bucket_dict['Name'])

    print('\nListing all filenames in the first bucket')
    for obj in bucket_one.objects.all():
        print(obj.key)

    print('\nDetailed listing of all files in the first bucket')
    for obj in bucket_one.objects.all():
        subsrc = obj.Object()
        print(obj.key, obj.storage_class, obj.last_modified,
              subsrc.version_id, subsrc.metadata)

    print('\nEmptying the first bucket')
    try:
        delete_all_objects(bucket_one_name)
    except Exception as e:
        print("Exception on deleting all objects from the first bucket:", str(e))

    print('\nEmptying the second bucket')
    try:
        delete_all_objects(bucket_two_name)
    except Exception as e:
        print("Exception on deleting all objects from the second bucket:", str(e))

    print('\nDeleting first and second bucket')
    try:
        s3_resource.Bucket(bucket_one_name).delete()
        s3_resource.meta.client.delete_bucket(Bucket=bucket_two_name)
    except Exception as e:
        print("Exception on deletion of the first and second bucket:", str(e))

    print('Deleting local temporary files')
    os.remove(first_tmp_file)
    os.remove(second_tmp_file)
    os.remove(third_tmp_file)

