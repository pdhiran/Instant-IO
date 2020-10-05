import logging
import json
import boto
import collections
from subprocess import PIPE, Popen
import time
import sys
import re
import os
import boto.s3.connection

with open('config.json', 'r') as fd:
    config = json.loads(fd.read())
log = logging.getLogger(__name__)
unique_id = time.strftime("%Y%m%d%H%M%S")
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=config['logging'], filename=f"log_IO_{unique_id}.txt", format=log_format)
stdout_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stdout_handler)


def cmdline(command):
    """
    handy method to execute the Shell commands
    :param command: shell command to be executed
    :return:
    """
    process = Popen(
        args=command,
        stdout=PIPE,
        shell=True
    )
    return process.communicate()[0].decode()


def count(func):
    """
    Decorator method to check how many times a particular method has been invoked
    :param func: name of the function
    :return: wrapped method
    """

    def wrapped(*args, **kwargs):
        wrapped.calls += 1
        return func(*args, **kwargs)

    wrapped.calls = 0
    return wrapped


def collect_hostname():
    """
    Collects the FQDN of the given host using Hostname -A
    :return: FQDN of the given host
    """
    cmd = "hostname -A"
    op = cmdline(cmd)
    log.debug(f"The o/p of all Hostnames : {op}")
    for name in op.split(" "):
        if re.search(r"\.com", name):
            return name.strip()
    # if no FQDN name was found, returning the IP of the host
    op = cmdline("hostname -I | awk '{print $1}'").strip()
    log.debug(f"The o/p of the IP's : {op}")
    return op


class RgwIoTools:
    """
    This class implements the methods required to trigger the Object IO for RGW
    """

    def __init__(self):
        """
        Initializing the connection for the objects
        """
        # self.host = collect_hostname()
        self.host = config['RGW']['rgw_host']
        if config['RGW']['create_rgw_user']:
            log.debug("User creation is set to true, creating a radosgw admin user with keys")
            user = f"operator_{unique_id}"
            disp_name = f"s3 {user}"
            email = f"{user}@example.com"
            self.access_key = unique_id
            self.secret_key = f"{unique_id}0000"

            admin_create_command = f"""radosgw-admin user create --uid="{user}" --display-name="{disp_name}" \
--email="{email}" --access_key="{self.access_key}" --secret="{self.secret_key}" """
            cmdline(admin_create_command)
            log.info(f"admin user for RGW : {user} created successfully")
        else:
            log.debug("User creation is set to false, creating a radosgw admin user provided with keys")
            self.access_key = config['RGW']['access_key']
            self.secret_key = config['RGW']['secret_key']

        self.conn = boto.connect_s3(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            host=self.host,
            port=8080,
            is_secure=False,  # comment if you are using ssl
            calling_format=boto.s3.connection.OrdinaryCallingFormat(), )
        log.debug("successfully created a connection with the Host for IO using BOTO tool")

    def list_buckets(self):
        """
        lists all the buckets created by the given user
        :return: dictionary of all the buckets with the timestamp
        """
        bucket_dictionary = {}
        log.debug("listing all the buckets on the host")
        for bucket in self.conn.get_all_buckets():
            log.info(f"{bucket.name}\t{bucket.creation_date}")
            bucket_dictionary[bucket.name] = bucket.creation_date
        log.debug(f"all the buckets on the host are : {str(bucket_dictionary)}")
        return bucket_dictionary

    def create_buckets(self, quantity):
        """
        Creates the buckets as many as specified in the
        :param quantity: no of buckets to be created
        :return: Returns the list of buckets created
        """
        buckets_list = []
        log.debug("creating buckets for RGW IO")
        for no in range(int(quantity)):
            name = f'my-bucket-{unique_id}-no-{no}'
            log.debug(f"creating bucket : {name}")
            try:
                bucket = self.conn.create_bucket(name)
                buckets_list.append(bucket.name)
            except Exception as err:
                log.error(f"An error occurred when creating the bucket {name}. Error message : \n {err}")
        log.debug(f"all the buckets created are : {str(buckets_list)}")
        return buckets_list

    def list_bucket_content(self, bucket=None):
        """
        Lists the content of the bucket.

        When a bucket name is provided, returns the contents of that particular bucket,
        else lists the contents of all the buckets created by the particular user
        :param bucket: Name of the bucket whose contents need to be listed.
        :return: dictionary of the objects with bucket name with key
        """
        objects_dictionary = {}
        bktobjects = collections.namedtuple('bktobjects', ['name', 'size', 'modified'])
        log.debug("Listing the objects inside the specified bucket(s)")
        if bucket:
            bucket = self.conn.get_bucket(bucket)
            log.debug(f"Indivudial bucket name given. Bucket {bucket.name}")
            key_list = []
            for key in bucket.list():
                log.info(f"bucket : {bucket.name}\t{key.name}\t{key.size}\t{key.last_modified}")
                key_list.append(bktobjects(key.name, key.size, key.last_modified))
            objects_dictionary[bucket.name] = key_list
        else:
            log.debug("listing contents of all the buckets created by user")
            for bucket in self.conn.get_all_buckets():
                # bucket = self.conn.get_bucket(bucket)
                key_list = []
                for key in bucket.list():
                    log.info(f"bucket : {bucket.name}\t{key.name}\t{key.size}\t{key.last_modified}")
                    key_list.append(bktobjects(key.name, key.size, key.last_modified))
                objects_dictionary[bucket.name] = key_list
        log.debug(f"the objects are : {str(objects_dictionary)}")
        return objects_dictionary

    def create_bucket_object(self, bucket, quantity):
        """
        creates the given number of objects inside the given bucket
        :param bucket: name of the bucket where the object needs to be created
        :param quantity: number of objects to be created
        :return: list of all the keys of objects created
        """
        obj_key_list = []
        log.info(f"creating {quantity} objects inside bucket {bucket}")
        bucket = self.conn.get_bucket(bucket)
        for no in range(int(quantity)):
            ukey = f"obj_{unique_id}_no{no}"
            log.debug(f"creating the object no : {no} with key : {ukey}")
            try:
                key = bucket.new_key(ukey)
                copy_string = f"""
                This is a test object being written for the key : {ukey}
                This python script can be used to write IO into the given host.
                """
                key.set_contents_from_string(copy_string)
                obj_key_list.append(ukey)
            except Exception as err:
                log.error(f"An error occurred when creating the object {ukey} in bucket {bucket}."
                          f" Error message : \n {err}")
        log.debug(f"All the keys created are : {str(obj_key_list)}")
        return obj_key_list

    def delete_boto_object(self, bucket, key=None, delete_all=False):
        """
        Deletes the given object from the bucket.

        If the Key is specified, deletes only the object from the bucket, otherwise deletes all the objects from
        the given bucket
        :param bucket: name of the bucket from where the object needs to be deleted
        :param key: name of the key to be deleted.
        :param delete_all: If true, deletes all the objects in the given bucket
        :return: None
        """
        log.info(f"Deleting the object(s) present in the given bucket {bucket}")
        key_list = [key, ]
        bucket = self.conn.get_bucket(bucket)
        if delete_all:
            log.debug(f"selected to delete all the objects in bucket {bucket.name}")
            key_list_dict = self.list_bucket_content(bucket)
            key_list = [ob.name for ob in key_list_dict[bucket]]
            log.debug(f"the keys obtained for bucket {bucket.name} are : {key_list}")

        for key in key_list:
            try:
                bucket.delete_key(key)
            except Exception as err:
                log.error(f"An error occurred when deleting the object {key} in bucket {bucket.name}."
                          f" Error message : \n {err}")
            log.debug(f"Delete the object {key} in bucket {bucket.name}")
        log.info(f"done with deleting object(s) in bucket {bucket.name}")

    def delete_boto_bucket(self, bucket):
        """
        Deletes the empty bucket. If the bucket is not empty, deletes all the objects and then delets bucket
        :param bucket: Name of the bucket to be deleted.
        :return: None
        """
        bucket = self.conn.get_bucket(bucket)
        log.info(f"Bucket provided to be deleted : {bucket.name}")
        contents = self.list_bucket_content(bucket.name)
        if len(contents[bucket.name]) >= 1:
            log.info(f"Bucket {bucket.name} is not empty. Deleting objects before deleting")
            self.delete_boto_object(bucket=bucket.name, delete_all=True)
        try:
            self.conn.delete_bucket(bucket.name)
        except Exception as err:
            log.error(f"An error occurred when deleting bucket {bucket.name}."
                      f" Error message : \n {err}")
        log.info(f"completed deleting bucket {bucket.name}")

    def download_boto_objects(self, bucket, key=None):
        """
        Used to download the object on to local file system simulating read option.

        If Key is specified along with bucket name, only that object will be downloaded, Otherwise all the objects in
        the bucket will be downloaded. Creates a folder called boto_objects and downloads them in the folder.
        :param bucket: Name of the bucket from where to download a object
        :param key: Name of the object to be downloaded
        :return: None
        """

        bucket = self.conn.get_bucket(bucket)
        # creating a folder for downloading the files
        folder_name = f"object_downloads_{unique_id}"
        if not os.path.isdir(folder_name):
            folder_create_cmd = f"mkdir {folder_name}"
            log.debug(f"Creating the folder : {folder_name} via the command : {folder_create_cmd}")
            cmdline(folder_create_cmd)
        log.info(f"Downloading object(s) from the bucket {bucket.name}")
        keys = [key, ]
        if not key:
            log.debug(f"Downloading all the objects from the bucket {bucket.name}")
            bkt_content = self.list_bucket_content(bucket=bucket.name)
            log.debug(f"\n\nDownloading The contents of bucket : {bucket.name}. The list of objects obtained"
                      f" is :\n{str(bkt_content)}\n\n and number of objects is/are {len(bkt_content[bucket.name])}")
            keys = [bkt_content[bucket.name][cnt].name for cnt in range(len(bkt_content[bucket.name]))]
            log.debug(f"All the keys obtained for downloading are : {keys}")

        # Proceeding to download all the keys provided
        for key in keys:
            log.debug(f"Downloading the objects {key} from the bucket {bucket.name}")
            # creating a file to download the contents of the object
            file_name = f"object_{bucket.name}_{key}.txt"
            file_create_cmd = f"touch {folder_name}/{file_name}"
            cmdline(file_create_cmd)
            log.debug(f"the name of the download file is {file_name}, creating file via command : {file_create_cmd}")
            try:
                key = bucket.get_key(key)
                key.get_contents_to_filename(f"{folder_name}/{file_name}")
            except Exception as err:
                log.error(f"An error occurred when downloading the object {key} in bucket {bucket.name}."
                          f" Error message : \n {err}")

    def generate_boto_obj_url(self, bucket, key=None):
        """
        Used to create download URL for the object simulating read option.

        If Key is specified along with bucket name, only for that object the URL will be generated,
         Otherwise all the objects in the bucket will have the download URL's.
        :param bucket: Name of the bucket from where to download a object
        :param key: Name of the object for which URL should be generated
        :return: Returns the list of objects URL's
        """
        bucket = self.conn.get_bucket(bucket)
        log.info(f"Creating URL's for object(s) from the bucket {bucket.name}")
        all_url = []
        keys = [key, ]
        if not key:
            log.debug(f"Downloading all the objects from the bucket {bucket.name}")
            bkt_content = self.list_bucket_content(bucket=bucket.name)
            keys = [bkt_content[bucket.name][cnt].name for cnt in range(len(bkt_content[bucket.name]))]

        # Proceeding to download all the keys provided
        for key in keys:
            log.debug(f"Downloading the objects {key} from the bucket {bucket.name}")
            try:
                key_name = bucket.get_key(key)
                obj_url = key_name.generate_url(0, query_auth=False, force_http=True)
                log.debug(f"The URL generated is : {str(obj_url)} of type {type(obj_url)}")
                all_url.append(obj_url)
            except Exception as err:
                log.error(f"An error occurred when generating URI the object {key} in bucket {bucket.name}."
                          f" Error message : \n {err}")
        return all_url


class RadosIoTools:
    """
    This class implements the methods required to trigger the Object IO via Rados Bench tool
    """

    @count
    def __init__(self):
        """
        Initializing class object by creating a pool for triggering Rados bench
        """
        self.pool_name = f"instant_io_pool_{self.__init__.calls}_{unique_id}"
        # pool_create_cmd = f"sudo ceph osd pool create {self.pool_name} 256 256"
        pool_create_cmd = f"sudo ceph osd pool create {self.pool_name} 64 64"
        log.debug(f"Creating pool : {self.pool_name} using the command : {pool_create_cmd}")
        cmdline(pool_create_cmd)
        enable_app_cmd = f"sudo ceph osd pool application enable {self.pool_name} rados"
        log.debug(f"Enabling rbd application on pool : {self.pool_name} using the command : {enable_app_cmd}")
        cmdline(enable_app_cmd)
        # checking if the pool creation was successful
        all_pools = cmdline("ceph df")
        log.debug(f"All the pools in the cluster : {all_pools}")
        if not all_pools.find(self.pool_name):
            # log.error("failed to create admin user for rados gateway... Exiting")
            log.error(f"failed to create pool {self.pool_name} for rados bench... Exiting")
            exit(100)
        log.info(f"Created pool {self.pool_name} for Rados Bench successfully")

    def bench_write_ops(self, bsize, duration):
        """
        Method to trigger Write operation via the Rados Bench tool
        :param bsize: block size to write
        :param duration: no of seconds to write the bench objects
        :return: None
        """
        # dropping the cache from the system before triggering the test
        cmd = "sudo echo 3 | sudo tee /proc/sys/vm/drop_caches && sudo sync"
        cmdline(cmd)
        log.debug("Performing Normal writes.")
        bench_write_cmd = f"sudo rados --no-log-to-stderr -b {int(bsize)} -p {self.pool_name} " \
                          f"bench {duration} write --no-cleanup"
        op = cmdline(bench_write_cmd)
        log.debug(f"Performed Write on pool {self.pool_name} using command : {cmd} \n  Output :: \n {op} \n")
        log.info(f"finished performing write operation via Rados Bench tool on pool {self.pool_name}")

    def bench_read_ops(self, duration):
        """
        Method to perform sequential and Random reads on using the rados bench tool
        :param duration: no of seconds to read the bench objects
        :return: None
        """
        log.info(f"Performing read operations on the pool {self.pool_name}")
        if config['Rados_Bench']['sequential_read']:
            log.info(f"Performing sequental read operation on the pool {self.pool_name}")
            cmd = f"rados --no-log-to-stderr -p {self.pool_name} bench {duration} seq"
            log.debug(f"Performing sequential read operations on the pool {self.pool_name} using {cmd}")
            op = cmdline(cmd)
            log.debug(f"Performed sequential read on pool {self.pool_name} using command :{cmd} \nOutput :: \n{op}\n")

        if config['Rados_Bench']['random_read']:
            log.info(f"Performing sequental read operation on the pool {self.pool_name}")
            cmd = f"rados --no-log-to-stderr -p {self.pool_name} bench {duration} rand"
            log.debug(f"Performing Random read operations on the pool {self.pool_name} using {cmd}")
            op = cmdline(cmd)
            log.debug(f"Performed sequential read on pool {self.pool_name} using command :{cmd} \nOutput :: \n{op}\n")

        else:
            log.info("Read operations not specified in the config file... Exiting ....")

    def bench_cleanup(self):
        """
        Removes the data created by the rados bench command
        :return: None
        """
        log.info(f"Deleting the objects created in the pool : {self.pool_name}")
        cmd = f"rados -p {self.pool_name} cleanup"
        op = cmdline(cmd)
        log.debug(f"Performed cleanup of pool {self.pool_name} using command : {cmd} \n  Output :: \n {op} \n")
        # cmd = f"ceph osd pool delete {self.pool_name} --yes-i-really-really-mean-it"
        # cmdline(cmd)


class RbdFioTools:
    """
    Class containing modules for running File IO for Rados block devices
    """

    @count
    def __init__(self):
        """
        Performs all the pre-requsits fro running FIO on for testing.

        Steps performed in init:
        1. Create a pool for testing
        2. Create a rbd image in the test pool
        3. Map image to a block device
        4. Make file system
        5. Mount the Ceph rbd image image
        """
        log.debug("Performing pre-requisites for running FIO on the given host")
        # Create a pool for testing
        self.pool_name = f"rbd_io_pool_{self.__init__.calls}_{unique_id}"
        pool_create_cmd = f"sudo ceph osd pool create {self.pool_name} 256 256"
        log.debug(f"Creating pool : {self.pool_name} using the command : {pool_create_cmd}")
        cmdline(pool_create_cmd)

        enable_app_cmd = f"sudo ceph osd pool application enable {self.pool_name} rbd"
        log.debug(f"Enabling rbd application on pool : {self.pool_name} using the command : {enable_app_cmd}")
        cmdline(enable_app_cmd)

        # Creating a image on the given pool
        self.image_name = f"rbd_io_image_{self.__init__.calls}_{unique_id}"
        image_create = f"sudo rbd create {self.image_name} --size 4096 --pool {self.pool_name} --image-feature layering"
        log.debug(f"Creating image : {self.image_name} using the command : {image_create}")
        cmdline(image_create)

        # Mapping the image create to the client
        image_map_cmd = f"sudo rbd map {self.image_name} --pool {self.pool_name} --name client.admin"
        log.debug(f"Mapping image : {self.image_name} to client using the command : {image_map_cmd}")
        cmdline(image_map_cmd)

        # Creating file system on the image created
        create_fs_cmd = f"sudo mkfs.ext4 -m0 /dev/rbd/{self.pool_name}/{self.image_name}"
        log.debug(f"Creating the File system on the image: {self.image_name} using cmd command : {create_fs_cmd}")
        cmdline(create_fs_cmd)

        # Mounting the image on /mnt/ceph-block-device
        mount_image_cmd = f"sudo mount /dev/rbd/{self.pool_name}/{self.image_name} /mnt/ceph-block-device"
        log.debug(f"Mounting the image: {self.image_name} using cmd command : {mount_image_cmd}")
        cmdline(create_fs_cmd)

        # Performing a small write using rbd-bench
        bench_cmd = f"sudo rbd bench-write {self.image_name} --pool={self.pool_name}"
        log.debug(f"Running rbd-bench the image: {self.image_name} using cmd command : {bench_cmd}")
        cmdline(bench_cmd)

        # Capturning image details :
        details_cmd = f"rbd info {self.pool_name}/{self.image_name}"
        log.debug(f"image details for: {self.image_name} is : \n {cmdline(details_cmd)}")

        # collecting config specified in the JSON file
        self.num_loops = config['RBD']['num_loops']
        self.num_jobs = config['RBD']['num_parallel_jobs']
        self.block_size = config['RBD']['block_size']
        self.write_size = config['RBD']['write_size']
        self.run_time = config['RBD']['run_time']
        delete = 0 if config['RBD']['delete_file_data'] else 1

        self.gen_fio_cmd = f"sudo fio --name=global --ioengine=rbd --clientname=admin --pool={self.pool_name}" \
                           f" --rbdname={self.image_name} --bs={self.block_size} --size={self.write_size}" \
                           f" --direct=0 --iodepth=32 --runtime={self.run_time} --numjobs={self.num_jobs}" \
                           f" --loops={self.num_loops} --cgroup_nodelete={delete} --group_reporting "

        log.debug(f"Base command for triggering FIO is : {self.gen_fio_cmd}")

    @staticmethod
    def complete_prereqs():
        """
        Completes pre-reqs of creating a mount directory and installing the FIO rpms on the node
        :return: None
        """
        # cmd to install the FIO RPM on the given node for running File IO
        output = cmdline('sudo rpm -qa')
        if 'fio' not in output:
            cmd = "sudo yum install fio -y"
            log.debug(f"Installing the fio rpms using the cmd {cmd}")
            cmdline(cmd)

        # Creating a mount directory for mounting RBD images created
        folder_name = f"/mnt/ceph-block-device"
        if not os.path.isdir(folder_name):
            cmd = "sudo mkdir /mnt/ceph-block-device"
            log.debug(f"Creating a mount directory using the cmd {cmd}")
            cmdline(cmd)

        log.info("Completing the pre-reqs of installing the FIO rpm and creating the mount directory")

    def fio_write_ops(self):
        """
        Method triggers sequential and Random writes on the given pool.
        """
        log.info(f"Performing Random and Sequential write on the image : {self.image_name}")
        fio_write_cmd = f"{self.gen_fio_cmd} --name=seq_write --rw=write --name=rand_write --rw=randwrite"
        op = cmdline(fio_write_cmd)
        log.debug(f"Performed the Write actions. \n Output collected :\n\n {op}\n\n")

        # Capturing image details :
        details_cmd = f"rbd info {self.pool_name}/{self.image_name}"
        log.debug(f"image details after write operations for: {self.image_name} is : \n {cmdline(details_cmd)}")

    def fio_read_ops(self):
        """
        Method triggers sequential and Random reads on the given pool.
        """
        log.info(f"Performing Random and Sequential reads on the image : {self.image_name}")
        fio_read_cmd = f"{self.gen_fio_cmd} --name=seq_read --rw=read --name=rand_read --rw=randread"
        op = cmdline(fio_read_cmd)
        log.debug(f"Performed the Read actions. \n Output collected :\n\n {op}\n\n")

        # Capturning image details :
        details_cmd = f"rbd info {self.pool_name}/{self.image_name}"
        log.debug(f"image details after read operations for: {self.image_name} is : \n {cmdline(details_cmd)}")

    def fio_readwrite_ops(self):
        """
        Method triggers sequential and Random reads on the given pool.
        """
        log.info(f"Performing Random and Sequential reads on the image : {self.image_name}")
        fio_read_cmd = f"{self.gen_fio_cmd} --name=seq_readwrite --rw=readwrite --name=rand_readwrite --rw=randrw"
        op = cmdline(fio_read_cmd)
        log.debug(f"Performed the Read & write actions. \n Output collected :\n\n {op}\n\n")

        # Capturning image details :
        details_cmd = f"rbd info {self.pool_name}/{self.image_name}"
        log.debug(f"image details after read/write operations for: {self.image_name} is : \n {cmdline(details_cmd)}")


def run_rgw_io():
    """
    Creates object of class RgwIoTools and runs IO
    :return: None
    """
    con = f"1. Create a RGW admin user with the keys : {config['RGW']['create_rgw_user']}\n"
    con1 = f"4. The Secret Key provided is :{config['RGW']['secret_key']}\n" \
           f"5. The access Key provided is :{config['RGW']['access_key']}\n"
    con2 = f"2. Number of buckets being created : {config['RGW']['num_buckets']}\n" \
           f"3. Number of objects in each bucket: {config['RGW']['num_objects']}\n"
    con3 = f"6. Downloading the objects and placing them in folder : object_downloads_{unique_id} "
    log.info(f"\n\nTriggering  RGW IO using BOTO tool with the below config :\n {con}{con2}")
    if config['RGW']['create_rgw_user']:
        log.info(con1)
    if config['RGW']['download_objects']:
        log.info(con3)

    rgw_obj = RgwIoTools()
    # Creating no of buckets specified in the config
    if config['RGW']['create_bkt_obj']:
        log.debug("Creating new buckets")
        rgw_obj.create_buckets(quantity=config['RGW']['num_buckets'])

    # Listing all the Newly created buckets
    dict_buckets = rgw_obj.list_buckets()
    bucket_list = [keys for keys in dict_buckets.keys()]
    log.debug(f"all the buckets Present for the given User are are : {str(bucket_list)}")

    if config['RGW']['create_bkt_obj']:
        # creating objects in each bucket as provided in the config file
        for bkt in bucket_list:
            obj = rgw_obj.create_bucket_object(bucket=bkt, quantity=config['RGW']['num_objects'])
            log.debug(f"all the objects created : {str(obj)}")

    # Listing the contents of a single bucket
    bkt_content_single = rgw_obj.list_bucket_content(bucket=bucket_list[0])
    log.debug(f"\n\n\n the contents of single bucket {bucket_list[0]} are \n {bkt_content_single}\n\n")

    # Listing contents of all the buckets created
    bkt_content_all = rgw_obj.list_bucket_content()
    log.debug(f"\n\n\n the contents all buckets are  \n {bkt_content_all}\n\n")

    # Downloading the objects created and placing them in the folder
    # bucket_name = bucket_list[0]
    # bkt_content_single = rgw_obj.list_bucket_content(bucket=bucket_name)
    # Selecting the 1st object from the bucket to be deleted
    # single_key = bkt_content_single[bucket_name][0].name

    # command for downloading 1 the object in the given bucket with the key provided
    # rgw_obj.download_boto_objects(bucket=bucket_name, key=single_key)

    # downloading all the objects in all the buckets
    if config['RGW']['download_objects']:
        for names in bucket_list:
            log.info(f"Downloading objects for bucket : {names}")
            rgw_obj.download_boto_objects(bucket=names)
            all_uri = rgw_obj.generate_boto_obj_url(bucket=names)
            log.debug(f"The URL's generated for bucket {names} are :\n{str(all_uri)}\n")

    # Selecting a single key and deleting a single object by providing object key and the bucket name
    # bucket_name = li[0]
    # Selecting the 1st object from the bucket to be deleted
    # single_key = bkt_content_single[bucket_name][0].name
    # rgw_obj.delete_boto_object(bucket=bucket_name, key=single_key)
    # bkt_content_single = rgw_obj.list_bucket_content(bucket=li[0])
    # log.debug(f"contents of bucket after deleting a single key {single_key} is given below\n{bkt_content_single}")

    # deleting all the objects and the buckets created
    if config['RGW']['delete_buckets_and_objects']:
        for bucket in bucket_list:
            rgw_obj.delete_boto_bucket(bucket)
        list_buckets = rgw_obj.list_buckets()
        log.debug(f"\n\n\nAfter deleting all the buckets {str(list_buckets.keys())}\n\n\n")
    log.info("Finished Running RGW IO using BOTO tool")


def run_rados_io():
    """
    Creates object of class RadosIoTools and runs IO
    :return: None
    """
    log.info(f"Option present to run Rados bench on the given Host with config :\n\n {config['Rados_Bench']}\n\n")
    block_size = config['Rados_Bench']['Size']
    dur_write = config['Rados_Bench']['write_seconds']
    dur_read = config['Rados_Bench']['write_seconds']

    for i in range(config['Rados_Bench']['no_pools']):
        name = RadosIoTools()
        name.bench_write_ops(bsize=block_size, duration=dur_write)
        name.bench_read_ops(duration=dur_read)

        # Deleting the benckmark objects created
        if config['Rados_Bench']['delete_buckets_and_objects']:
            name.bench_cleanup()


def run_block_io():
    """
    Creates object of class RbdFioTools and runs IO
    :return: None
    """
    log.info(f"Option present to run FIO on the given Host with config :\n\n {config['RBD']}\n\n")
    RbdFioTools.complete_prereqs()
    rbd_obj = RbdFioTools()
    rbd_obj.fio_write_ops()
    rbd_obj.fio_read_ops()
    rbd_obj.fio_readwrite_ops()


if __name__ == '__main__':
    log.info("Starting the script to start instant IO on the given host")
    # todo: Check if RGW node is configured or not. If not, don't trigger RGW IO
    if config['RGW']['trigger']:
        run_rgw_io()
    if config['Rados_Bench']['trigger']:
        run_rados_io()
    if config['RBD']['trigger']:
        run_block_io()
