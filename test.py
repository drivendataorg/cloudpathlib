from cloudpathlib import S3Path, S3Client

client = S3Client(profile_name="bad_profile")

import gc

gc.collect()
