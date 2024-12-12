bucket_name="462-proj-dataset"
expire_time=604800
$object_key="671648670"
aws s3 ls s3://$bucket_name --recursive | awk '{print $4}' | while read object_key; do
    url=$(aws s3 presign s3://$bucket_name/$object_key --expires-in $expire_time)
    echo "Pre-signed URL for $object_key: $url"
done > presigned_urls.txt

