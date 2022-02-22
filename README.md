Sample S3 Uploader Java App
===================================

AWS Java function that expects a bucket name and a prefix, to download all objects from that prefix and put them into a
.zip file that will be uploaded in the `/archive` directory of that bucket.

# Running it

1. Run `mvn clean install` to generate the .jar file. This will generate 2 .jar files in the `/target` folder: a thin
   one and a fat one.
2. Create a new Lambda Function in AWS in the same region that the bucket is located with the _thin_ .jar and assign to
   it a policy that has access to that bucket. Define `com.test.s3bucketlambdaexporter.Handler::handleRequest` as the
   handler.
3. Create a new Lambda Layer with the _fat_ .jar and associate this layer to the function.
6. Run the Lambda Function by testing it with the "hello-world" template
   using `{ "bucket": "the-bucket", "prefix": "some-prefix" }` as the payload. Alternatively, run it with the AWS
   cli: `aws lambda invoke --function-name s3-bucket-lambda-exporter --payload '{ "bucket": "the-bucket", "prefix": "some-prefix" }' response.json` (
   in`response.json` you should be able to see the name's file).

# ToDo

* Reduce the size of the resulting .jar files.
