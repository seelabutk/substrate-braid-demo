# Steps to deploy the demo locally.

1. Download and unzip one of [our datasets](http://accona.eecs.utk.edu:9595/).
2. Install [Docker](https://www.docker.com/). Make sure you install the CLI as a part of this.
3. Open substrate.config.yaml and set the `data.source` and `braid.directory`. `data.source` should be set to the _directory_ containing the dataset.
4. Set `braid.file_pattern` based upon your chosen dataset. For example, `UGRD-1yr-720x361-fcst0.tgz` requires `braid.file_pattern` to be `UGRD-1yr-720x361-fcst0-*of*_*of*_*of*_*of*.dat`.
5. Uncomment the `docker` options and modify them if desired.
6. Run `./run.sh start` to launch the demo and `./run.sh stop` to destroy it.
7. If you are running an older version of Docker, it may be necessary to run in privileged mode with `./run.sh --privileged start/stop`.

# Steps to deploy to AWS.

1. Install [Docker](https://www.docker.com/). Make sure you install the CLI as a part of this.
2. Open substrate.config.yaml and set `braid.directory`.
3. Set `braid.file_pattern to match your chosen dataset. An example of how to correlate the chosen dataset from S3 is given in the provided `substrate.config.yaml`.
4. Uncomment the `aws` options and set `aws.bucket`. Note that `aws.bucket` must correspond to a unique bucket name across all S3 buckets (including other peoples' buckets).
5. Run `./run.sh --env AWS_ACCESS_KEY_ID={{YOUR_ACCESS_KEY_ID}} --env AWS_SECRET_ACCESS_KEY={{YOUR_ACCESS_KEY}} start` to deploy the demo and `./run.sh --env --env AWS_ACCESS_KEY_ID={{YOUR_ACCESS_KEY_ID}} --env AWS_SECRET_ACCESS_KEY={{YOUR_ACCESS_KEY}} stop` to destroy it.
6. If your AWS credentials include a session token, make sure to include it in the above command as `--env AWS_SESSION_TOKEN={{YOUR_SESSION_TOKEN}}`.
7. If you are running an older version of Docker, it may be necessary to run in privileged mode with `./run.sh --privileged start/stop`.

If you need to deploy the AWS instance with HTTPS support, then you will need to own a domain name which you can use for the demo. Once you have a domain name, the following high-level steps will be necessary to run over HTTPS.

1. Uncomment the `aws.https` option in substrate.config.yaml. This will create an Application Load Balancer for the demo which can be associated with an SSL certificate.
2. Using the AWS Certificate Manager, request an SSL certificate for the demo instance.
3. Open the EC2 Load Balancer panel in the AWS console, select the load balancer for the demo, and then modify the listener to listen on port 443 (you'll also be asked to add the SSL cert you requested to the load balancer at this point).
4. Update the domain's DNS records to point to the load balancer.
