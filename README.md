# taxi-rides-flink-jobs

This repo contains the Maven version of [Taxi Ride example](https://github.com/ververica/flink-training)

## Building Flink jobs locally

```shell
mvn spotless:apply clean package
```

## Building Docker image locally

```shell

BUILDKIT_PROGRESS=plain docker build --build-arg FLINK_VERSION=1.17.1 -f Dockerfile .

# If building in Apple Silicon M1/M2 laptop

BUILDKIT_PROGRESS=plain docker buildx build --platform linux/amd64 \
                              --build-arg FLINK_VERSION=1.17.1 \
                              -f Dockerfile . \
                              -t taxi-rides-flink-jobs-image:aug1-2023-v1
                              
docker push 432504884617.dkr.ecr.us-east-1.amazonaws.com/orkhan/taxi-rides-flink-jobs-image:aug1-2023-v1

```

## Building jars locally and then copying into Docker

```shell

# Build all jars first locally

mvn spotless:apply clean package

# Build docker image
BUILDKIT_PROGRESS=plain docker build --build-arg FLINK_VERSION=1.17.1 -f Dockerfile_local .

# If building in Apple Silicon M1/M2 laptop
# Build docker image
BUILDKIT_PROGRESS=plain docker buildx build --platform linux/amd64 \
                              --build-arg FLINK_VERSION=1.17.1 \
                              -f Dockerfile_local . \
                              -t taxi-rides-flink-jobs-image:aug1-2023-v1

# Push docker image to AWS ECR (Elastic Container Repository)                       


```


## How to Create a Repo in ECR : 
 - Check the successfully creation of the repository in Amazon Elastic Container Registry
```agsl
    aws ecr create-repository --repository-name flink-jobs --region us-east-1
```

## How to Push a Docker Image to ECR

### For Docker to push the image to ECR
    - first we have to authenticate our Docker credentials with AWS, Store the encrypted token somewhere
```agsl
    password=$(aws ecr get-login-password --region us-east-1)
```

## We are querying the ECR API provided by AWS CLI. Later we are pipelining Docker login.
```agsl
    aws ecr --region <region> | docker login -u AWS -p <encrypted_token> <repo_uri>
    
    password=$(aws ecr get-login-password --region us-east-1 --profile brown) && docker login -u AWS -p $password 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs
    
```

## How to Tag a Local Docker Image
```agsl
    docker tag <source_image_tag> <target_ecr_repo_uri>
    <source_image_tag>  ->>  username/image_name:tag
     docker tag taxi-rides-flink-jobs-image:aug1-2023-v1 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug1-2023-v1
```

## How to Push the Docker Image to ECR
```agsl
    docker push <ecr-repo-uri>
    docker push 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug1-2023-v1
```