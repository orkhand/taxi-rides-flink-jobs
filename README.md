# taxi-rides-flink-jobs

This repo contains the Maven version of [Taxi Ride example](https://github.com/ververica/flink-training)

## Building Flink jobs locally

```shell
mvn spotless:apply clean package
```

## Building Docker image locally

## How to Push a Docker Image to ECR

## We are querying the ECR API provided by AWS CLI. Later we are pipelining Docker login.
```agsl
    docker login -u AWS -p <encrypted_token> <repo_uri>
    
    password=$(aws ecr get-login-password --region us-east-1 --profile brown) && docker login -u AWS -p $password 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs
    
```

```shell

BUILDKIT_PROGRESS=plain docker build --build-arg FLINK_VERSION=1.17.1 -f Dockerfile .

# If building in Apple Silicon M1/M2 laptop

BUILDKIT_PROGRESS=plain docker buildx build --platform linux/amd64 \
                              --build-arg FLINK_VERSION=1.17.1 \
                              -f Dockerfile . \
                              -t 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug2-v2
                              
docker push 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug2-v2

```

## Building jars locally and then copying into Docker

```shell

# Build all jars first locally

mvn spotless:apply clean package -DskipTests

# Build docker image
BUILDKIT_PROGRESS=plain docker build --build-arg FLINK_VERSION=1.17.1 -f Dockerfile_local .

# If building in Apple Silicon M1/M2 laptop
# Build docker image
                              
BUILDKIT_PROGRESS=plain docker buildx build --platform linux/amd64 \
                              --build-arg FLINK_VERSION=1.17.1 \
                              -f Dockerfile_local . \
                              -t 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug3-v5 \
                              && docker push 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug3-v5

# Push docker image to AWS ECR (Elastic Container Repository)  
docker push <ecr-repo-uri>
         
docker push 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug2-v4

```

## How to Create a Repo in ECR : 
 - Check the successfully creation of the repository in Amazon Elastic Container Registry
```agsl
    aws ecr create-repository --repository-name flink-jobs --region us-east-1
```

## Useful docker commands

```agsl
 # Run Flink image
 
 docker run 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug2-v6 jobmanager
 docker run 432504884617.dkr.ecr.us-east-1.amazonaws.com/flink-jobs:aug2-v6 taskmanager

 # Show running docker containers
 docker ps
 
 # Kill running docker container
 docker kill 5e72fe22df09
 
 # ssh into docker container
 docker exec -it 28335f719d65  /bin/bash
```