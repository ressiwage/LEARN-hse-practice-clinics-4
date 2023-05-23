#!/bin/bash
docker build -t site .
docker run --name site --expose=4000 -p 4000:5000 -d site:latest