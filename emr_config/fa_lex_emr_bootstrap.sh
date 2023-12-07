#!/bin/bash

sudo yum install -y python3-devel
sudo yum install -y git-core
sudo python3 -m pip install boto3 python-Levenshtein
sudo /usr/bin/pip3 install fsspec
sudo /usr/bin/pip3 install --upgrade pip
sudo /usr/local/bin/pip3 install s3fs
sudo /usr/local/bin/pip3 install tabulate
sudo /usr/local/bin/pip3 install numpy==1.21.3
sudo /usr/local/bin/pip3 install pandas==1.2.5
sudo /usr/local/bin/pip3 install dataclasses
sudo /usr/local/bin/pip3 install datetime
sudo /usr/local/bin/pip3 install argparse
sudo /usr/local/bin/pip3 install pillow
sudo /usr/local/bin/pip3 install matplotlib
sudo /usr/local/bin/pip3 install "git+https://github.com/twwhatever/BlingFire.git#subdirectory=dist-pypi"
sudo /usr/local/bin/pip3 install spacy
sudo python3 -m spacy download en_core_web_sm