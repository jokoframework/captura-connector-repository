#/bin/bash
dir=../conf
if [[ ! -e $dir ]]; then
    mkdir -v $dir
fi
cp -v ../mf_cr.sample.properties ../conf/mf_cr.properties 