#/bin/bash
dir_conf=../conf
dir_logs=../logs
if [[ ! -e $dir_conf ]]; then
    mkdir -v $dir_conf
fi
if [[ ! -e $dir_logs ]]; then
    mkdir -v $dir_logs
fi
cp -v ../mf_cr.sample.properties ../conf/mf_cr.properties 
