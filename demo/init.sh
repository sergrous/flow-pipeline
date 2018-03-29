#!/bin/bash

curl --silent http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz -o GeoLite2-Country.tar.gz
mkdir tmp
mkdir dataproc
tar -xvf GeoLite2-Country.tar.gz -C tmp
cp tmp/GeoLite2-Country_*/GeoLite2-Country.mmdb dataproc/GeoLite2-Country.mmdb
rm -rf tmp GeoLite2-Country.tar.gz