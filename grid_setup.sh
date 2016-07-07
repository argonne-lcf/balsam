#!/usr/bin/env bash

ROOT_PATH=/users/hpcusers
ARGOBALSAM_PATH=$ROOT_PATH/argobalsam/dev
GRIDCERT_PATH=$ROOT_PATH/argobalsam/gridsecurity

export X509_USER_CERT=$GRIDCERT_PATH/$USER/xrootdsrv-cert.pem
export X509_USER_KEY=$GRIDCERT_PATH/$USER/xrootdsrv-key.pem
export X509_CACERTS=$GRIDCERT_PATH/$USER/cacerts.pem
export X509_CERT_DIR=/etc/grid-security/certificates

