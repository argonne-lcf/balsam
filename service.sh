#!/usr/bin/env bash

ROOT_PATH=/users/hpcusers
ARGOBALSAM_PATH=$ROOT_PATH/argobalsam/dev
GRIDCERT_PATH=$ROOT_PATH/argobalsam/gridsecurity

#source $ROOT_PATH/scripts/setupPython-2.7.sh

#export X509_USER_CERT=/tmp/x509up_u30168
#export X509_USER_KEY=/tmp/x509up_u30168

export X509_USER_CERT=$GRIDCERT_PATH/$USER/xrootdsrv-cert.pem
export X509_USER_KEY=$GRIDCERT_PATH/$USER/xrootdsrv-key.pem
#export X509_CERT_DIR=$GRIDCERT_PATH/certificates
export X509_CACERTS=$GRIDCERT_PATH/$USER/cacerts.pem
export X509_CERT_DIR=/etc/grid-security/certificates

#export X509_USER_CERT=/users/hpcusers/argobalsam/production/argobalsam/keycert.txt
#export X509_USER_KEY=/users/hpcusers/argobalsam/production/argobalsam/keycert.txt


PID_FILE=pids.txt

start() {
   # kill old processes first
   while read pid; do
      kill -0 $pid > /dev/null 2>&1
      if [[ $? == '0' ]]; then
         echo killing old instance before starting: PID = $pid, cmd:
         echo $(cat /proc/$pid/cmdline | strings -1)
         kill -- -$pid
      fi
   done <$PID_FILE
   python manage.py argo_service > ./log/argo_service.out 2>&1  &
   echo $! > $PID_FILE
   python manage.py balsam_service > ./log/balsam_service.out 2>&1  &
   echo $! >> $PID_FILE
   python manage.py runserver 8001 > ./log/runserver.log 2>&1 &
   echo $! >> $PID_FILE
}

stop() {
   while read pid; do
      kill -0 $pid > /dev/null 2>&1
      if [[ $? == '0' ]]; then
         echo killing instance: PID = $pid, cmd:
         echo $(cat /proc/$pid/cmdline | strings -1)
         kill -- -$pid
      fi
   done <$PID_FILE
}

case "$1" in
    start)
        start
    ;;
    stop)
        stop
    ;;
    reload|restart|force-reload)
        stop
        start
    ;;
    **)
        echo "Usage: $0 {start|stop|reload}" 1>&2
    ;;
esac




