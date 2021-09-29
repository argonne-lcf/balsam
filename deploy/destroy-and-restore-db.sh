#!/usr/bin/env bash
set -e

if [ $# -ne 1 ]; then 
    echo "illegal number of parameters"
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

if [[ $1 != *.sql ]]; then 
    echo "Must pass an .sql file to restore from"
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

if [[ $1 != *.sql ]]; then 
    echo "Must pass an .sql file to restore from"
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

if [ ! -f "$1" ]; then 
    echo "The file $1 does not exist"
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

read -p "Really DESTROY the current Balsam database and restore from $1?" -n 1 -r
echo    # (optional) move to a new line

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Dropping balsam db"
    docker exec postgres dropdb -U postgres -f balsam

    echo "Re-creating balsam db"
    docker exec postgres createdb -U postgres balsam

    echo "Restoring from $1"
    docker exec -i postgres psql -U postgres balsam < $1

    echo "Restore done. Applying migrations:"
    docker exec gunicorn balsam server migrate

    echo "All done: success"
fi

[[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
