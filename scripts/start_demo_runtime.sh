#!/bin/sh
# Free-tier demo wrapper only. Production deployments keep these roles separate.
set -u

children=""
stopping=0

start_child() {
    "$@" &
    children="$children $!"
}

stop_children() {
    if [ "$stopping" -eq 1 ]; then
        return
    fi

    stopping=1
    for pid in $children; do
        kill -TERM "$pid" 2>/dev/null || :
    done
}

trap 'stop_children' INT TERM

start_child fluxion-scheduler
start_child fluxion-publisher
start_child fluxion-worker
start_child fluxion-reaper

while :; do
    for pid in $children; do
        if kill -0 "$pid" 2>/dev/null; then
            continue
        fi

        wait "$pid"
        child_status=$?

        if [ "$stopping" -eq 1 ]; then
            exit_status=0
        elif [ "$child_status" -eq 0 ]; then
            # A critical runtime ending cleanly is still unexpected here.
            exit_status=1
            stop_children
        else
            exit_status=$child_status
            stop_children
        fi

        for other_pid in $children; do
            if [ "$other_pid" != "$pid" ]; then
                wait "$other_pid" 2>/dev/null || :
            fi
        done

        exit "$exit_status"
    done

    sleep 1
done
