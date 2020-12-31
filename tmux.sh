#!/bin/bash
tmux_session_6() {
tmux new-session 'while true; do ./worker-pvpdb.py update pvpdb-worker-1; sleep 3600; done' \; \
     split-window -h -p 66 'while true; do ./worker-pvpdb.py update pvpdb-worker-2; sleep 3600; done' \; \
     split-window -h 'while true; do ./worker-pvpdb.py update pvpdb-worker-3; sleep 3600; done' \; \
     select-pane -t 0 \; \
     split-window -v 'while true; do ./worker-pvpdb.py update pvpdb-worker-4; sleep 3600; done' \; \
     select-pane -t 2 \; \
     split-window -v 'while true; do ./worker-pvpdb.py update pvpdb-worker-5; sleep 3600; done' \; \
     select-pane -t 4 \; \
     split-window -v 'while true; do ./worker-pvpdb.py update pvpdb-worker-6; sleep 3600; done' \;
}

tmux_session_4() {
tmux new-session 'while true; do ./worker-pvpdb.py update pvpdb-worker-1; sleep 3600; done' \; \
     split-window -h 'while true; do ./worker-pvpdb.py update pvpdb-worker-2; sleep 3600; done' \; \
     select-pane -t 0 \; \
     split-window -v 'while true; do ./worker-pvpdb.py update pvpdb-worker-3; sleep 3600; done' \; \
     select-pane -t 2 \; \
     split-window -v 'while true; do ./worker-pvpdb.py update pvpdb-worker-4; sleep 3600; done' \;
}

tmux_session_6
