#!/bin/sh
echo "Commit message is '$@'"
svn commit --message "$@" --config-dir ./svn_config
