#!/bin/sh
echo "Commit message is '$@'"
svn commit -m "$@" --config-dir ../svn_config
