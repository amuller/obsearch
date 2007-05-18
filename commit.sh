#!/bin/sh

svn commit --message "$1" --config-dir ./.svn_config
