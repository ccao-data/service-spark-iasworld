#!/bin/bash
# This container entrypoint makes sure the container's user has an entry in the
# `nss_wrapper` passwd file, then hands off command execution to the base
# image's entrypoint.
#
# Our Compose file runs containers as the host user's UID, which doesn't
# exist in the image, so Spark and Hadoop can't look up a username or home
# directory and crash. The Dockerfile sets up `nss_wrapper` so that user lookups
# read from the file indicated by `$NSS_WRAPPER_PASSWD` instead of
# `/etc/passwd`, and this entrypoint adds the current UID to that file.
#
# For more details about our approach to using `nss_wrapper`, see the comments
# in the Dockerfile.
set -euo pipefail

# `getent passwd <UID>` prints the user entry for a UID, or fails if there is
# none. `nss_wrapper` intercepts it, so this checks the wrapper file, not the
# real `/etc/passwd` file. By skipping the write when an entry exists, we avoid
# duplicate entries when the container restarts, since the file persists across
# restarts. It also leaves real users like `spark` and `root` alone.
if ! getent passwd "$(id -u)" &> /dev/null; then
    # A passwd entry has 7 colon-separated fields:
    #   name:password:UID:GID:description:home directory:login shell
    # - `x` is the standard placeholder meaning the password isn't stored in
    #   this file. This user has no password, since nothing logs in as it
    # - The GID is 0 (root group), matching `user: <UID>:0` in Compose. That
    #   group already exists in the copied group file, so no group entry is
    #   needed
    # - The home directory is `/tmp` because it's writable. Spark keeps a cache
    #   in the home directory
    # - `/bin/false` as the shell means the user can't open a login session
    echo "spark-host:x:$(id -u):$(id -g):Spark host user:/tmp:/bin/false" \
        >> "$NSS_WRAPPER_PASSWD"
fi

# Hand off execution to the base image's entrypoint, which runs the container
# command. `exec` replaces this script with that process, so `docker stop`
# signals reach Spark directly
exec /opt/entrypoint.sh "$@"
