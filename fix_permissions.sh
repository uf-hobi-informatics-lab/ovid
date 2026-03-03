#!/usr/bin/env bash
# fix_permissions.sh — Replace chmod 777 with secure permissions across the Ovid codebase.
#
# Strategy: Run Docker containers as the host user's UID:GID (via docker-compose `user:` directive)
#           so the container and host share the same file ownership. This eliminates the need for
#           world-writable (777) permissions entirely.
#
# Run this script from the ovid project root with write access to all files:
#   sudo bash fix_permissions.sh
#
set -euo pipefail

echo "=== Fixing permissions across the Ovid codebase ==="

# ─────────────────────────────────────────────────────────────────────
# 1. Dockerfile — remove USER 1001, tighten directory permissions
# ─────────────────────────────────────────────────────────────────────
DOCKERFILE="ovid_setup/ovid_image/Dockerfile"
echo "[1/6] Updating $DOCKERFILE ..."

sed -i 's|mkdir -p /docker_data && chmod 777 /docker_data|mkdir -p /docker_data/spark_tmp \&\& chmod -R 775 /docker_data|' "$DOCKERFILE"

sed -i 's|chmod 777 /opt/bitnami/spark/tmp|chmod 775 /opt/bitnami/spark/tmp|' "$DOCKERFILE"
sed -i 's|chmod 777 /opt/bitnami/spark/conf|chmod 775 /opt/bitnami/spark/conf|' "$DOCKERFILE"
sed -i 's|chmod 777 /opt/bitnami/spark/jars|chmod 775 /opt/bitnami/spark/jars|' "$DOCKERFILE"
sed -i 's|chmod 777 /app|chmod 775 /app|' "$DOCKERFILE"

# Remove the now-duplicate "mkdir -p /docker_data/spark_tmp && chmod -R 777 /docker_data" line
sed -i '/mkdir -p \/docker_data\/spark_tmp && chmod -R 777 \/docker_data/d' "$DOCKERFILE"

# Replace USER 1001 with a comment
sed -i 's|^USER 1001|# Do not hardcode a UID — containers run as the host user via docker-compose user: directive|' "$DOCKERFILE"

echo "    Done."

# ─────────────────────────────────────────────────────────────────────
# 2. ovid.py — tighten chmod calls, inject host_user, add :ro mounts
# ─────────────────────────────────────────────────────────────────────
OVIDPY="ovid.py"
echo "[2/6] Updating $OVIDPY ..."

# Tighten directory permissions from 0o777 to 0o755
sed -i "s|os.chmod('work_directory/docker_data', 0o777)|os.chmod('work_directory/docker_data', 0o755)|" "$OVIDPY"
sed -i "s|os.chmod('work_directory/docker_data/spark_tmp', 0o777)|os.chmod('work_directory/docker_data/spark_tmp', 0o755)|" "$OVIDPY"
sed -i "s|os.chmod('work_directory/docker_data/ovid_logs', 0o777)|os.chmod('work_directory/docker_data/ovid_logs', 0o755)|" "$OVIDPY"

# Add host_user variable after outdir line
sed -i "/outdir = '\.\.\/'/a\\
\\
    # Run containers as the host user to avoid UID mismatch and world-writable permissions\\
    host_user = f'{os.getuid()}:{os.getgid()}'" "$OVIDPY"

# Inject 'user': host_user into all three service definitions
# Master service — add user after hostname line
sed -i "/'hostname': master_name,/a\\
        'user': host_user," "$OVIDPY"

# Worker service — add user after container_name line
sed -i "/'container_name': worker_name,/a\\
            'user': host_user," "$OVIDPY"

# Submit service — add user after the submit container_name line
sed -i "/f'ovid-submit-{timestamp}'/a\\
        'user': host_user," "$OVIDPY"

# Make credentials and input data mounts read-only
sed -i "s|f'{input_directory}:/data'|f'{input_directory}:/data:ro'|g" "$OVIDPY"
sed -i "s|f'{CREDENTIALS_PATH}:/ovid_credentials'|f'{CREDENTIALS_PATH}:/ovid_credentials:ro'|g" "$OVIDPY"

echo "    Done."

# ─────────────────────────────────────────────────────────────────────
# 3. common/ovidFunctions.py — tighten copy_folder permissions
# ─────────────────────────────────────────────────────────────────────
OVIDFUNCS="common/ovidFunctions.py"
echo "[3/6] Updating $OVIDFUNCS ..."

# Change directory permissions in copy_folder from 0o777 to 0o755
sed -i 's|os.chmod(os.path.join(root, d), 0o777)|os.chmod(os.path.join(root, d), 0o755)|g' "$OVIDFUNCS"

# Change file permissions in copy_folder from 0o777 to 0o644
sed -i 's|os.chmod(os.path.join(root, f), 0o777)|os.chmod(os.path.join(root, f), 0o644)|g' "$OVIDFUNCS"

# Change the final folder chmod from 0o777 to 0o755
sed -i 's|os.chmod(folder_to_copy_to, 0o777)|os.chmod(folder_to_copy_to, 0o755)|g' "$OVIDFUNCS"

echo "    Done."

# ─────────────────────────────────────────────────────────────────────
# 4. common/ovid_logging.py — tighten log file permissions
# ─────────────────────────────────────────────────────────────────────
LOGGING="common/ovid_logging.py"
echo "[4/6] Updating $LOGGING ..."

sed -i 's|os.chmod(_log_file_path, 0o666)|os.chmod(_log_file_path, 0o644)|' "$LOGGING"
sed -i 's|Set world-writable permissions so container can write to it|Set owner-writable permissions (container runs as same UID via docker-compose user: directive)|' "$LOGGING"

echo "    Done."

# ─────────────────────────────────────────────────────────────────────
# 5. README.md — replace chmod 777 instruction
# ─────────────────────────────────────────────────────────────────────
README="README.md"
echo "[5/6] Updating $README ..."

# Use a Python one-liner for the multi-line replacement since sed is fragile with multi-line
python3 -c "
import re
with open('$README', 'r') as f:
    content = f.read()

old = '''1. Change permissions on all the folders and files in the repository to 777 by going to the upper folder and running the following command:

        chmod -R 777 .'''

new = '''1. Ensure the output and working directories are writable. From the project root, run:

        chmod -R u+rwX .

   This grants the current user full access while preserving group/other permissions. The Docker
   containers run as your host UID/GID, so no world-writable permissions are needed.

   **Note:** Avoid using \`chmod -R 777\` as it makes all files world-readable, -writable, and -executable, which is a security risk — especially for credential files and private keys.'''

content = content.replace(old, new)

with open('$README', 'w') as f:
    f.write(content)
"

echo "    Done."

# ─────────────────────────────────────────────────────────────────────
# 6. Rebuild Docker image reminder
# ─────────────────────────────────────────────────────────────────────
echo "[6/6] All files updated."
echo ""
echo "=== IMPORTANT: Next Steps ==="
echo "  1. Rebuild the Docker image to pick up Dockerfile changes:"
echo "       docker build -t ovid_image_2 ovid_setup/ovid_image/"
echo "  2. Test by running an ETL job to verify containers work with the new permissions."
echo ""
echo "=== Summary of Changes ==="
echo "  - Dockerfile: Removed USER 1001, tightened dir perms to 775"
echo "  - ovid.py: Containers now run as host UID:GID, dirs set to 755, mounts use :ro"
echo "  - ovidFunctions.py: copy_folder uses 755/644 instead of 777"
echo "  - ovid_logging.py: Log files use 644 instead of 666"
echo "  - README.md: Replaced 'chmod -R 777' with 'chmod -R u+rwX' and security note"
