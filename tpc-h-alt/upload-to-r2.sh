#!/usr/bin/env bash
#
# upload_to_r2.sh
#
# Copies TPCH CSV + Parquet files (customer, lineitem, nation, orders,
# part, partsupp, region, supplier) from the script’s folder to Cloudflare R2
# using `rclone copy`. Expects exactly:
#   customer.csv    customer.parquet
#   lineitem.csv    lineitem.parquet
#   nation.csv      nation.parquet
#   orders.csv      orders.parquet
#   part.csv        part.parquet
#   partsupp.csv    partsupp.parquet
#   region.csv      region.parquet
#   supplier.csv    supplier.parquet
# living in the same directory as this script.
#
# HOW TO USE:
#   1. Configure an rclone remote named “r2” (or pick a different name, but then
#      update the R2_REMOTE variable below).
#   2. Place this script *beside* your 16 TPCH files.
#   3. Edit R2_REMOTE, R2_BUCKET, and (optionally) R2_DEST_PATH below.
#   4. chmod +x upload_to_r2.sh
#   5. ./upload_to_r2.sh
#

set -euo pipefail

# --------------------------------- CONFIGURE THIS SECTION BASED ON YOUR RCLONE CONFIG ---------------------------------

# 1) rclone remote name for R2 (as you set up via `rclone config`)
#    If you didn’t use “r2”, replace this with your remote’s name.
R2_REMOTE="r2"

# 2) Your R2 bucket’s name (exactly as it appears in Cloudflare), change it accordingly
R2_BUCKET="my-r2-bucket"

# 3) Optional path inside the bucket. E.g., "tpch/May-31-2025". Leave empty or "/"
#    if you want to drop all files at the root of the bucket. Default is root.
R2_DEST_PATH=""   # ← change this (or set to "" for root)

# 4) rclone flags. --progress is generally helpful. Add any flags you like.
RCLONE_FLAGS=(
  --progress
)

# ---------------------------- END OF THE CONFIGURATION SECTION --------------------------

# --------------- DON’T TOUCH BELOW UNLESS YOU KNOW WHAT YOU'RE DOING ---------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DIR="$SCRIPT_DIR"

# Build the remote “root” location: e.g., r2:my-r2-bucket/tpch
if [[ -z "${R2_DEST_PATH// }" ]]; then
  FULL_REMOTE="${R2_REMOTE}:${R2_BUCKET}"
else
  R2_DEST_PATH="${R2_DEST_PATH#/}"
  R2_DEST_PATH="${R2_DEST_PATH%/}"
  FULL_REMOTE="${R2_REMOTE}:${R2_BUCKET}/${R2_DEST_PATH}"
fi

# Sanity check by listing buckets
echo "Checking if rclone can see ${R2_REMOTE}:${R2_BUCKET}..."
if ! rclone ls "${R2_REMOTE}:${R2_BUCKET}" &> /dev/null; then
  echo "Error: Failed to list rclone remote '${R2_REMOTE}:${R2_BUCKET}'." >&2
  echo "       Verify your 'rclone config' credentials." >&2
  exit 1
fi
echo "Bucket OK: ${R2_REMOTE}:${R2_BUCKET}"
echo "Files will be copied from: ${LOCAL_DIR}"
echo "Destination on R2:        ${FULL_REMOTE}"
echo

TABLES=(customer lineitem nation orders part partsupp region supplier)

for name in "${TABLES[@]}"; do
  for ext in csv parquet; do
    src_file="${LOCAL_DIR}/${name}.${ext}"

    if [[ ! -f "$src_file" ]]; then
      echo "Warning: '$src_file' not found. Skipping."
      continue
    fi

    dest_remote="${FULL_REMOTE}/${name}.${ext}"

    echo "Copying '${name}.${ext}' → '${dest_remote}'"
    if ! rclone copy "$src_file" "$dest_remote" "${RCLONE_FLAGS[@]}"; then
      echo "Error: rclone copy failed for '$src_file'." >&2
      exit 1
    fi
  done
done

echo
echo "Done! All files were copied to R2 at:"
echo "   → ${FULL_REMOTE}"
echo

exit 0
