NEW="Collected"
mkdir -p "$NEW" && find . -mindepth 1 -maxdepth 1 -type d ! -name "$NEW" -exec mv -v {} "$NEW"/ \;

