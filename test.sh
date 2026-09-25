for pkg in $(cargo metadata --format-version 1 | jq -r '.workspace_members[]' | cut -d' ' -f1); do
    cargo test -p "$pkg" --all-targets
done
