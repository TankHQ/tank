# Remember to increase the version first
cd tank-core/ &&
cargo publish &&
cd ../ &&
cd tank-macros/ &&
cargo publish &&
cd ../ &&
cargo publish &&
cd tank-tests/ &&
cargo publish &&
cd ../ &&
cd tank-duckdb/ &&
cargo publish &&
cd ../ &&
cd tank-mysql/ &&
cargo publish &&
cd ../ &&
cd tank-postgres/ &&
cargo publish &&
cd ../ &&
cd tank-scylladb/ &&
cargo publish &&
cd ../ &&
cd tank-sqlite/ &&
cargo publish &&
cd ../
