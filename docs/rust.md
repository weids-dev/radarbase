## Crate and Mod in Rust
* **Crate**: A crate is the highest level of organization in Rust. It's a package of Rust code. Each crate will compile to a library or a binary. Crates can depend on other crates, which can be added to your project through the Cargo.toml file. Each crate has a root module that contains all the other modules in the crate.
* **Mod**: A mod (module) is a way of organizing code within a crate. Modules allow you to group related definitions together and manage the namespaces of your libraries. You can define modules in Rust with the mod keyword.

## RefCell
The `RefCell` allowing rust code that potentially violates the borrowing rules to compile, and it enforces these rules at runtime by inserting some detector (or guard) to that memory area, if there is a violation of the borrowing rules at runtime (sucnh as having multiple mutable references or a mutable reference alongside immutable ones), a panic will occur.
This ensures that Rust's guarantees of memory safety are still upheld, even when compile-time checks are bypassed.

It should be used sparingly, and if you find yourself using it frequently, it could indicate a need to rethink your design.
