# Changelog

## [0.2.0](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/compare/trogon-eventstore@v0.1.0...trogon-eventstore@v0.2.0) (2026-09-09)


### Features

* **client:** preserve distributed trace context ([#9](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/9)) ([9f54ab9](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/9f54ab9691259b5b3d52b30ea4272c017650ad85))
* **examples:** demonstrate competing inventory lifecycles ([#12](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/12)) ([360931d](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/360931dd7c2c2c63355fe11a2855815df4b52835))
* **examples:** demonstrate idempotent appends ([#11](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/11)) ([87784f2](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/87784f230516b0364241d89dda253ffe497874f4))
* **examples:** distinguish command replay from append retry ([#13](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/13)) ([a473467](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/a473467780e687cc67a973de4eddd5850b5080ae))
* **observability:** preserve cross-client trace continuity ([#10](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/10)) ([f309717](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/f3097178c000bad8cc41f25487519aa91bd3d473))


### Bug Fixes

* **client:** align with the server contract ([fa09650](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/fa096505190e33030c37f96f0a6dd695ce6e7083))
* **tests:** restore Rust integration coverage ([#8](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/8)) ([ae80285](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/ae80285e6738b1913f7f0f06975eaccb10d98234))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * trogon-eventstore-macros bumped from 0.1.0 to 0.2.0

## [0.1.0](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/compare/trogon-eventstore@v0.0.1...trogon-eventstore@v0.1.0) (2026-08-23)


### Features

* **client:** establish independent Rust distribution ([#1](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/issues/1)) ([a60aeed](https://github.com/TrogonStack/TrogonEventStore-Client-Rust/commit/a60aeed741feaeadd42c0a7209505de8d7ac52ee))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * trogon-eventstore-macros bumped from 0.0.1 to 0.1.0
