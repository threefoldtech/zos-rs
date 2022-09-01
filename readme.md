> this is heavily under construction

# Introduction
ZOS-RS is mostly a research project that turned into a seed to zos modules implementation in rust. The project is built in a way so that it can __gradually__ replace current go implementation.

To accomplish this this project implements special compatibility types (found under `src/bus/types`) that are used by the zbus services to communicate between both the go and the rust modules.

Not all types are defined right now, only what is enough to implement a proof of concept which was the `zui` module which is the zos information window. More modules to follow:

# What is implemented
- [rbus](https://github.com/threefoldtech/rbus) which is the [zbus](https://github.com/threefoldtech/zbus) implementation in rust
- defining modules APIs, as defined under [bus](src/bus)
- modules
  - [x] zui
    - still need improvements. it consumes around 0.2% of cpu continuously
  - [ ] flist
  - [ ] stroage
  - [ ] identity
  - [ ] node
  - [ ] container
  - [ ] vm
  - [ ] gateway
  - [ ] networkd
  - [ ] ...


# How to proceed from here
Well, we start to implement other modules one by one. at some point we will replace the zos go implementation with those modules in development. And we will keep replacing them one by one until it replaces it all.
