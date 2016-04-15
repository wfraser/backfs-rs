[![Build Status](https://travis-ci.org/wfraser/backfs-rs.svg?branch=master)](https://travis-ci.org/wfraser/backfs-rs)

backfs-rs is a rewrite in Rust of a project I wrote in C some years ago.
It's a virtual filesystem that acts like a proxy for other filesystems, with an arbitrary-size on-disk cache.
The idea is that you point it at a slow network filesystem, give it a couple gigs of disk to play with, and then everything is available like before, but the data most recently accessed is on the local disk and can be read much faster.

The original C version is located at https://github.com/wfraser/backfs

There's a lot more info on what this is and how it works over at the old project's page.

IMPORTANT: This version isn't finished yet; don't use it unless you just want to hack on it!
