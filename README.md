# mwt-utilities

This repository contains code to assist extracting data from the Multi-Worm Tracker
system.  It's intended for the case where you want just a bit of information from
the files, not a full analysis.

It's supposed to handle all versions of MWT output, including old-style text
files and WCON (JSON-based), as well as any newer formats that might be used
(e.g. binary).  Right now it only does old-style, however.

## How do I do anything with this?

You'll need [mill](https://github.com/lihaoyi/mill) to build it.

Basically, you call `mwt.utilities.Contents.from(path)` and then call methods
on that or read the error message.

A better description needs to be written, of course.
