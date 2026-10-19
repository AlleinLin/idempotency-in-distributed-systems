"""Actual application-process exit after independent platform E commits."""

import os

import protocol

protocol.execute()
# No R_J has run. Parent process will recover from the committed original key.
os._exit(73)
