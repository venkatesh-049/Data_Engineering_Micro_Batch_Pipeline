"""Local compatibility shim for pytest's legacy py import.

This prevents accidental import of a broken global py.py on this machine.
"""

import _pytest._py.error as error
import _pytest._py.path as path

__all__ = ["error", "path"]
