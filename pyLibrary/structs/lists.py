# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from copy import deepcopy
from pyLibrary.structs.nones import Null
from pyLibrary.structs.wraps import wrap, unwrap


_get = object.__getattribute__
_set = object.__setattr__


class StructList(list):
    """
    ENCAPSULATES HANDING OF Nulls BY wrapING ALL MEMBERS AS NEEDED
    ENCAPSULATES FLAT SLICES ([::]) FOR USE IN WINDOW FUNCTIONS
    """
    EMPTY = None

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        # list.__init__(self)
        if vals == None:
            self.list = []
        elif isinstance(vals, StructList):
            self.list = vals.list
        else:
            self.list = vals

    def __getitem__(self, index):
        if isinstance(index, slice):
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                from pyLibrary.env.logs import Log
                Log.error("slice step must be None, do not know how to deal with values")
            length = len(_get(self, "list"))

            i = index.start
            i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return StructList(_get(self, "list")[i:j])

        if index < 0 or len(_get(self, "list")) <= index:
            return Null
        return wrap(_get(self, "list")[index])

    def __setitem__(self, i, y):
        _get(self, "list")[i] = unwrap(y)

    def __getattribute__(self, key):
        try:
            if key != "index":  # WE DO NOT WANT TO IMPLEMENT THE index METHOD
                output = _get(self, key)
                return output
        except Exception, e:
            if key[0:2] == "__":  # SYSTEM LEVEL ATTRIBUTES CAN NOT BE USED FOR SELECT
                raise e
        return StructList.select(self, key)

    def select(self, key):
        output = []
        for v in _get(self, "list"):
            try:
                output.append(v.__getattribute__(key))
            except Exception, e:
                try:
                    output.append(v.__getitem__(key))
                except Exception, f:
                    output.append(None)

        return StructList(output)

    def __iter__(self):
        return (wrap(v) for v in _get(self, "list"))

    def __contains__(self, item):
        return list.__contains__(_get(self, "list"), item)

    def append(self, val):
        _get(self, "list").append(unwrap(val))
        return self

    def __str__(self):
        return _get(self, "list").__str__()

    def __len__(self):
        return _get(self, "list").__len__()

    def __getslice__(self, i, j):
        from pyLibrary.env.logs import Log

        Log.error("slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes.  Use [start:stop:step] (see https://github.com/klahnakoski/pyLibrary/blob/master/pyLibrary/structs/README.md#slicing-is-broken-in-python-27)")

    def copy(self):
        return StructList(list(_get(self, "list")))

    def __copy__(self):
        return StructList(list(_get(self, "list")))

    def __deepcopy__(self, memo):
        d = _get(self, "list")
        return wrap(deepcopy(d, memo))

    def remove(self, x):
        _get(self, "list").remove(x)
        return self

    def extend(self, values):
        for v in values:
            _get(self, "list").append(unwrap(v))
        return self

    def pop(self):
        return wrap(_get(self, "list").pop())

    def __add__(self, value):
        output = list(_get(self, "list"))
        output.extend(value)
        return StructList(vals=output)

    def __or__(self, value):
        output = list(_get(self, "list"))
        output.append(value)
        return StructList(vals=output)

    def __radd__(self, other):
        output = list(other)
        output.extend(_get(self, "list"))
        return StructList(vals=output)

    def right(self, num=None):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT [-num:]
        """
        if num == None:
            return StructList([_get(self, "list")[-1]])
        if num <= 0:
            return Null

        return StructList(_get(self, "list")[-num:])

    def leftBut(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        if num == None:
            return StructList([_get(self, "list")[:-1:]])
        if num <= 0:
            return StructList.EMPTY

        return StructList(_get(self, "list")[:-num:])

    def last(self):
        """
        RETURN LAST ELEMENT IN StructList [-1]
        """
        lst = _get(self, "list")
        if lst:
            return wrap(lst[-1])
        return Null

    def map(self, oper, includeNone=True):
        if includeNone:
            return StructList([oper(v) for v in _get(self, "list")])
        else:
            return StructList([oper(v) for v in _get(self, "list") if v != None])


StructList.EMPTY = Null

