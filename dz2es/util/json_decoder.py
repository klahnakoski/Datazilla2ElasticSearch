# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals


# THE INTENT IS TO NEVER ACTUALLY PARSE ARRAYS OF PRIMITIVE VALUES, RATHER FIND
# THE START AND END OF THOSE ARRAYS AND SIMPLY STRING COPY THEM TO THE
# INEVITABLE JSON OUTPUT
import json
from dz2es.util.jsons import json_encoder, use_pypy
from dz2es.util.struct import StructList, Null, wrap, unwrap, EmptyList


ARRAY = 1
VALUE = 3
OBJECT = 4

builtin_json_decoder = json._default_decoder.decode


class JSONList(object):
    def __init__(self, json, s, e):
        self.json = json
        self.start = s
        self.end = e
        self.list = None

    def _convert(self):
        if self.list is None:
            self.list = builtin_json_decoder(self.json[self.start:self.end])

    def __getitem__(self, index):
        self._convert()
        if isinstance(index, slice):
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                from .env.logs import Log

                Log.error("slice step must be None, do not know how to deal with values")
            length = len(self.list)

            i = index.start
            i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return StructList(self.list[i:j])

        if index < 0 or len(self.list) <= index:
            return Null
        return wrap(self.list[index])

    def __setitem__(self, i, y):
        self._convert()
        self.json = None
        self.list[i] = unwrap(y)

    def __iter__(self):
        self._convert()
        return (wrap(v) for v in self.list)

    def __contains__(self, item):
        self._convert()
        return list.__contains__(self.list, item)

    def append(self, val):
        self._convert()
        self.json = None
        self.list.append(unwrap(val))
        return self

    def __str__(self):
        return self.json[self.start:self.end]

    def __len__(self):
        self._convert()
        return self.list.__len__()

    def __getslice__(self, i, j):
        from .env.logs import Log

        Log.error("slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes.  Use [start:stop:step]")

    def copy(self):
        if self.list is not None:
            return list(self.list)
        return JSONList(self.json, self.start, self.end)

    def remove(self, x):
        self._convert()
        self.json = None
        self.list.remove(x)
        return self

    def extend(self, values):
        self._convert()
        self.json = None
        for v in values:
            self.list.append(unwrap(v))
        return self

    def pop(self):
        self._convert()
        self.json = None
        return wrap(self.list.pop())

    def __add__(self, value):
        self._convert()
        output = list(self.list)
        output.extend(value)
        return StructList(vals=output)

    def __or__(self, value):
        self._convert()
        output = list(self.list)
        output.append(value)
        return StructList(vals=output)

    def __radd__(self, other):
        self._convert()
        output = list(other)
        output.extend(self.list)
        return StructList(vals=output)

    def right(self, num=None):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT
        """
        self._convert()
        if num == None:
            return StructList([self.list[-1]])
        if num <= 0:
            return EmptyList
        return StructList(self.list[-num])

    def leftBut(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        self._convert()
        if num == None:
            return StructList([self.list[:-1:]])
        if num <= 0:
            return EmptyList
        return StructList(self.list[:-num:])

    def last(self):
        """
        RETURN LAST ELEMENT IN StructList
        """
        self._convert()
        if self.list:
            return wrap(self.list[-1])
        return Null

    def __json__(self):
        if self.json is not None:
            return self.json[self.start, self.end]
        else:
            return json_encoder.encode(self)


def parse_string(i, json):
    simple = True
    j = i + 1
    while j < len(json):
        c = json[j]
        if c == "\"":
            if simple:
                return j, json[i + 1:j]
            else:
                return j, builtin_json_decoder(json[i:j + 1])
        elif c == "\\":
            simple = False
            j += 1
            c = json[j]
            if c == "u":
                j += 4
            elif c not in ["\"", "\\", "/", "b", "n", "f", "n", "t"]:
                j -= 1
        j += 1
    return i, None


def jump_string(i, json):
    j = i + 1
    while j < len(json):
        c = json[j]
        if c == "\"":
            return j
        elif c == "\\":
            j += 1
            c = json[j]
            if c == "u":
                j += 4
            elif c not in ["\"", "\\", "/", "b", "n", "f", "n", "t"]:
                j -= 1
        j += 1
    return j


def jump_array(i, json):
    j = i + 1
    empty = True
    while j < len(json):
        c = json[j]
        if c in ["[", "{"]:
            return i, None  # NOT PRIMITIVE
        elif c == "]":
            if empty:
                return j, []
            else:
                return j, JSONList(json, i, j + 1)
        elif c == "\"":
            empty = False
            j = jump_string(j, json)
        elif c not in [" ", "\t", "\r", "\n"]:
            empty = False
        j += 1
    return i, None


def parse_const(i, json):
    try:
        j = i + 1
        while j < len(json):
            c = json[j]
            if c in [" ", "\t", "\n", "\r", ",", "}", "]"]:
                const = json[i:j]
                try:
                    val = {
                        "0": 0,
                        "-1": -1,
                        "1": 1,
                        "true": True,
                        "false": False,
                        "null": None
                    }[const]
                except Exception:
                    try:
                        val = int(const)
                    except Exception:
                        val = float(const)

                return j - 1, val
            j += 1
        return i
    except Exception, e:
        from .env.logs import Log

        Log.error("Can not parse const", e)


def decode(json):
    var = ""
    curr = []
    mode = ARRAY
    stack = []

    # FIRST PASS SIMPLY GETS STRUCTURE
    i = 0
    while i < len(json):
        c = json[i]
        if mode == ARRAY:
            if c in [" ", "\t", "\n", "\r", ","]:
                pass
            elif c == "]":
                curr = stack.pop()
                if isinstance(curr, dict):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "[":
                i, arr = jump_array(i, json)
                if arr is None:
                    arr = []
                    stack.append(curr)
                    curr.append(arr)
                    curr = arr
                    mode = ARRAY
                else:
                    curr.append(arr)
            elif c == "{":
                obj = {}
                stack.append(curr)
                curr.append(obj)
                curr = obj
                mode = OBJECT
            elif c == "\"":
                i, val = parse_string(i, json)
                curr.children.append(val)
            else:
                i, val = parse_const(i, json)
                curr.children.append(i)
                j = i + 1
                while j < len(json):
                    c = json[j]
                    if c in [" ", "\t", "\n", "\r", ",", "]"]:
                        i = j - 1
                        break
                    j += 1
            i += 1
        elif mode == OBJECT:
            if c in [" ", "\t", "\n", "\r", ","]:
                pass
            elif c == ":":
                mode = VALUE
            elif c == "}":
                curr = stack.pop()
                if isinstance(curr, dict):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "\"":
                i, var = parse_string(i, json)
            i += 1
        elif mode == VALUE:
            if c in [" ", "\t", "\n", "\r"]:
                pass
            elif c == "}":
                curr = stack.pop()
                if isinstance(curr, dict):
                    mode = OBJECT
                else:
                    mode = ARRAY
            elif c == "[":
                i, arr = jump_array(i, json)
                if arr is None:
                    arr = []
                    stack.append(curr)
                    curr[var] = arr
                    curr = arr
                    mode = ARRAY
                else:
                    curr[var] = arr
                    mode = OBJECT
            elif c == "{":
                obj = {}
                stack.append(curr)
                curr[var] = obj
                curr = obj
                mode = OBJECT
            elif c == "\"":
                i, val = parse_string(i, json)
                curr[var] = val
                mode = OBJECT
            else:
                i, val = parse_const(i, json)
                curr[var] = val
                mode = OBJECT
            i += 1

    return curr[0]


if use_pypy:
    json_decoder = decode
else:
    json_decoder = builtin_json_decoder
