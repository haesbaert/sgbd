#!/usr/bin/env python

"""
 Copyright (c) 2011 Christiano F. Haesbaert <haesbaert@haesbaert.org>

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.
 
 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""
import os
import time
import struct
import pickle
import sys

BLOCKNUM          = 8192
BLOCKSIZE         = 4096
DATAFILESIZE      = BLOCKNUM * BLOCKSIZE
MAXBUFFERLEN      = 256
MAXBRANCHKEYS     = 400
MAXBRANCHPOINTERS = MAXBRANCHKEYS + 1
MAXLEAFKEYS       = 330
MAXLEAFPOINTERS   = MAXLEAFKEYS + 1
MAXRECORDS        = 64
UNUSED            = 0
LEAF              = 1
BRANCH            = 2
RECORD            = 3

class DataFile(object):
    """
    Lower-most class, represents a datafile and all information about blocks
    which is always available, regardless of wire state. DataFile and
    Dictionary is merged into this class, since they're closely related. 
    """

    def __init__(self, path):
        """DataFile constructor.
        
        Arguments:
        - `path`: DataFile file path, where block information is to be
        stored. 
        """
        self.path = path
        # _blocks is a tuple of BLOCKNUM lists in the form [blocktype, full]
        self._blocks = tuple([[UNUSED, False, -1] for _ in xrange(BLOCKNUM)])
        # Alloc an initial root leaf
        self.root = self.alloc(LEAF)
        # Zerout datafile
        if os.system("dd if=/dev/zero of={0} bs={1} count={2}".
                     format(self.path, BLOCKSIZE, BLOCKNUM)):
            raise ValueError("dd error")
        # Open file
        self.fh = open(self.fspath, "r+b", BLOCKSIZE)

    def alloc(self, blocktype):
        """Alloc a bloc, fetch an UNUSED block and change it's block type,
        returning the number
        
        Arguments:
        - `self`:
        - `blocktype`: UNUSED, LEAF, RECORD, or BRANCH
        """
        for (bnum, (btype, _, _)) in enumerate(self._blocks):
            if btype == UNUSED:
                self._blocks[bnum][0] = blocktype
                self._blocks[bnum][1] = False
                self._blocks[bnum][2] = -1
                return bnum
        raise ValueError("No more UNUSED blocks :-(")
        
    def get_meta(self, blocknum):
        """Get the metadata for block blocknum. Returns a tuple like (type, fullness)
        
        Arguments:
        - `self`:
        - `blocknum`: Block number
        """
        return self._blocks[blocknum]
    
    def get_notfull(self, blocktype):
        """Get any block of type btype that is not full.
        
        Arguments:
        - `self`:
        - `blocktype`: UNUSED, LEAF, RECORD, or BRANCH
        """
        for (bnum, (btype, full, _)) in enumerate(self._blocks):
            if blocktype != btype:
                continue
            if full:
                continue
            
            return bnum
        
    def set_fullness(self, blocknum, fullness):
        """Set block number blocknum fullness to full (True) not full (False) 
        
        Arguments:
        - `self`:
        - `blocknum`: Block number, 0-8191
        - `fullness`: True for full, False for not full
        """
        (btype, _, _) = self._blocks[blocknum]
        if btype == UNUSED:
            raise ValueError("Setting fullness on an unused block !")
        self._blocks[blocknum][1] = fullness

    def get_parent(self, blocknum):
        """Get the parent block number for blocknum.
        
        Arguments:
        - `self`:
        - `blocknum`: Block number, 0-8191
        """
        (_, _, parentnum) = self.get_meta(blocknum)
        return parentnum
    
    # def setparent/getparent
    
class Buffer(object):
    """The Buffer cache, holds at most 256 frames(blocks)
    """

    def __init__(self, datafile):
        """Constructor
        
        Arguments:
        - `self`:
        - `datafile`: Backstorage for this Buffer, a DataFile object.
        """
        self._frames   = []
        self._datafile = datafile

    def full(self):
        """Check if buffer is full.
        
        Arguments:
        - `self`:
        """
        return len(self._frames) == MAXBUFFERLEN

    def alloc(self, blocktype):
        """Get new, unused block of blocktype
        
        Arguments:
        - `self`:
        - `blocktype`: Blocktype
        """
        blocknum = self._datafile.alloc_block(blocktype)
        return self.get_block(blocknum)
    
    def get_notfull(self, blocktype):
        """Get any block object which isn't full of blocktype.
        
        Arguments:
        - `self`:
        - `blocktype`: UNUSED, LEAF, RECORD, or BRANCH
        """
        bnum = self._datafile.get_notfull(blocktype):
        if bnum is None:
            bnum = self.alloc(blocktype)
        # If still None, we're fucked.
        if bnum is None:
            raise ValueError("No more notfull blocks of type {0}".format(
                    blocktype))
    def get_block(self, blocknum)
        """Get the block referenced from blocknum, make a
        victim if necessary, return the full, constructed block.
        
        Arguments:
        - `self`:
        - `blocknum`: block number
        """
        # Search in our frames
        for b in self._frames:
            if blocknum == b.blocknum:
                b.timestamp()
                return b
        # No luck, go down and fetch from datafile
        # Check if we need to swap someone
        if self.full():
            raise ValueError("Unimplemented")
        (btype, fullness) = self._datafile.get_meta(blocknum)
        if btype == LEAF:
            b = LeafBlock(self, blocknum)
        elif btype == RECORD:
            b = RecordBlock(self, blocknum)
        elif btype == BRANCH:
            b = BranchBlock(self, blocknum)
        else:
            raise ValueError("get_block on invalid blocktype: {0}".format(btype))
        # Place buffer in frame (wire)
        self._frames.append(b)
        return b

class Block(object):
    """Generic block class
    """

    def __init__(self, buf, blocknum, blocktype):
        """Wire/Create a block
        
        Arguments:
        - `buf`: A Buffer object
        - `blocknum`: Blocknumber
        - `btype`: Blocktype, 
        """
        self._buffer   = buf
        self._datafile = buf._datafile
        self.blocknum  = blocknum
        self.blocktype = blocktype
        self.keys      = []
        # self.pointers  = []

    def get_parent(self):
        """Get parent block, may return None if root
        
        Arguments:
        - `self`:
        """
        parentnum = self._datafile.get_parent(self.blocknum)
        if parentnum == -1:
            return None
        return self._buffer.get_block(parentnum)

    def full(self):
        """Checks if buffer is full
        
        Arguments:
        - `self`:
        """
        (_, fullness) = self._datafile.get_meta(self.blocknum)
        return fullness

class LeafBlock(Block):
    """A Leaf block.
    """

    def __init__(self, buf, blocknum):
        """Needs a buffer/datafile relation for metadata
        
        Arguments:
        - `buf`: A Buffer Object
        - `blocknum`: Blocknumber
        """
        Block__init__(self, buf, blocknum, LEAF)
    # XXX this is wong
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        self._datafile.set_fullness(self.blocknum,
                                    len(self.keys) == MAXLEAFKEYS)

    def insert(self, record):
        """Insert a record, leaf MUST NOT be full.
        
        Arguments:
        - `self`: 
        - `record`: record to insert.
        """
        if self.full():
            raise ValueError("Leaf is already full you dumbass !")

        pos = 0
        for (key, _) in self.keys():
            if key > record.key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, (record.blocknum, record.offset))
        self._refresh_fullness()

class Record(object):
    """A data record
    """

    def __init__(self, blocknum, offset):
        """Each record carries it's blocknum and offset
        
        Arguments:
        - `blocknum`: Block number
        - `offset`: Block offset
        """
        self._blocknum = blocknum
        self._offset = offset
        sekf.key  = 0
        self.desc = "Free"
        

class RecordBlock(Block):
    """A Record block, may contain up to 64 records
    """

    def __init__(self, buf, blocknum):
        """Needs a buffer/datafile relation for metadata
        
        Arguments:
        - `buf`: A Buffer Object
        - `blocknum`: Blocknumber
        """
        Block__init__(self, buf, blocknum, RECORD)
        self.records = [Record(self.blocknum, x) for x in xrange(MAXRECORDS)]
        
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        full = True
        # Find at least one free record
        for r in self.records:
            if r.pk == 0:
                full = False
                break
            
        self._datafile.set_fullness(self.blocknum, full)

    def alloc(self, key, desc):
        """Alloc a new record on this RecordBlock, return the record.
        
        Arguments:
        - `self`:
        - `key`: Record key
        - `desc`: Record desc
        """
        if self.full():
            raise ValueError("RecordBlock already full !")
        for r in self.records:
            # If record is free...
            if r.key == 0:
                r.key  = key
                r.desc = desc
                self._refresh_fullness()
                return r
            
        self._refresh_fullness()

# class BranchBlock(TODO)

class BplusTree(object):
    """A B+ Tree object, this where the shit happens.
    """

    def __init__(self, buf, rootnum):
        """Create a new BplusTree, needs a buf to fetch/store blocks
        
        Arguments:
        - `buf`: A Buffer object
        - `rootnum`: Number of root block
        """
        self._buf    = buf
        root         = self._buf.alloc(LEAF)
        self.rootnum = root.blocknum

    def get_root(self):
        """Fetch root block
        
        Arguments:
        - `self`:
        """
        return self._buf.get_block(self.rootnum)
    
    def search_leaf(self, key):
        """Search the leaf given a key insertion.
        
        Arguments:
        - `self`:
        - `key`: pk
        """
        branch = None
        leaf   = None
        b      = self.get_root()

        while b.blocktype != LEAF:
            for k in b.keys:
                if key > k:
                    b = self._buffer.get_block(b.pointers[k+1])
                    break

        return b
            
    def make_record(self, key, desc):
        """Allocate a new record from any not full recordblock, returns a
        Record object
        
        Arguments:
        - `self`:
        - `key`: Record key
        - `desc`: Record description
        """
        b = self.get_notfull(RECORD)
        return b.alloc(key, desc)
    
    def insert(self, key, desc):
        """Insert a record into bplustree, handles all cases
        
        Arguments:
        - `self`:
        - `key`: Record key
        - `desc`: Record desc
        """
        
        record = self.make_record(key, desc)
        leafblock = self.search_leaf(record)
        # Yey ! leaf is not full
        if not leafblock.full():
            parent = leafblock.get_parent()
            # No parent, or parent not full, insert
            if parent is None or parent.full():
                leafblock.insert(record)
                return
            # So we have a parent and it's full :-(
            raise ValueError("Unimplemented")

