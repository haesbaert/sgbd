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
        # Zerout datafile
        if os.system("dd if=/dev/zero of={0} bs={1} count={2}".
                     format(self.path, BLOCKSIZE, BLOCKNUM)):
            raise ValueError("dd error")
        # Open file
        self.fh = open(self.path, "r+b", BLOCKSIZE)

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
        """Get the metadata for block blocknum.
        Returns a tuple like (type, fullness)
        
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

    def set_parent(self, blocknum, pblocknum):
        """Set the parent for blocknum
        
        Arguments:
        - `self`:
        - `blocknum`: Block number, 0-8191
        - `pblocknum`: Parent block number, 0-8191
        """
        if blocknum < 1 or blocknum > 8191 or pblocknum < 1 or pblocknum > 8191:
            self._blocks[blocknum][2] = pblocknum

            
class Buffer(object):
    """The Buffer cache, holds at most 256 frames(blocks)
    """

    def __init__(self, path):
        """Constructor
        
        Arguments:
        - `self`:
        - `path`: Backstorage for this Buffer, a string.
        """
        self._frames   = []
        self._datafile = DataFile(path)

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
        blocknum = self._datafile.alloc(blocktype)
        return self.get_block(blocknum)
    
    def get_notfull(self, blocktype):
        """Get any block object which isn't full of blocktype.
        
        Arguments:
        - `self`:
        - `blocktype`: UNUSED, LEAF, RECORD, or BRANCH
        """
        bnum = self._datafile.get_notfull(blocktype)
        if bnum is None:
            return self.alloc(blocktype)
        else:
            return self.get_block(bnum)
        
    def get_block(self, blocknum):
        """Get the block referenced from blocknum, make a
        victim if necessary, return the full, constructed block.
        
        Arguments:
        - `self`:
        - `blocknum`: block number
        """
        # Search in our frames
        for b in self._frames:
            if blocknum == b.blocknum:
                #b.timestamp()
                return b
        # No luck, go down and fetch from datafile
        # Check if we need to swap someone
        if self.full():
            raise ValueError("Unimplemented")
        (btype, _, _) = self._datafile.get_meta(blocknum)
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

    def is_root(self):
        """Check if this is the root block
        
        Arguments:
        - `self`:
        """
        return self.get_parent() == None
        
    def get_parent(self):
        """Get parent block, may return None if root
        
        Arguments:
        - `self`:
        """
        parentnum = self._datafile.get_parent(self.blocknum)
        if parentnum == -1:
            return None
        return self._buffer.get_block(parentnum)

    def set_parent(self, parentblock):
        """Set parent block
        
        Arguments:
        - `self`:
        - `parentblock`: The parent block, must be a block object.
        """
        self._datafile.set_parent(self.blocknum, parentblock.blocknum)
    
    def full(self):
        """Checks if block is full
        
        Arguments:
        - `self`:
        """
        (_, fullness, _) = self._datafile.get_meta(self.blocknum)
        return fullness

    
# TODO unify keys/pointers into LeafBlock
class LeafBlock(Block):
    """A Leaf block.
    """

    def __init__(self, buf, blocknum):
        """Needs a buffer/datafile relation for metadata
        
        Arguments:
        - `buf`: A Buffer Object
        - `blocknum`: Blocknumber
        """
        Block.__init__(self, buf, blocknum, LEAF)
        self.keys = []
        
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
        for rec in self.keys:
            if rec.key > record.key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, record)
        self._refresh_fullness()

    def insert_split(self, record, newleaf):
        """Split the records with rightleaf, top-half records will go to
        newleaf.
        
        Arguments:
        - `self`:
        - `newleaf`: The new right(higher) leafblock.
        """
        # can only split an already full leaf
        if not self.full():
            raise ValueError("Trying to split leaf which isn't full!")
        # Insert record to force a split
        pos = 0
        for rec in self.keys:
            if rec.key > record.key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, record)
        # Do the splitting
        for key in self.keys[len(self.keys)/2:]:
            self.keys.remove(key)
            newleaf.insert(key)
            
        self._refresh_fullness()
        newleaf._refresh_fullness()
        assert not self.full()
        assert not newleaf.full()
        # Return the middlekey
        return newleaf.keys[0]

class BranchBlock(Block):
    """A Branch block.
    """

    def __init__(self, buf, blocknum):
        """Needs a buffer/datafile relation for metadata
        
        Arguments:
        - `buf`: A Buffer Object
        - `blocknum`: Blocknumber
        """
        Block.__init__(self, buf, blocknum, BRANCH)
        # Keys are keys (pk) :-).
        self.keys     = []
        # Pointers are blocknums, len(keys) == (len(pointers) + 1)
        self.pointers = []
        
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        self._datafile.set_fullness(self.blocknum,
                                    len(self.keys) == MAXBRANCHKEYS)
        
    def insert(self, leftblocknum, key, rightblocknum):
        """Insert new leaf a pointers
        
        Arguments:
        - `self`:
        - `leftblocknum`: Pointer to left block
        - `key`: The key, pk
        - `rightblocknum`: Pointer to right block
        """
        if self.full():
            raise ValueError("Branch is already full you dumbass !")
        pos = 0
        for k in self.keys:
            if k > key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, key)
        self.pointers.insert(pos, leftblocknum)
        self.pointers.insert(pos + 1, rightblocknum)
        self._refresh_fullness()

    def split(self, otherbranchblock):
        """Move the top-half keys/pointers to the otherblock, return the middle key
        
        Arguments:
        - `self`:
        - `otherbranchblock`:
        """
        # Can only split an already full branch
        if not self.full():
            raise ValueError("Trying to split branch which isn't full!")

        # Do the splitting
        for key in self.keys[len(self.keys)/2:]:
            self.keys.remove(key)
            otherbranchblock.insert(key)
            
        
class Record(object):
    """A data record
    """

    def __init__(self, blocknum, offset):
        """Each record carries it's blocknum and offset
        
        Arguments:
        - `blocknum`: Block number
        - `offset`: Block offset
        """
        self.blocknum = blocknum
        self.offset   = offset
        self.key      = 0
        self.desc     = "Free"
        

class RecordBlock(Block):
    """A Record block, may contain up to 64 records
    """

    def __init__(self, buf, blocknum):
        """Needs a buffer/datafile relation for metadata
        
        Arguments:
        - `buf`: A Buffer Object
        - `blocknum`: Blocknumber
        """
        Block.__init__(self, buf, blocknum, RECORD)
        self.records = [Record(self.blocknum, x) for x in xrange(MAXRECORDS)]
        
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        full = True
        # Find at least one free record
        for r in self.records:
            if r.key == 0:
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
        raise ValueError("Should have found a free record")

        
class BplusTree(object):
    """A B+ Tree object, this where the shit happens.
    """

    def __init__(self, path):
        """Create a new BplusTree, needs a buf to fetch/store blocks
        
        Arguments:
        - `path`: Buffer storage path
        - `rootnum`: Number of root block
        """
        self._buf    = Buffer(path)
        # Make sure root is there.
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
        b = self.get_root()

        while b.blocktype != LEAF:
            for i, k in enumerate(b.keys):
                if key > k:
                    b = self._buf.get_block(b.pointers[i+1])
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
        b = self._buf.get_notfull(RECORD)
        return b.alloc(key, desc)
    
    def insert(self, key, desc):
        """Insert a record into bplustree, handles all cases
        
        Arguments:
        - `self`:
        - `key`: Record key
        - `desc`: Record desc
        """
        
        record = self.make_record(key, desc)
        leafblock = self.search_leaf(record.key)
        # Case 1: Yey ! leaf is not full
        if not leafblock.full():
            leafblock.insert(record)
            return
        
        # Awww leaf is full :(
        indexblock = leafblock.get_parent()
        if not leafblock.is_root():
            raise ValueError("Unimplemented")

        # Case 2: Leaf is full, but parent isn't or root splitting
        # Split the leaf, move top half to new leaf
        newleafblock = self._buf.alloc(LEAF)
        # Insert and split
        leafmiddlekey = leafblock.insert_split(record, newleafblock)
        # Root splitting
        if leafblock.is_root():
            # Alloc a new root
            newroot = self._buf.alloc(BRANCH)
            self.rootnum = newroot.blocknum
            indexblock = newroot
        # else: # Only root splitting for now
        #     raise ValueError("Unimplemented")
        # Finish case 2
        if not indexblock.full():
            # TODO
            leafblock.set_parent(indexblock)
            newleafblock.set_parent(indexblock)
            indexblock.insert(leafblock.blocknum, leafmiddlekey.key,
                              newleafblock.blocknum)
            return
            
        # Case 3, indexblock is also full
        # Will never fall here if Case 2, since indexblock won't be full.
        # while indexblock.full():

        while indexblock and indexblock.full():
            # Alloc a new branchblock, will be the neighbour of our current
            # indexblock and will have the top-half keys of indexblock.
            newindexblock = self._buf.alloc(BRANCH)
            # Insert the leaf key into indexblock (which is full) and force a
            # splitting, after the call, newindexblock should have the top-half
            # keys of indexblock, the middlekey is the first key of
            # newindexblock.
            middlekey = indexblock.insert_split(leafblock.blocknum,
                                                leafmiddlekey,
                                                newleafblock.blocknum,
                                                newindexblock)
            # Now we do not know if leafblock is in indexblock or
            # newindexblock, we don't care

            # We must now insert the middle key in the upperindex (parent),
            # with pointers to indexblock and newindexblock, if there is no
            # parent, we get a new root.
            upperindexblock = indexblock.get_parent()
            # upperindexblock is the parent of our current index.
            if upperindexblock is None:
                # Root splitting
                # Alloc a new root
                newroot = self._buf.alloc(BRANCH)
                self.rootnum = newroot.blocknum
                upperindexblock = newroot

            # Now link the fucking branches
            upperindexblock.insert(indexblock.blocknum, middlekey,
                                   newindexblock.blocknum)
            indexblock.set_parent(upperindexblock)
            newindexblock.set_parent(newindexblock)
            # Go up
            indexblock = indexblock.get_parent()

            # We should have the following
"""
           _________________
          /                 \
          | upperindexblock |
          \_________________/
            /              \
           /                \
          /                  \
         /                    \
       _/_______________       \_________________             
      /                 \      /                 \
      |  indexblock     |      | newindexblock   |
      \_________________/      \_________________/

"""
        # End of case 3
        

            
        
