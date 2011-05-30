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

# Constants to this module
BLOCKNUM         = 8192
BLOCKSIZE        = 4096
DATAFILESIZE     = BLOCKNUM * BLOCKSIZE
MAXBUFFERLEN     = 256
MAXBRANCHES      = 400
MAXLEAFKEYS      = 330
MAXRECORDS       = 64
KEYSIZE          = 12
BLOCKTYPE_LEAF   = 1
BLOCKTYPE_BRANCH = 2
BLOCKTYPE_RECORD = 3


class MetaBlock(object):
    """
    Describes all metadata on a given block, we always have 8192 of these.
    """

    def __init__(self, blocknum):
        """
        Creates a new metablock with block index number blocknum
        Arguments:
        - `blocknum`: integer Block number.
        """
        self.blocknum  = blocknum
        self.blocktype = None
        self.wired     = False
        self.offset    = self.blocknum * BLOCKSIZE
        
    def __str__(self):
        if self.blocktype == BLOCKTYPE_LEAF:
            t = "Leaf"
        elif self.blocktype == BLOCKTYPE_BRANCH:
            t = "Branch"
        elif self.blocktype == BLOCKTYPE_RECORD:
            t = "Record"
        else:
            raise ValueError("Unexpected blocktype {0}".format(self.blocktype))
        
        return "Metablock: {0} ({1}) wired: {2} offset: {3}".format(
            self.blocknum, t, self.wired, self.offset)


class Block(object):
    def __init__(self, metablock):
        if metablock.blocktype is None:
            raise ValueError("metablock.type unset")
        self.metablock = metablock
        self.timestamp = 0

    def touch(self):
        """Refresh timestamp, block must be wired
        
        Arguments:
        - `self`:
        """
        if not self.metablock.wired:
            raise ValueError("touch() on an unwired block")
        
        self.timestamp = time.time()
    
    def flush(self, _fh):
        """Abstract method, will flush block onto filehandle fh.
        
        Arguments:
        - `self`:
        - `fh`: File
        """
        raise TypeError("Block.flush not implemented")

    def load(self, _fh):
        """Abstract method, will load block from filehandle.
        
        Arguments:
        - `self`:
        - `fh`: File
        """
        raise TypeError("Block.load not implemented")
    
    def full(self):
        """Abstract method, True if block is somewhat full, False otherwise.
        
        Arguments:
        - `self`:
        """
        raise TypeError("Block.full not implemented")

    def nextfree(self):
        """Abstract method, returns the next free record/key/or so on, None otherwise.
        
        Arguments:
        - `self`:
        """
        raise TypeError("Block.nextfree not implemented")

class Record(object):
    def __init__(self, blocknum, offset):
        """
        
        Arguments:
        - `self`:
        """
        self.blocknum = blocknum
        self.offset   = offset
        
        self.pk       = 0
        self.desc     = "Default"

    def free(self):
        return self.pk == 0
    
# TODO unify key code
class BranchKey(object):
    def __init__(self, blocknum, offset):
        """
        """
        self.blocknum     = blocknum
        self.offset       = offset
        
        self.pk             = 0
        self.child_blocknum = -1

    def __cmp__(self, other):
        return cmp(self.pk, other.pk)
    
    def free(self):
        return self.pk == 0

    
class BranchBlock(Block):
    def __init__(self, metablock):
        Block.__init__(self, metablock)
        self.metablock.blocktype = BLOCKTYPE_BRANCH
        self.allbranches = tuple([BranchKey(metablock.blocknum, x)
                                  for x in xrange(MAXBRANCHES)])
        self.branches = []

    def __len__(self):
        return len(self.branches)
    
    def nextfree(self):
        for x in self.allbranches:
            if x.free():
                return x
            
        return None

    def full(self):
        return len(self.branches) == len(self.allbranches)

    def load(self, fh):
        self.touch()
        fh.seek(self.metablock.offset)
        for bk in self.allbranches:
            (bk.pk, bk.child_blocknum) = struct.unpack("qH", fh.read(10))
            if not bk.free():
                self.insert(bk)
            
    def flush(self, fh):
        if not self.metablock.wired:
            raise ValueError("flush on unwired block")
        fh.seek(self.metablock.offset)
        for bk in self.branches:
            s = struct.pack("qH", bk.pk, bk.child_blocknum)
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())
        
    def branchkey_from_leaf(self, leaf):
        if self.full():
            raise ValueError("Branch is full")
        
        bk = self.nextfree()
        bk.pk             = leaf.keys[0].pk
        if bk.pk == 0:
            raise ValueError("Unexpected pk")
        bk.child_blocknum = leaf.metablock.blocknum
        
        return bk

    def insert(self, leaf):
        if self.full():
            raise ValueError("Branch is full")
        
        pos = 0
        bk = self.branchkey_from_leaf(leaf)
        for bkaux in self.branches:
            if bkaux > bk:
                break
            pos = pos + 1
        self.branches.insert(pos, bk)
        
    
class RecordBlock(Block):
    """
    """

    def __init__(self, metablock):
        """
        """
        Block.__init__(self, metablock)
        self.metablock.blocktype = BLOCKTYPE_RECORD
        self.records = tuple([Record(metablock.blocknum, x)
                              for x in xrange(MAXRECORDS)])
        
    def __len__(self):
        return len(self.records)
    
    def nextfree(self):
        for x in self.records:
            if x.free():
                return x

        return None

    def full(self):
        return self.nextfree() == None

    def flush(self, fh):
        if not self.metablock.wired:
            raise ValueError("flush on unwired block")
        
        fh.seek(self.metablock.offset)
        for rec in self.records:
            s = struct.pack("q56s", rec.pk, rec.desc)
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())

    def load(self, fh):
        self.touch()
        fh.seek(self.metablock.offset)
        for rec in self.records:
            (rec.pk, rec.desc) = struct.unpack("q56s", fh.read(64))
        # FIXME

class LeafKey(object):
    """
    """

    def __init__(self, blocknum, offset):
        """
        """
        self.blocknum     = blocknum
        self.offset       = offset
        
        self.pk           = 0
        self.rid_blocknum = -1
        self.rid_offset   = -1

        
    def __cmp__(self, other):
        return cmp(self.pk, other.pk)

    def free(self):
        return self.pk == 0
        

class LeafBlock(Block):
    """
    A block which hold rowids, that is, a leaf block.
    """

    def __init__(self, metablock):
        """
        
        Arguments:
        - `self`:
        - `metablock`: MetaBlock
        """
        Block.__init__(self, metablock)
        self.metablock.blocktype = BLOCKTYPE_LEAF
        self.allkeys = tuple([LeafKey(metablock.blocknum, x)
                              for x in xrange(MAXLEAFKEYS)])
        self.keys = []

    def __len__(self):
        return len(self.keys)
        
    def flush(self, fh):
        if not self.metablock.wired:
            raise ValueError("flush on unwired block")
        
        fh.seek(self.metablock.offset)
        for lk in self.allkeys:
            s = struct.pack("QHH", lk.pk, lk.rid_blocknum, lk.rid_offset)
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())

    def load(self, fh):
        self.touch()
        fh.seek(self.metablock.offset)
        for lk in self.allkeys:
            (lk.pk, lk.rid_blocknum, lk.rid_offset) = struct.unpack("QHH", fh.read(12))
            if not lk.free():
                self.insert(lk)

    def full(self):
        return len(self.keys) == len(self.allkeys)

    def nextfree(self):
        for lk in self.allkeys:
            if lk.free():
                return lk
        return None

    def movekey(self, lk, other):
        olk = other.nextfree()
        olk.pk           = lk.pk
        olk.rid_blocknum = lk.rid_blocknum
        olk.rid_offset   = lk.rid_offset

        lk.pk           = 0
        lk.rid_blocknum = -1
        lk.rid_offset   = -1
        self.keys.remove(lk)
        other.insert(olk)
        
    def leafkey_from_rec(self, rec):
        lk              = self.nextfree()
        if lk is None:
            raise ValueError("No more free leafkeys")
        lk.pk           = rec.pk
        lk.rid_blocknum = rec.blocknum
        lk.rid_offset   = rec.offset
        
        return lk
        
    def lookup(self, pk):
        lk = None

        for lka in self.keys:
            if lka.pk == pk:
                lk = lka;
        return lk

    def insert(self, rec):
        if self.full():
            raise ValueError("Leaf is full")
        pos = 0

        lk = self.leafkey_from_rec(rec)
        for lkaux in self.keys:
            if lkaux > lk:
                break
            pos = pos + 1
        self.keys.insert(pos, lk)
        
        return lk
        
class Sgbd(object):
    def __init__(self, fspath):
        self.fspath     = fspath
        self.metablocks = [MetaBlock(x) for x in xrange(BLOCKNUM)]
        self.buffer     = []
        
        # FIXME
        self.root           = self.metablocks[0]
        self.root.blocktype = BLOCKTYPE_LEAF
        if not os.path.exists(self.fspath):
            if os.system("dd if=/dev/zero of={0} bs={1} count={2}".
                         format(self.fspath, BLOCKSIZE, BLOCKNUM)):
                raise ValueError("dd error")
        # Open datafile, 4096bytes buf.
        self.fsh = open(self.fspath, "r+b", BLOCKSIZE)

    def fetch_block(self, blocknum):

        # Lookup for block in buffer
        for b in self.buffer:
            if b.metablock.blocknum == blocknum:
                b.touch()
                return b
        # Miss, we need to wire
        # Fetch the metablock from blocknum
        metablock = self.metablocks[blocknum]
        
        return self.wire(metablock)

    def fetch_root(self):
        return self.fetch_block(self.root.blocknum)
    
    def wire(self, metablock):
        if metablock.wired:
            raise ValueError("Block already wired")
        if metablock.blocktype is None:
            raise ValueError("Metablock type unset")
        
        # If we're full, we must swap
        if len(self.buffer) == MAXBUFFERLEN:
            victim = self.victim()
            self.unwire(victim)
            assert len(self.buffer) < MAXBUFFERLEN, "Buffer still full"

        if metablock.blocktype == BLOCKTYPE_LEAF:
            block = LeafBlock(metablock)
        elif metablock.blocktype == BLOCKTYPE_BRANCH:
            block = BranchBlock(metablock)
        elif metablock.blocktype == BLOCKTYPE_RECORD:
            block = RecordBlock(metablock)
        else:
            raise ValueError("Unknown metablock.blocktype {0}".format(metablock.blocktype))
        
        self.buffer.append(block)
        metablock.wired = True
        block.load(self.fsh)
        block.touch()
        
        return block
        
    def unwire(self, block):
        """
        Unwire a block, remove from blocks list and flush to disk.
        
        Arguments:
        - `self`: 
        - `block`: Block to be unwired
        """
        if not block.metablock.wired:
            raise ValueError("unwire on unwired block")

        block.flush(self.fsh)
        block.metablock.wired = False
        self.buffer.remove(block)

    def victim(self):
        """Select the next victim
        
        Arguments:
        - `self`:
        """
        if not self.buffer:
            raise ValueError("buffer blocks is empty")
        
        victim = self.buffer[0]
        for b in self.buffer:
            if b.timestamp < victim.timestamp:
                victim = b
                
        return victim

    def alloc_block(self, blocktype):
        for mb in self.metablocks:
            if mb.blocktype == None:
                mb.blocktype = blocktype
                b = self.wire(mb)
                b.touch()
                return b
            
        return None
        
    def fetch_freeblock(self, blocktype):
        for b in self.buffer:
            if b.metablock.blocktype != blocktype:
                continue
            if b.full():
                continue
            b.touch()
            return b
        
        return self.alloc_block(blocktype)
    
    # def fetch_freerecord(self):
    #     b = self.fetch_freeblock(BLOCKTYPE_RECORD)
    #     if b.full():
    #         raise ValueError("No more free records")
    #     return b.nextfree()

    def close(self):
        for b in self.buffer[:]:
            self.unwire(b)
        self.fsh.close()
        self.fsh = None
        f = open(self.fspath + ".pickle", "w")
        pickle.dump(self, f)
        f.close()

    def make_record(self, pk, desc):
        b = self.fetch_freeblock(BLOCKTYPE_RECORD)
        if not b:
            raise ValueError("No more free record blocks")
        rec = b.nextfree()
        if not rec:
            raise ValueError("No more free records")
        rec.pk   = pk
        rec.desc = desc
        
        return rec
        
    def find_leaf(self, pk):
        b = self.fetch_root()
        if b.metablock.blocktype == BLOCKTYPE_LEAF:
            return b
        else: # TODO
            raise ValueError("Unimplemented")
        
    def lookup(self, pk):
        # Find the leaf to this record
        leaf = self.find_leaf(pk)

        lk = leaf.lookup(pk)
        if not lk:
            return None
        rb = self.fetch_block(lk.rid_blocknum)
        return rb.records[lk.rid_offset]
        
    def insert(self, pk, desc="Default description"):
        """
        
        Arguments:
        - `self`:
        - `pk`:
        - `desc`:
        """
        # Avoid duplicates
        if self.lookup(pk):
            return None
        # Find the leaf to this record
        leaf = self.find_leaf(pk)
        # Make a new record
        rec = self.make_record(pk, desc)
        # If we have room, go on and insert.
        if not leaf.full():
            leaf.insert(rec)
        else:
            # Get a new Leaf
            newleaf = self.alloc_block(BLOCKTYPE_LEAF)
            # Move the top half leafkeys to the new leaf
            for lk in leaf.keys[len(leaf)/2:]:
                leaf.movekey(lk, newleaf)
                
            # Our parent can only be root for now
            # Split root
            #parent = self.fetch_root()
            newroot = self.alloc_block(BLOCKTYPE_BRANCH)
            # Repoint root
            self.root = newroot.metablock
            # Link leafs in newroot
            newroot.insert(leaf)
            newroot.insert(newleaf)
        
        return leaf
            
        
"""
Interactive interface functions, designed to be used withing python interactive
interpreter.
"""

def record_insert(pk, desc="Default description"):
    """
    Insert a record with given key pk with description desc, desc should not be
    longer than 56 bytes.
    Arguments:
    - `pk`: Integer, Primary key
    - `desc`: String, Description
    """
    
