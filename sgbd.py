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

# Constants to this module
BLOCKNUM     = 8192
BLOCKSIZE    = 4096
DATAFILESIZE = BLOCKNUM * BLOCKSIZE
MAXBUFFERLEN = 256
KEYSIZE      = 12
BLOCKTYPE_LEAF   = 1
BLOCKTYPE_BRANCH = 2
BLOCKTYPE_RECORD = 3


class MetaBlock(object):
    """
    Describes all metadata on a given block, we always have 8192 of these.
    """

    def __init__(self, idx):
        """
        Creates a new metablock with block index number idx
        Arguments:
        - `idx`: integer Block number.
        """
        self.idx       = idx
        self.free      = True
        self.blocktype = None
        self.wired     = False
        self.offset    = self.idx * BLOCKSIZE


class Block(object):
    def __init__(self, metablock):
        if metablock.type is None:
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
    
    def flush(self, fh):
        """Abstract method, will flush block onto filehandle fh.
        
        Arguments:
        - `self`:
        - `fh`: File
        """
        raise TypeError("Block.flush not implemented")

    def load(self, fh):
        """Abstract method, will load block from filehandle.
        
        Arguments:
        - `self`:
        - `fh`: File
        """
        raise TypeError("Block.load not implemented")
    

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
        self.keys = []   # Rowids are ternary tuples (pk, blocknum, offset)

    def flush(self, fh):
        if not self.metablock.wired:
            raise ValueError("flush on unwired block")
        
        fh.seek(self.metablock.offset)
        for (pk, bnum, offset) in self.keys:
            s = struct.pack("QHH", pk, bnum, offset)
            fh.write(s)
        fh.flush()
        os.fsync()

    def load(self, fh):
        if self.metablock.wired:
            raise ValueError("load on wired block")
        if self.keys:
            raise ValueError("Keys not empty")

        fh.seek(self.metablock.offset)
        for _ in xrange(self.metablock.entries):
            k = struct.unpack("QHH", fh.read())
            self.keys.append(k)
        fh.flush()
        os.fsync()

    
class FileSys(object):
    def __init__(self, fspath):
        self.fspath     = fspath
        self.root       = None
        self.metablocks = [MetaBlock(x) for x in xrange(BLOCKNUM)]
        self.buffer     = []
        if not os.path.exists(self.fspath):
            if os.system("dd if=/dev/zero of={0} bs={1} count={2}".
                         format(self.fspath, BLOCKSIZE, BLOCKNUM)):
                raise ValueError("dd error")
        # Open datafile, 4096bytes buf.
        self.fsh = open(self.fspath, "wrb", BLOCKSIZE)

    def wire(self, metablock):
        if metablock.wired:
            raise ValueError("Block already wired")
        if metablock.type is None:
            raise ValueError("Metablock type unset")
        
        # If we're full, we must swap
        if len(self.buffer) == MAXBUFFERLEN:
            victim = self.victim()
            self.unwire(victim)
            assert len(self.buffer) < MAXBUFFERLEN, "Buffer still full"

        if metablock.blocktype == BLOCKTYPE_LEAF:
            block = LeafBlock(metablock)
        # elif metablock.blocktype == BLOCKTYPE_BRANCH:
        #     block = BranchBlock(metablock)
        # elif metablock.blocktype == BLOCKTYPE_RECORD:
        #     block = RecordBlock(metablock)
        else:
            raise ValueError("Unknown metablock.type {0}", metablock.blocktype)
        
        metablock.wired = True
        self.buffer.append(block)
        block.load(self.fsh)
        
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

