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

# Constants to this module
BLOCKNUM     = 8192
BLOCKSIZE    = 4096
DATAFILESIZE = BLOCKUM * BLOCKSIZE
MAXBUFFERLEN = 256
BLOCKTYPE = {
    LEAF   = 1,
    BRANCH = 2,
    RECORD = 3,
    }

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


class Block():
    def __init__(self, metablock):
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
    
    def serialize(self):
        """Abstract method must return a serialized string.
        
        Arguments:
        - `self`:
        """
        
        raise TypeError("Block.serialize not implemented")

    def deserialize(self, stream):
        """Abstract method should load stream into something.
        
        Arguments:
        - `self`:
        - `stream`: A stream of bytes to be deserialized
        """
        
        raise TypeError("Block.deserialize not implemented")
    


class LeafBlock(Block):
    """
    A block which hold rowids, that is, a leaf block.
    """

    def __init__(self, n):
        """
        
        Arguments:
        - `self`:
        - `n`: Block number
        """
        Block.__init__(self, n)
        self.blocktype = BLOCKTYPE.LEAF
        self.rowids = []        # Rowids are binary tuples

    def serialize(self):
        """
        Returns a serialized string of block, usefull for storing into disk and
        such, guaranteed to fit in 4096
        Arguments:
        - `self`:
        """
        
        
        # TODO
        pass


class RecordBlock(Block):
    """
    A block which holds all records, which are a tuple (pk, desc)
    """

    def __init__(self, n):
        """
        
        Arguments:
        - `self`:
        - `n`: Block number
        """
        Block.__init__(self, n)
        self.blocktype = BLOCKTYPE.RECORD
        self.records = []

    def __len__(self):
        """Returns the number of records in this block.
        
        Arguments:
        - `self`:
        """

        return len(self.records)

    def serialize(self):
        """
        Returns a serialized string of block, usefull for storing into disk and
        such, guaranteed to fit in 4096 bytes.
        Arguments:
        - `self`:
        """
        # TODO
        pass
    
    
class FileSys():
    def __init__(self, fspath):
        self.fspath     = fspath
        self.root       = None
        self.metablocks = [MetaBlock(x) for x in xrange(BLOCKNUM)]
        self.buffer     = []
        if os.path.exists(self.fspath):
            if os.system("dd if=/dev/zero of={0} bs={1} count={2}",
                      self.fspath, BLOCKSIZE, BLOCKNUM):
                raise ValueError("dd error")
        # Open datafile, 4096bytes buf.
        self.fsh = open(self.fspath, "wrb", BLOCKSIZE)

    def wire(self, metablock):
        if len(self.buffer) == MAXBUFFERLEN:
            raise ValueError("Buffer Cache is full")
        if metablock.wired:
            raise ValueError("Block already wired")
        
        metablock.wired = True
        block = Block(metablock)
        self.buffer.append(block)
        self.fs.seek(block.metablock.offset)
        datum = self.fs.read(BLOCKSIZE)
        block.deserialize(datum)
        
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
        
        datum = block.serialize()
        block.metablock.wired = False
        self.buffer.remove(block)
        self.fs.seek(block.metablock.offset)
        self.fs.write(datum)
        self.fs.flush()
        os.fsync()

    def victim(self):
        """Select the next victim
        
        Arguments:
        - `self`:
        """
        if not self.buffer:
            raise ValueError("buffer blocks is empty")
        
        candidate = self.buffer[0]
        for b in self.buffer:
            if b.timestamp < cadidate.timestamp:
                candidate = b
                
        return candidate

"""
Simulando uma insercao.
insert_entry(15, "descricao 15")
	- [fs] Solicita bloco raiz
        	- [buffer] se bloco esta wired, se nao, wire ou swap
        - [fs] Bloco so pode ser branch ou leaf
        	- [fs] Se leaf...
                	- [fs] Se existe espaco no bloco
                                - [fs] Busca DataEntry do rowid, copia pk/desc
                        	- [fs] Insere rowid address no bloco
                        - [fs] Se nao, faz a danca TODO.
        	- [fs] Se branch...
                	- [fs] Desce na arvore ate uma leaf.
                        - 

"""

    
