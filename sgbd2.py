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
import types
import random

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
        #if blocknum < 1 or blocknum > 8191 or pblocknum < 1 or pblocknum > 8191:
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
        # self._frames   = []
        self._frames = {}
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
        
        if self._frames.has_key(blocknum):
            b = self._frames[blocknum]
            b.touch()
            return b
        
        if self.full():
            victim = self._frames[self._frames.keys()[0]]
            for i in self._frames:
                b = self._frames[i]
                if b.timestamp < victim.timestamp:
                    victim = b
            victim.flush()
            self._frames.pop(victim.blocknum)
            if self.full():
                raise ValueError("Still full !")
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
        self._frames[blocknum] = b
        b.touch()
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
        self.timestamp = self.touch()

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

    def touch(self):
        """Update the block timestamp, used to victimize.
        
        Arguments:
        - `self`:
        """
        self.timestamp = time.time()
        return self.timestamp
    
    def flush(self):
        """Flush this block, abstract
        
        Arguments:
        - `self`:
        """
        raise ValueError("Unimplemented")
        
    def load(self):
        """load this block, abstract
        
        Arguments:
        - `self`:
        """
        raise ValueError("Unimplemented")
    
    def offset(self):
        """Get block disk offset
         
        Arguments:
        - `self`:
        """
        return self.blocknum * BLOCKSIZE    
    
    
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
        self.keys     = []
        self.pointers = []
        self.load()
        
    def load(self):
        """Load keys and pointers from disk.
        
        Arguments:
        - `self`:
        """
        if self.keys or self.pointers:
            raise ValueError("keys and pointers must be empty")
        self._refresh_fullness()
        fh = self._datafile.fh
        fh.seek(self.offset())
        for _ in xrange(MAXLEAFKEYS):
            k, pb, po = struct.unpack("QHH", fh.read(12))
            if k == 0:
                continue
            self.insert(k, (pb, po))
        self._refresh_fullness()

    def flush(self):
        """Flush keys and pointers to disk.
        
        Arguments:
        - `self`:
        """
        fh = self._datafile.fh
        fh.seek(self.offset())
        # XXX use BLOCKSIZE instead of 4096
        fh.write(struct.pack("4096s", "0"))
        fh.seek(self.offset())
        for i, k in enumerate(self.keys):
            p = self.pointers[i]
            s = struct.pack("QHH", k, p[0], p[1])
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())
        self.keys = []
        self.pointers = []
    
    # XXX this is wong
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        self._datafile.set_fullness(self.blocknum,
                                    len(self.keys) == MAXLEAFKEYS)

    def insert(self, key, pointer):
        """Insert a record, leaf MUST NOT be full.
        
        Arguments:
        - `self`: 
        - `rowid`: rowid to insert.
        """
        if type(key) is not types.IntType:
            raise TypeError("Key not an integer")
        if type(pointer) is not types.TupleType:
            raise TypeError("Pointer not a tuple")
        
        if self.full():
            raise ValueError("Leaf is already full you dumbass !")

        pos = 0
        for k in self.keys:
            if k > key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, key)
        self.pointers.insert(pos, pointer)
        self._refresh_fullness()

    def insert_split(self, key, pointer, newleaf):
        """Split the records with rightleaf, top-half records will go to
        newleaf.
        
        Arguments:
        - `self`:
        - `newleaf`: The new right(higher) leafblock.
        """
        if type(key) is not types.IntType:
            raise TypeError("Key not an integer")
        if type(pointer) is not types.TupleType:
            raise TypeError("Pointer not a tuple")
        # can only split an already full leaf
        if not self.full():
            raise ValueError("Trying to split leaf which isn't full!")
        # Insert record to force a split
        pos = 0
        for k in self.keys:
            if k > key:
                break
            pos = pos + 1
            
        self.keys.insert(pos, key)
        self.pointers.insert(pos, pointer)
        
        # Do the splitting
        for _ in xrange(0, len(self.keys)/2 + 1):
            k = self.keys.pop()
            p = self.pointers.pop()
            newleaf.insert(k, p)
        
        self._refresh_fullness()
        newleaf._refresh_fullness()
        assert not self.full()
        assert not newleaf.full()
        # Return the middlekey and middle pointer
        return newleaf.keys[0], newleaf.pointers[0]

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
        self.load()
        
    def _refresh_fullness(self):
        """Refresh fullness
        
        Arguments:
        - `self`:
        """
        self._datafile.set_fullness(self.blocknum,
                                    len(self.keys) == MAXBRANCHKEYS)
        
    def flush(self):
        """Flush keys and pointers to disk.
        
        Arguments:
        - `self`:
        """
        fh = self._datafile.fh
        fh.seek(self.offset())
        # XXX use BLOCKSIZE instead of 4096
        fh.write(struct.pack("4096s", "0"))
        fh.seek(self.offset())
        for i, k in enumerate(self.keys):
            l = self.pointers[i]
            r = self.pointers[i+1]
            s = struct.pack("QHH", k, l, r)
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())
        self.keys = []
        self.pointers = []
        
    def load(self):
        """Load keys and pointers from disk.
        
        Arguments:
        - `self`:
        """
        if self.keys or self.pointers:
            raise ValueError("keys and pointers must be empty")
        self._refresh_fullness()
        fh = self._datafile.fh
        fh.seek(self.offset())
        for _ in xrange(MAXBRANCHKEYS):
            k, l, r = struct.unpack("QHH", fh.read(12))
            if k == 0:
                continue
            self.new_insert(l, k, r)
        self._refresh_fullness()

    def new_insert(self, leftblocknum, key, rightblocknum):
        if self.full():
            raise ValueError("Branch is already full you dumbass !")
        pos = 0
        for k in self.keys:
            if k > key:
                break
            pos = pos + 1
        self.keys.insert(pos, key)

        if not self.pointers:
            self.pointers.append(leftblocknum)
            self.pointers.append(rightblocknum)
        else:
            # if self.pointers[pos] != leftblocknum:
            #     raise ValueError("Deu merda {0} {1}".format(
            #             self.pointers[pos], leftblocknum))
            self.pointers.insert(pos + 1, rightblocknum)

        self._refresh_fullness()

    def new_insert_split(self, leftblocknum, key, rightblocknum, newindex):
        if not self.full():
            raise ValueError("Branch isn't full !")

        print ("new_insert_split {0} {1} {2}".format(leftblocknum, key, rightblocknum))
        pos = 0
        for k in self.keys:
            if k > key:
                break
            pos = pos + 1
        self.keys.insert(pos, key)

        if not self.pointers:
            self.pointers.append(leftblocknum)
            self.pointers.append(rightblocknum)
        else:
            # if self.pointers[pos] != leftblocknum:
            #     raise ValueError("Deu merda {0} {1}".format(
            #             self.pointers[pos], leftblocknum))
            self.pointers.insert(pos + 1, rightblocknum)
            self.pointers[pos] = leftblocknum
            #self.pointers.insert(pos + 1, rightblocknum)

        print (self.keys)
        print (self.pointers)
        
        # for _ in xrange(0, len(self.keys)/2 + 1):
        #     k = self.keys.pop()
        #     p = self.pointers.pop()
        #     p2 = self.pointers[0]
        #     newindex.new_insert(p, k, p2)

        # split antes, keys eh impar, pointers eh par
        tmp_selfkeys = self.keys[:len(self.keys)/2+1]
        tmp_selfpointers = self.pointers[:len(self.pointers)/2+1]
        if len(tmp_selfkeys) + 1!= len(tmp_selfpointers) :
            raise ValueError("tmp_selfkeys {0} != tmp_selfpointers {1}"
                             .format(len(tmp_selfkeys), len(tmp_selfpointers)))
        
        # Top half
        newindex.keys = self.keys[len(self.keys)/2+1:]
        newindex.pointers = self.pointers[len(self.pointers)/2:]
        if len(newindex.keys) + 1 != len(newindex.pointers):
            raise ValueError("newindex.keys {0} != newindex.pointers {1}"
                             .format(len(newindex.keys), len(newindex.pointers)))

        print("newindex keys", newindex.keys)
        print("newindex pointers", newindex.pointers)
        # tiro a middlekey
        indexmiddlekey  = newindex.keys.pop(0)
        indexmiddlepointer = newindex.pointers.pop(0) # ou 1 ?
        # copia do tmp
        self.keys = tmp_selfkeys
        self.pointers = tmp_selfpointers
        
        self._refresh_fullness()
        newindex._refresh_fullness()

        print("index {0} newindex {1}".format(self.blocknum, newindex.blocknum))
        print("indexmiddlekey = {0}, indexmiddlepointer = {1}".format(
                indexmiddlekey, indexmiddlepointer
                ))
        
        indexmiddlepointer = rightblocknum
        return indexmiddlekey, indexmiddlepointer
        
        
    def _insert(self, leftblocknum, key, rightblocknum):
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

    def insert_split(self, lleaf, leafmiddlekey, rleaf, newindex):
        """
        
        Arguments:
        - `self`:
        """

        # leafmiddlekey = 85
        # lleaf = 75,80
        # rleaf = 85,90,95...
        # INSERE O LEAFMIDDLE-KEY, AE FAZ O SPLIT, DEPOIS VE ONDE TA O MIDDLEKEY E
        #SETA O RLEAF PARENT PRA ONDE TA O MIDDLE-KEY
        
        pos = 0
        for k in self.keys:
            if k > leafmiddlekey:
                break
            pos = pos + 1
        self.keys.insert(pos, leafmiddlekey)
        self.pointers.insert(pos, lleaf.blocknum)
        self.pointers.insert(pos + 1, rleaf.blocknum)
        
        #self.pointers.insert(pos, pointer)
        # Do the splitting
        for _ in xrange(0, len(self.keys)/2 + 1):
            k = self.keys.pop()
            p = self.pointers.pop()
            newindex.insert(p, k, self.pointers[0])

        middlekey = newindex.keys.pop(0)
        self._refresh_fullness()
        newindex._refresh_fullness()

        if lleaf.blocknum in self.pointers:
            print("left leaf {0} in self {1}".format(lleaf.blocknum, self.blocknum))
        else:
            print("left leaf {0} in newindex {1}".format(lleaf.blocknum, newindex.blocknum))
            
        if rleaf.blocknum in self.pointers:
            print("left leaf {0} in self {1}".format(rleaf.blocknum, self.blocknum))
        else:
            print("left leaf {0} in newindex {1}".format(rleaf.blocknum, newindex.blocknum))
        
        # # Search leafmiddlekey to fix parent pointer
        # if leafmiddlekey in self.keys:
        #     rleaf.set_parent(self)
        # elif leafmiddlekey in newindex.keys:
        #     rleaf.set_parent(newindex)
        # else:
        #     raise ValueError("Fudeu")
        
        return middlekey, (self.blocknum, newindex.blocknum)
        
        
        # middlekey_pos = len(self.keys)/2
        # middlekey = self.keys.pop(middlekey_pos)
        # # Do the splitting
        # for i in xrange(0, middlekey_pos - 1):
        #     k = self.keys.pop(middlekey_pos)
        #     newindex.keys.append(k)
        #     # #p = self.pointers.pop()
        #     # newleaf.insert(k, p)


        # return middlekey
    
        
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
        self.records = []
        self.load()
        
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

    def load(self):
        """Load records from disk.
        
        Arguments:
        - `self`:
        """
        if self.records:
            raise ValueError("records must be empty")
        self._refresh_fullness()
        fh = self._datafile.fh
        fh.seek(self.offset())
        for x in xrange(MAXRECORDS):
            k, desc = struct.unpack("Q56s", fh.read(64))
            r = Record(self.blocknum, x)
            r.key = k
            r.desc = desc.split('\x00')[0]
            self.records.append(r)
        self._refresh_fullness()

    def flush(self):
        """Flush records.
        
        Arguments:
        - `self`:
        """
        fh = self._datafile.fh
        fh.seek(self.offset())
        # XXX use BLOCKSIZE instead of 4096
        fh.write(struct.pack("4096s", "0"))
        fh.seek(self.offset())
        for r in self.records:
            s = struct.pack("Q56s", r.key, r.desc)
            fh.write(s)
        fh.flush()
        os.fsync(fh.fileno())
        self.records = []
        
        
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
        self.path    = path
        # Make sure root is there.
        root         = self._buf.alloc(LEAF)
        self.rootnum = root.blocknum

    def close(self):
        """Save all state to disk
        
        Arguments:
        - `self`:
        """
        for i in self._buf._frames:
            b = self._buf._frames[i]
            b.flush()
        f = open(self._buf._datafile.path + ".pickle", "w")
        self._buf._frames = {}
        self._buf._datafile.fh.close()
        self._buf._datafile.fh = None
        pickle.dump(self, f)
        f.close()
        del self
    
    def get_root(self):
        """Fetch root block
        
        Arguments:
        - `self`:
        """
        return self._buf.get_block(self.rootnum)

    def randomize(self, n):
        """Insert n random registers
        
        Arguments:
        - `self`:
        - `n`: Number of random registers
        """
        for x in xrange(1, n + 1):
            k = random.randrange(1, 4000000)
            self.insert(k, "Descricao {0}".format(k))
    
    def search_leaf(self, key):
        """Search the leaf given a key insertion.
        
        Arguments:
        - `self`:
        - `key`: pk
        """
        b = self.get_root()

        while b.blocktype != LEAF:
            pos = 0
            for k in b.keys:
                if key >= k:
                    pos = pos + 1
                else:
                    break
            b = self._buf.get_block(b.pointers[pos])

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

    def lookup_pprint(self, key):
        """Lookup with pretty printing :-)
        
        Arguments:
        - `self`:
        - `key`: Record key to be looked up
        """
        r = self.lookup(key)
        if not r:
            print("Record {0}".format(key))
        else:
            print("Record {0} => {1}".format(r.key, r.desc))
        
    def lookup(self, key):
        """Lookup for a given record.
        
        Arguments:
        - `self`:
        - `key`: record key (pk)
        """
        # Get the leaf
        leaf = self.search_leaf(key)
        if not leaf:
            return None
        # Get the record pointer
        p = None
        for i, k in enumerate(leaf.keys):
            if k == key:
                p = leaf.pointers[i]
                break
        if p is None:
            return None
        # Get the record block and return the record.
        rb = self._buf.get_block(p[0])
        return rb.records[p[1]]

    def update(self, key, desc):
        """Update a record of key to new desc
        
        Arguments:
        - `self`:
        - `key`: Key of record
        - `desc`: New description of record
        """
        rec      = self.lookup(key)
        if not rec:
            return None
        rec.desc = desc
        return rec
    
    def insert(self, key, desc):
        """Insert a record into bplustree, handles all cases
        
        Arguments:
        - `self`:
        - `key`: Record key
        - `desc`: Record desc
        """
        if key < 1:
            raise ValueError("Invalid key !!")
        print("inserting {0}".format(key))
        # Avoid double insert
        if self.lookup(key):
            return None
        r           = self.make_record(key, desc)
        rec_key     = r.key
        rec_pointer = (r.blocknum, r.offset)
        leafblock   = self.search_leaf(rec_key)
        # Case 1: Yey ! leaf is not full
        if not leafblock.full():
            leafblock.insert(rec_key, rec_pointer)
            return
        
        # Awww leaf is full :(
        indexblock = leafblock.get_parent()

        # Case 2: Leaf is full, but parent isn't or root splitting
        # Split the leaf, move top half to new leaf
        newleafblock                     = self._buf.alloc(LEAF)
        # Insert and split
        leafmiddlekey, leafmiddlepointer = \
            leafblock.insert_split(rec_key, rec_pointer, newleafblock)
        # Leaf Root splitting
        if leafblock.is_root():
            # Alloc a new root
            newroot                      = self._buf.alloc(BRANCH)
            self.rootnum                 = newroot.blocknum
            indexblock                   = newroot
            leafblock.set_parent(indexblock)
        # Finish case 2
        if not indexblock.full():
            leafblock.set_parent(indexblock)
            newleafblock.set_parent(indexblock)
            indexblock.new_insert(leafblock.blocknum, leafmiddlekey, 
                                 newleafblock.blocknum)
            return
            
        raise ValueError("Shouldn't reach here")
    
#         while indexblock and indexblock.full():
#             # Alloc a new branchblock, will be the neighbour of our current
#             # indexblock and will have the top-half keys of indexblock.
#             newindexblock            = self._buf.alloc(BRANCH)
#             # Insert the leaf key into indexblock (which is full) and force a
#             # splitting, after the call, newindexblock should have the top-half
#             # keys of indexblock, the middlekey is the first key of
#             # newindexblock.
#             middlekey, middlepointer = indexblock.new_insert_split(leafblock.blocknum,
#                                                     leafmiddlekey,
#                                                     newleafblock.blocknum,
#                                                     newindexblock)
            
#             # Now we do not know if leafblock is in indexblock or
#             # newindexblock, we don't care

#             # We must now insert the middle key in the upperindex (parent),
#             # with pointers to indexblock and newindexblock, if there is no
#             # parent, we get a new root.
#             upperindexblock     = indexblock.get_parent()
#             # upperindexblock is the parent of our current index.
#             if upperindexblock is None:
#                 # Root splitting
#                 # Alloc a new root
#                 newroot         = self._buf.alloc(BRANCH)
#                 self.rootnum    = newroot.blocknum
#                 upperindexblock = newroot

#             # Now link the fucking branches
#             upperindexblock.new_insert(indexblock.blocknum, middlekey,
#                                    middlepointer)
#             indexblock.set_parent(upperindexblock)
#             # Go up
#             indexblock = indexblock.get_parent()
#             print("upperindex = {0}".format(upperindexblock.blocknum))
#             break
#             # We should have the following
# """
#            _________________
#           /                      \
#           | upperindexblock |
#           \_________________/
#             /                    \
#            /                     \
#           /                      \
#          /                       \
#        _/_______________       \_________________             
#       /                 \      / \
#       |  indexblock     |      | newindexblock   |
#       \_________________/      \_________________/

# """
#         # End of case 3

def load_from_file(path):
    """Load a BplusTree from file and return the object
    
    Arguments:
    - `path`: file path
    """
    f = open(path, "r+b")
    bp = pickle.load(f)
    f.close()
    bp._buf._datafile.fh = open(bp._buf._datafile.path, "r+b", BLOCKSIZE)

    return bp
