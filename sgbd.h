/*
 * Copyright (c) 2010 Christiano F. Haesbaert <haesbaert@haesbaert.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef SGBD_H
#define SGBD_H

#include <err.h>
#include <errno.h>
#include <event.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#define FRAMENUM	256	/* Numer of buffercache frames */
#define BLKNUM		8192	/* Total number of blocks */
#define INONUM		64	/* Inodes per block */
#define INOSZ		64	/* Inode size */

struct rowid {
	u_int16_t rid_block;
	u_int16_t rid_inode;
};

struct inode {
	struct rowid	 ino_rid;
	void		*ino_data;
};

enum metainode_state {
	INODE_STA_FREE,
	INODE_STA_USED
};

struct metablock {
	u_int16_t	mb_block; /* Block offset */
	char		mb_metainodes[INONUM];
};

struct frame {
	struct metablock	*fr_mb;
#define fr_inodes fr_mb->mb_inodes	
	struct timespec		 fr_timestamp;
	char			 fr_data[INONUM][INOSZ]; /* 64 inodes of 64 bytes */
};

/* Filesystem (Datafile) */
struct filesystem {
	char			*fs_backstoragepath;
	FILE			*fs_backstorage;
	struct metablock	 fs_metablocks[BLKNUM];
};

struct buffercache {
	struct frame bc_frames[FRAMENUM];
};

struct metablock	*fs_any_free(void);
struct frame		*bc_next_victim(void);
struct frame		*bc_swap(struct metablock *);
struct frame		*bc_frame_by_rid(struct rowid *rid);
void			 fr_load(struct frame *, struct metablock *);
void			 fr_flush(struct frame *);
struct inode		*fr_inode_alloc(struct frame *);
void			 fr_timestamp(struct frame *);
struct inode		*inode_by_rid(struct rowid *);
struct inode		*inode_alloc(void);
void			 inode_free(struct inode *);











#endif

	
