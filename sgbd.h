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

#define MBWIRED(mb) ((mb)->mb_fr != NULL)

struct rowid {
	u_int8_t  pk;
	u_int16_t blk_off;
};

struct table {
	u_int8_t	ta_pk;
	char		ta_desc[56];
};

struct bplus_node {
	struct rowid bp_parent;
	struct rowid bp_left;
	struct rowid bp_right;
};

enum frame_state {
	FR_STA_UNWIRED,
	FR_STA_WIRED
};

struct frame {
	struct metablock	*fr_mb;
	enum frame_state	 fr_state;
	struct timespec		 fr_tp;
	char			 fr_data[INONUM][INOSZ]; /* 64 inodes of 64 bytes */
};

enum metainode_state {
	MI_STA_FREE,
	MI_STA_USED
};

struct metainode {
	struct metablock	*mi_blk;	/* Parent block */
	enum metainode_state	 mi_state;
	void			*mi_data;
};

struct metablock {
	u_int16_t		 mb_off; 	/* Block offset */
	struct frame		*mb_fr; 	/* Not null if wired */
	struct metainode	 mb_inodes[INONUM]; /* Inode list */
};

/* Filesystem (Datafile) */
struct filesystem {
	char			*fs_backstoragepath;
	FILE			*fs_backstorage;
	struct metablock	 fs_metablocks[BLKNUM];
	struct frame		 fs_frames[FRAMENUM];
};

void *			bc_alloc_ino(void);
void			bc_free_ino(void *);
struct metainode *	bc_get_free_mi(void);
struct frame *		bc_swap(struct metablock *, struct metablock *);
struct metainode *	fr_get_free_mi(struct frame *);
void			fr_flush(struct frame *);
void			fr_load(struct frame *);







#endif

	
