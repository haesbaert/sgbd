/*
 * Copyright (c) 2011 Christiano F. Haesbaert <haesbaert@haesbaert.org>
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

#include "sgbd.h"

struct filesystem filesystem;
struct buffercache buffercache;
static int vflag = 0;

/* NEW STUFF */


/*
 * Start the fucken ball.
 */
void
fs_init(void)
{
	int new = 0;
	struct stat sb;

	if (filesystem.fs_backstoragepath == NULL)
		filesystem.fs_backstoragepath = "/tmp/sgbd.fs";
	/* Check if this is a new filesystem */
	if (stat(filesystem.fs_backstoragepath, &sb) == -1) {
		if (errno != ENOENT)
			errx(1, "stat");
		new = 1;
	}

	/* Open file */
	filesystem.fs_backstorage = fopen(filesystem.fs_backstoragepath, "w+");
	if (filesystem.fs_backstorage == NULL)
		err(1, "fopen %s", filesystem.fs_backstoragepath);
	if (new) {
		char dummyblock[INOSZ];
		
		if (vflag)
			fprintf(stderr, "new filesystem at %s\n",
			    filesystem.fs_backstoragepath);
		bzero(dummyblock, sizeof(dummyblock));
		if (fwrite(dummyblock, sizeof(dummyblock), INONUM,
		    filesystem.fs_backstorage) != INONUM)
			err(1, "fwrite");
	}
}

/*
 * Return a metablock with at least one free inode. Pure.
 */
struct metablock *
fs_any_free(void)
{
	struct metablock *mb;
	int i, j;
	
	/* Search for a free inode in filesystem */
	for (i = 0; i < BLKNUM; i++) {
		mb = &filesystem.fs_metablocks[i];
		
		for (j = 0; j < INONUM; j++) {
			if (mb->mb_metainodes[j] == INODE_STA_FREE)
				return (mb);
		}
	}
	/* No free inodes, choke */
	errno = ENOMEM;
	err(1, "fs_any_free");
	
	return (NULL);    
}

void
bc_init(void)
{
	/* TODO */
}

struct frame *
bc_next_victim(void)
{
	struct frame *fr, *fr_victim;
	int i;
	
	fr_victim = &buffercache.bc_frames[0];
	for (i = 0; i < FRAMENUM; i++) {
		fr = &buffercache.bc_frames[i];
		/* We have a free frame, use it */
		if (fr->fr_mb == NULL)
			return (fr);
		/* Search for the lowest timestamp (least recently used) */
		if (timespeccmp(&fr->fr_timestamp, &fr_victim->fr_timestamp, <))
			fr_victim = fr;
	}
	
	return (fr_victim);
}

struct frame *
bc_swap(struct metablock *mb)
{
	struct frame *fr;
	
	/* Search for a possible victim */
	fr = bc_next_victim();
	/* If the frame is wired, flush and unwire */
	if (fr->fr_mb != NULL)
		fr_flush(fr);
	/* Sanity check */
	if (fr->fr_mb != NULL)
		errx(1, "Frame is still null");
	/* Wire block into frame */
	fr_load(fr, mb);
	fr_timestamp(fr);
	
	return (fr);
}

struct frame *
bc_frame_by_rid(struct rowid *rid)
{
	struct frame *fr;
	struct metablock *mb;
	int i;
	
	/* Sanity check */
	if (rid->rid_block >= BLKNUM ||
	    rid->rid_inode >= INONUM)
		errx(1, "Invalid rid %u %u", rid->rid_block, rid->rid_inode);
	
	/* Check if block is wired */
	for (i = 0; i < FRAMENUM; i++) {
		fr = &buffercache.bc_frames[i];
		mb = fr->fr_mb;
		/* Skip Unwired frames*/
		if (mb == NULL)
			continue;
		if (mb->mb_block == rid->rid_block) {
			fr_timestamp(fr);
			return (fr);
		}
	}
	
	/* Damn, unwired :/, we need to swap */
	mb = &filesystem.fs_metablocks[rid->rid_block];
	/* Can't fail */
	return (bc_swap(mb));
}

void
fr_load(struct frame *fr, struct metablock *mb)
{
	/* FILE *f; */
	
	/* Sanity check */
	if (fr->fr_mb != NULL)
		errx(1, "fr_flush: load on wired frame");
	
	fr_timestamp(fr);
	/* f  = filesystem.fs_backstorage; */
	/* fseek into block offset */
	/* if (fseek(f, mb->mb_block, SEEK_SET) == -1) */
	/* 	err(1, "fseek"); */
	/* Flush */
	/* if (fread(fr->fr_data, sizeof(fr->fr_data), 1, f) */
	/*     != sizeof(fr->fr_data)) */
	/* 	err(1, "fread"); */
	fr->fr_mb = mb;
}

void
fr_flush(struct frame *fr)
{
	/* FILE			*f; */
	struct metablock	*mb;

	/* Sanity check */
	if (fr->fr_mb == NULL)
		errx(1, "fr_flush: flush on unwired frame");
	/* f  = filesystem.fs_backstorage; */
	mb = fr->fr_mb;
	/* fseek into block offset */
	/* if (fseek(f, mb->mb_block, SEEK_SET) == -1) */
	/* 	err(1, "fseek"); */
	/* Flush */
	/* if (fwrite(fr->fr_data, sizeof(fr->fr_data), 1, f) */
	/*     != sizeof(fr->fr_data)) */
	/* 	err(1, "fwrite"); */
	/* Unwire */
	fr->fr_mb = NULL;
}

/*
 * Alloc an inode from any free metainode in frame fr. Not pure.
 */
struct inode *
fr_inode_alloc(struct frame *fr)
{
	struct inode *ino;
	struct metablock *mb;
	int j;
	
	mb = fr->fr_mb;
	for (j = 0; j < INONUM; j++) {
		if (mb->mb_metainodes[j] == INODE_STA_USED)
			continue;
		/* Cool, we have the frame, make an inode */
		if ((ino = calloc(1, sizeof(*ino))) == NULL)
			err(1, "calloc");

		ino->ino_rid.rid_block = mb->mb_block;
		ino->ino_rid.rid_inode = j;
		ino->ino_data = &fr->fr_data[j];
		mb->mb_metainodes[j] = INODE_STA_USED;
		fr_timestamp(fr);
			
		return (ino);
	}
	
	return (NULL);
}

void
fr_timestamp(struct frame *fr)
{
	clock_gettime(CLOCK_MONOTONIC, &fr->fr_timestamp);
}

struct inode *
inode_by_rid(struct rowid *rid)
{
	struct frame *fr;
	struct inode *ino;
	
	/* Sanity check */
	if (rid->rid_block >= BLKNUM ||
	    rid->rid_inode >= INONUM)
		errx(1, "Invalid rid %u %u", rid->rid_block, rid->rid_inode);
	
	fr = bc_frame_by_rid(rid);
	/* Cool, we have the frame, make an inode */
	if ((ino = calloc(1, sizeof(*ino))) == NULL)
		err(1, "calloc");
	ino->ino_rid = *rid;
	ino->ino_data = &fr->fr_data[rid->rid_inode];
	
	return (ino);
}


struct inode *
inode_alloc(void)
{
	struct frame *fr;
	struct metablock *mb;
	struct inode *ino;
	int i;
	
	/* Search for any free inode in buffercache */
	for (i = 0; i < FRAMENUM; i++) {
		fr = &buffercache.bc_frames[i];
		mb = fr->fr_mb;
		/* Skip Unwired frames*/
		if (mb == NULL)
			continue;
		
		ino = fr_inode_alloc(fr);
		if (ino != NULL)
			return (ino);
	}
	/* If we got here, we'll need to swap */
	mb = fs_any_free();
	fr = bc_swap(mb);
	fr_timestamp(fr);
	ino = fr_inode_alloc(fr);
	if (ino != NULL)
		return (ino);
	
	errx(1, "inode_alloc: whoops, still no free inode");
	return (NULL);		/* NOTREACHED */
}

void
inode_free(struct inode *ino)
{
	struct metablock *mb;
	
	/* Holy shit batman, that's ugly. */
	mb = &filesystem.fs_metablocks[ino->ino_rid.rid_block];
	mb->mb_metainodes[ino->ino_rid.rid_inode] = INODE_STA_FREE;
	free(ino);
}

static void
test_one(void)
{
	printf("Running test 1\n");
	/* TODO */
}

int
main(int argc, char *argv[])
{
	char ch;
	
	while ((ch = getopt(argc, argv, "v")) != -1) {
		switch (ch) {
		case 'v':
			vflag++;
			break;
		default:
			errx(1, "TODO USAGE()");
			break;	/* NOTREACHED */
		}
	}

	printf("verbose level: %d\n", vflag);
	
	fs_init();
	/* Call lex main. */
	/* TODO yylex() */
	
	return (0);
}
