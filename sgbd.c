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

struct filesystem fs;

void *
bc_alloc_ino(void)
{
	struct frame		*fr;
	struct metablock	*mb;
	struct metainode	*mi;
	int			 i;
	
	/* Search for a free inode in buffercache */
	mi = bc_get_free_mi();
	
	/* If we have no free nodes in buffercache, swap */
	if (mi == NULL) {
		struct metablock *mb_out, *mb_in;
		/* mb_in  = fs_any_free(); */
		fr = bc_swapin(mb_in);
		mi = fr_get_free_mi(fr);
		if (mi == NULL)
			errx(1, "bc_alloc_ino: still can't find a free mi");
	}
	
	/* Cool, now we know mi points to a valid metainode */
	return (mi->mi_data);
}

void
bc_free_ino(void *datum)
{
	/* TODO */
	;
}

struct metainode *
bc_get_free_mi(void)
{
	struct frame		*fr;
	struct metainode	*mi;
	int i;
	
	for (i = 0; i < FRAMENUM; i++) {
		fr = &fs_frames[i];
		/* Skip free frames */
		if (fr_state == FR_STA_UNWIRED)
			continue;

		mi = fr_get_free_mi(fr);
		if (mi != NULL)
			return (mi);
	}
	
	return (NULL);
}

struct frame *
bc_swap(struct metablock *mb_in)
{
	struct frame *fr;
	struct metainode *mi;
	struct mb_out *mb_out
	int i;
	
	/* mb_out = next_victim(); */
	/* Sanity check */
	if (!MBWIRED(mb_out))
		errx(1, "bc_swap: mb_out is already unwired");
	if (MBWIRED(mb_in))
		errx(1, "bc_swap: mb_in is already wired");
	
	
	/* Save frame to swap */
	fr = mb_out->mb_fr;
	mb_out->mb_fr = NULL;
	/* Flush frame */
	fr_flush(fr);
	/* Wire in */
	mb_in->mb_fr = fr;
	fr->fr_state = FR_STA_UNWIRED;
	fr->fr_mb = mb_in;
	/* Load frame */
	fr_load(fr);
	fr->fr_state = FR_STA_WIRED;
	/* Update timestamp */
	fr_timestamp(fr);
	/* Repoint metainodes data */
	for (i = 0; i < INONUM; i++) {
		mi = &mb_out->mb_metainodes[i];
		mi->mi_data = NULL;
		mi = &mb_in->mb_metainodes[i];
		mi->mi_data = &fr->fr_data[i];
	}
	
	return (fr);
}

/*
 * Search for a free metainode in frame fr.
 */
struct metainode *
fr_get_free_mi(struct frame *fr)
{
	struct metainode *mi;
	struct metablock *mb;
	int i;

	/* Sanity check */
	if (fr->fr_state == FR_STA_UNWIRED)
		errx(1, "bc_search_free: unwired frame");
	
	fr_timestamp(fr);
	mb = fr->fr_mb;
	/* Find a free mi */
	for (i = 0; i < INONUM; i++) {
		mi = &mb->mb_inodes[i];
		if (mi->mi_state == MI_STA_FREE)
			return (mi);
	}

	return (NULL);
}

void
fr_flush(struct frame *fr)
{
	FILE			*f;
	struct metablock	*mb;

	/* Sanity check */
	if (fr->fr_state != FR_STA_WIRED)
		errx(1, "fr_flush: flush on unwired frame");
	f  = fs->fs_backstorage;
	mb = fr->fr_mb;
	/* fseek into block offset */
	if (fseek(f, mb->mb_offset, SEEK_SET) == -1)
		err(1, "fseek");
	/* Flush */
	if (fwrite(fr->fr_data, sizeof(fr->fr_data), 1, f)
	    != sizeof(fr->fr_data))
		err(1, "fwrite");
}

void
fr_load(struct frame *fr)
{
	struct metablock	*mb;
	FILE			*f;
	
	/* Sanity check */
	if (fr->fr_state != FR_STA_UNWIRED)
		errx(1, "fr_flush: load on wired frame");
	
	fr_timestamp(fr);
	f  = fs->fs_backstorage;
	mb = fr->fr_mb;
	/* fseek into block offset */
	if (fseek(f, mb->mb_offset, SEEK_SET) == -1)
		err(1, "fseek");
	/* Flush */
	if (fread(fr->fr_data, sizeof(fr->fr_data), 1, f)
	    != sizeof(fr->fr_data))
		err(1, "fread");
}

void
fr_timestamp(struct frame *fr)
{
	clock_gettime(CLOCK_MONOTONIC, &fr->fr_tp);
}

struct frame *
bc_next_victim(void)
{
	struct frame *fr;
	struct frame *victim;
	
	    
}

struct metablock *
fs_get_any_free(void)
{
	int i;

	/* TODO */
	return (NULL);
}

int
main(void)
{
	
	return (0);
}
