PREFIX?=/usr/local
BINDIR=${PREFIX}/bin
MANDIR= ${PREFIX}/man/cat

PROG=	sgbd
SRCS=	sgbd.c

#MAN=	mdnsd.8

CFLAGS+= -g -Wall -I${.CURDIR} -I../ -O0
CFLAGS+= -Wstrict-prototypes -Wmissing-prototypes
CFLAGS+= -Wmissing-declarations
CFLAGS+= -Wshadow -Wpointer-arith -Wcast-qual
CFLAGS+= -Wsign-compare
#LDADD+= -levent -lutil
#DPADD+= ${LIBEVENT} ${LIBUTIL}

.include <bsd.prog.mk>
.include <bsd.man.mk>
