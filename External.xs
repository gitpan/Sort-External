#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#define SORTEX_UTF8_FLAG 0x1
#define SORTEX_TAINTED_FLAG 0x2

MODULE = Sort::External		PACKAGE = Sort::External		

PROTOTYPES: DISABLE

=for comment

Return a rough estimate of the memory occupied by the arguments.

=cut

SV*
_add_up_lengths (...)
PREINIT:
    UV   sum;
    int  i;
    SV  *element;
CODE:
{
    sum = 0;
    /* add up the allocated lengths of the scalars on the stack */
    for (i = 0; i < items; i++) {
        element = ST(i);   
        sum += sv_len(element);
    }
    /* add 15 bytes per scalar as overhead */
    sum += 15 * items;

    RETVAL = newSVuv(sum);
}
OUTPUT: RETVAL

=for comment

Print an array of scalars to a file using an encoding which allows them to be
recovered later.

=cut

SV*
_print_to_sortfile (fh, ...)
    PerlIO *fh;
PREINIT:
    SV       *scratch_sv;   
    int		  i;
    int		  check;        /* check value for i/o ops */
    int       type;         /* the type of an SV */
    char     *string;       /* pointer to the string in an SV */
    char      num_buf[5];   /* buffer to hold compressed integer */
    char     *end_of_buf;   /* remember the end of the buffer */
    char     *encoded_len;  /* used when encoding compressed integer */
    STRLEN    string_len;
    UV        aUV;
    char      flags;        /* status flags */
PPCODE:
{
    /* prepare a to hold the compressed integer */
    end_of_buf = num_buf + 5;
    
    for (i = 1; i < items; i++) {
        /* retrieve one scalar from the Perl stack */
        scratch_sv   = ST(i);
        string     = SvPV(scratch_sv, string_len);
        aUV        = string_len;

        /* throw an error if the item isn't a plain old scalar */
        type = SvTYPE(scratch_sv);
        if (type > SVt_PVMG || type == SVt_RV) {
            croak("can't handle anything other than plain scalars");
        }
        
        /* encode the length of the scalar as a BER compressed integer */
        encoded_len = end_of_buf;
        do {
            *--encoded_len = (char)((aUV & 0x7f) | 0x80);
            aUV >>= 7;
        } while (aUV);
        *(end_of_buf - 1) &= 0x7f;  

        /* record utf8 and taint status */
        flags = 0;
        if (SvUTF8(scratch_sv)) {
            flags |= SORTEX_UTF8_FLAG;
        }
        if (SvTAINTED(scratch_sv)) {
            flags |= SORTEX_TAINTED_FLAG;
        }

        /* print len . string . flags to fh */
        check = PerlIO_write(fh, encoded_len, (end_of_buf - encoded_len));
        if (check < 0) {
            croak("PerlIO failed: errno %d", errno);
        }
        check = PerlIO_write(fh, string, string_len);
        if (check != string_len) {
            croak("PerlIO failed: tried to write %"UVuf" bytes, wrote %d",
                (UV)string_len, check);
        }
        check = PerlIO_write(fh, &flags, 1);
        if (check != 1) {
            croak("PerlIO failed: errno %d", errno);
        }
    }
}

MODULE = Sort::External   PACKAGE = Sort::External::Buffer

=for comment

Recover scalars from disk, 32k at a time.

=cut

SV*
_refill_buffer (obj_hash, ...)
    HV *obj_hash;
PREINIT:
    SV*       scratch_sv;
    PerlIO   *fh;           /* the object's tempfile handle */
    AV       *buffarray_av; /* the object's buffer array of items */
    char      num_buf[5];   /* buffer to hold compressed integer */
    char     *read_buf;     /* scratch pointer */
    UV        item_length;  /* length of a recovered SV */
    STRLEN    amount_read;  /* number of bytes we've read off disk */
    int       num_items;    /* number of items we've recovered */
    int		  check;        /* check value for i/o ops */
    char      flags;        /* status flags */
CODE:
{
    /* extract filehandle and buffer array from object */   
    scratch_sv = *(hv_fetch( obj_hash, "tf_handle", 9, 0 ));
    fh         = IoIFP( sv_2io(scratch_sv) );
    scratch_sv  = *(hv_fetch( obj_hash, "buffarray", 9, 0 ));
    buffarray_av = (AV*)SvRV(scratch_sv);

    amount_read = 0;
    num_items   = 0;
    while (1) {
        /* bail if we've read 32k */
        if (amount_read > 32768)
            break;

        /* retrieve and decode len */
        item_length = 0;
        while (1) {
            read_buf = num_buf;
            check = PerlIO_read(fh, read_buf, 1);
            if (check < 0) {
                croak("PerlIO failed: errno %d", errno);
            }
            else if (check == 0) {
                break;
            }
            amount_read++;
            item_length = (item_length << 7) | (*read_buf & 0x7f);
            if ((U8)(*read_buf) < 0x80)
                break; 
        }

        /* bail (by design) if the attempt to read the length failed */
        if (PerlIO_eof(fh)) 
            break;

        /* recover the stringified scalar from disk */
        scratch_sv = newSV(item_length + 1);
        SvCUR_set(scratch_sv, item_length);
        SvPOK_on(scratch_sv);
        read_buf = SvPVX(scratch_sv);
        check = PerlIO_read(fh, read_buf, item_length);
        if (check < item_length) {
            croak("PerlIO error: read %d bytes, expected %"UVuf" bytes", 
                check, (UV)item_length);
        }

        /* restore utf8 and tainted flags */
        check = PerlIO_read(fh, &flags, 1);
        if (check < 1) {
            croak("PerlIO failed: errno %d", errno);
        }
        if (flags & SORTEX_UTF8_FLAG) {
            SvUTF8_on(scratch_sv);
        }
        if (flags & SORTEX_TAINTED_FLAG) {
            SvTAINTED_on(scratch_sv);
        }

        /* add to the buffarray */
        av_push(buffarray_av, scratch_sv);

        /* track how much we've read so far */
        amount_read += item_length;
        num_items++;
    }

    RETVAL = newSViv(num_items);
}
OUTPUT: RETVAL

