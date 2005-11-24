#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#define SORTEX_UTF8_FLAG 0x1
#define SORTEX_TAINTED_FLAG 0x2

/* throw an error if a supplied check_val comes up negative */
void 
check_io_error(int check) {
    if (check < 0) 
        croak("PerlIO failed: errno %d", errno);
}

MODULE = Sort::External		PACKAGE = Sort::External		

PROTOTYPES: DISABLE

=for comment

Return a rough estimate of the memory occupied by the arguments.

=cut

SV*
_add_up_lengths (...)
PREINIT:
    UV sum;
    int i;
    SV *element;
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
OUTPUT:
    RETVAL

=for comment

Print an array of scalars to a file using an encoding which allows them to be
recovered later.

=cut

SV*
_print_to_sortfile (fh, ...)
    PerlIO *fh;
PREINIT:
    SV *thing_sv;
    int i, check;
    char buf_buf[5];
    char *string, *buf, *end_of_buf, *encoded_len;
    STRLEN string_len;
    UV aUV;
    char flags;
PPCODE:
{
    /* create a buffer to hold the compressed integer */
    buf = buf_buf;
    end_of_buf = buf + 5;
    
    for (i = 1; i < items; i++) {
        /* retrieve one scalar from the Perl stack */
        thing_sv   = ST(i);
        string_len = SvCUR(thing_sv);
        aUV        = string_len;
        string     = SvPV(thing_sv, string_len);
        
        /* encode the length of the scalar as a BER compressed integer */
        encoded_len = end_of_buf;
        do {
            *--encoded_len = (char)((aUV & 0x7f) | 0x80);
            aUV >>= 7;
        } while (aUV);
        *(end_of_buf - 1) &= 0x7f;  

        /* record utf8 and taint status */
        flags = 0;
        if (SvUTF8(thing_sv)) {
            flags |= SORTEX_UTF8_FLAG;
        }
        if (SvTAINTED(thing_sv)) {
            flags |= SORTEX_TAINTED_FLAG;
        }

        /* print len . string . flags to fh */
        check = PerlIO_write(fh, encoded_len, (end_of_buf - encoded_len));
        check_io_error(check);
        check = PerlIO_write(fh, string, string_len);
        check_io_error(check);
        check = PerlIO_write(fh, &flags, 1);
        check_io_error(check);
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
    SV *handle_ref, *buffarr_ref;
    PerlIO *fh;
    AV *buffarray_av;
    char num_buf[5];
    char *read_buf;
    UV item_length;
    STRLEN amount_read;
    int check, num_items;
    SV* aSV;
    char flags;
CODE:
{
    /* extract filehandle and buffer array from object */   
    handle_ref = *(hv_fetch( obj_hash, "tf_handle", 9, 0 ));
    fh         = IoIFP( sv_2io(handle_ref) );
    buffarr_ref  = *(hv_fetch( obj_hash, "buffarray", 9, 0 ));
    buffarray_av = (AV*)SvRV(buffarr_ref);


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
            check_io_error(check);
            if (check == 0)
                break;
            amount_read++;
            item_length = (item_length << 7) | (*read_buf & 0x7f);
            if ((U8)(*read_buf) < 0x80)
                break; 
        }

        /* bail (by design) if the attempt to read the length failed */
        if (PerlIO_eof(fh)) 
            break;

        /* recover the stringified scalar from disk */
        aSV = newSV(item_length + 1);
        SvCUR_set(aSV, item_length);
        SvPOK_on(aSV);
        read_buf = SvPVX(aSV);
        check = PerlIO_read(fh, read_buf, item_length);
        if (check < item_length) {
            croak("PerlIO error: read %d bytes, expected %"UVuf" bytes", 
                check, (UV)item_length);
        }

        /* restore utf8 and tainted flags */
        check = PerlIO_read(fh, &flags, 1);
        check_io_error(check);
        if (flags & SORTEX_UTF8_FLAG) {
            SvUTF8_on(aSV);
        }
        if (flags & SORTEX_TAINTED_FLAG) {
            SvTAINTED_on(aSV);
        }

        /* add to the buffarray */
        av_push(buffarray_av, aSV);

        /* track how much we've read so far */
        amount_read += item_length;
        num_items++;
    }

    RETVAL = newSViv(num_items);
}
OUTPUT:
    RETVAL

