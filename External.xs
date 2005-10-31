#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

void 
check_io_error(int check) {
    if (check < 0) 
        croak("PerlIO failed: errno %d", errno);
}

MODULE = Sort::External		PACKAGE = Sort::External		

PROTOTYPES: DISABLE


SV*
_add_up_lengths (...)
PREINIT:
    UV sum;
    int i;
    SV *element;
CODE:
{
    sum = 0;
    for (i = 0; i < items; i++) {
        element = ST(i);   
        sum += sv_len(element) + 15;
    }
    RETVAL = newSVuv(sum);
}
    OUTPUT:
        RETVAL

SV*
_print_to_sortfile (...)
PREINIT:
    SV *fh_sv_ref, *thing_sv;
    PerlIO *fh;
    int i, check, buf_size;
    char *string, *buf, *end_of_buf, *encoded_len;
    STRLEN string_len;
    UV aUV;
PPCODE:
{
    /* get the filehandle we'll print to */
    fh_sv_ref = ST(0);
    fh        = IoOFP( sv_2io(fh_sv_ref) );

    /* create a buffer to hold the compressed integer */
    buf_size = (sizeof(UV) * 8) / 7 + 1;
    New( 0, buf, buf_size, char );
    end_of_buf = buf + sizeof(buf);
    
    
    for (i = 1; i < items; i++) {
        /* retrieve one scalar from the Perl stack */
        thing_sv   = ST(i);
        string_len = SvCUR(thing_sv);
        aUV        = string_len;
        string     = SvPV(thing_sv, string_len);
        
        /* encode the lenght of the scalar as a BER compressed integer */
        encoded_len = end_of_buf;
        do {
            *--encoded_len = (char)((aUV & 0x7f) | 0x80);
            aUV >>= 7;
        } while (aUV);
        *(end_of_buf - 1) &= 0x7f;  

        /* print len . string to fh */
        check = PerlIO_write(fh, encoded_len, (end_of_buf - encoded_len));
        check_io_error(check);
        check = PerlIO_write(fh, string, string_len);
        check_io_error(check);
    }
}

MODULE = Sort::External   PACKAGE = Sort::External::Buffer

SV*
_refill_buffer (...)
PREINIT:
    SV *obj_ref_sv, *handle_ref, *buffarr_ref;
    HV *obj_hash;
    PerlIO *fh;
    AV *buffarray_av;
    char *buf_buf, *read_buf, *num_buf;
    UV item_length;
    STRLEN amount_read;
    int check, num_items;
    STRLEN filename_len;
    char *filename;
CODE:
{
    obj_ref_sv = ST(0);
    obj_hash   = (HV*)SvRV(obj_ref_sv);
    handle_ref = *(hv_fetch( obj_hash, "tf_handle", 9, 0 ));
    fh         = IoIFP( sv_2io(handle_ref) );

    buffarr_ref  = *(hv_fetch( obj_hash, "buffarray", 9, 0 ));
    buffarray_av = (AV*)SvRV(buffarr_ref);

    New(0, buf_buf, 32768, char);
    read_buf = buf_buf;
    num_buf  = buf_buf;
    
    amount_read = 0;
    num_items   = 0;

    filename_len = SvLEN(handle_ref);
    filename     = SvPV(handle_ref, filename_len);

    while (1) {
        if (amount_read > 32768)
            break;

        /* retrieve and decode len */
        item_length = 0;
        while (1) {
            check = PerlIO_read(fh, read_buf, 1);
            check_io_error(check);
            if (check == 0)
                break;
            amount_read++;

            item_length = (item_length << 7) | (*read_buf & 0x7f);
            if ((U8)(*read_buf) < 0x80)
                break; 
        }
        if (PerlIO_eof(fh))
            break;

        if (item_length > 32767) {
            New(0, read_buf, item_length, char);
            check = PerlIO_read(fh, read_buf, item_length);
            check_io_error(check);
            if (check < item_length) {
                croak("Incomplete read: %d read, %"UVuf"expected", 
                    check, item_length);
            }
                
            av_push(buffarray_av, newSVpvn(read_buf, item_length));
            Safefree(read_buf);
        }
        else {
            PerlIO_read(fh, read_buf, item_length);
            av_push(buffarray_av, newSVpvn(read_buf, item_length));
        }

        read_buf = buf_buf;
        amount_read += item_length;
        num_items++;
    }
    RETVAL = newSViv(num_items);
}
    OUTPUT:
        RETVAL
